import uuid

import firebase_admin
from core_data_modules.logging import Logger
from firebase_admin import credentials
from firebase_admin import firestore

BATCH_SIZE = 500
_UUID_KEY_NAME = "uuid"

log = Logger(__name__)


class FirestoreUuidTable(object):
    """
    Mapping table between a string and a random UUID backed by Firestore
    """
    def __init__(self, table_name, crypto_token_path, uuid_prefix):
        cred = credentials.Certificate(crypto_token_path)
        firebase_admin.initialize_app(cred)
        self._client = firestore.client()
        self._table_name = table_name
        self._uuid_prefix = uuid_prefix
        self._mappings_cache = dict()  # of data -> uuid

    def data_to_uuid_batch(self, list_of_data_requested):
        # Check if the request can be served entirely from the cache
        list_of_data_requested = set(list_of_data_requested)
        if len(list_of_data_requested - set(self._mappings_cache.keys())) == 0:
            log.info(f"Sourcing uuids for {len(list_of_data_requested)} data items from cache...")
            return {data: uuid for data, uuid in self._mappings_cache if data in list_of_data_requested}

        # Stream the datastore to a local copy
        # Separate out the mappings of existing items
        # Compute new mappings
        # Bulk update the data store
        # Return the mapping table
        log.info(f"Sourcing uuids for {len(list_of_data_requested)} data items from Firestore...")
        existing_mappings = dict() 
        for mapping in self._client.collection(f"tables/{self._table_name}/mappings").get():
            existing_mappings[mapping.id] = mapping.get(_UUID_KEY_NAME)

        set_of_data_requested = set(list_of_data_requested)
        new_mappings_needed = set_of_data_requested.difference(
            set(existing_mappings.keys()))

        log.info(f"Loaded {len(existing_mappings)} existing mappings. New mappings needed: {len(new_mappings_needed)}")

        new_mappings = dict()
        for data in new_mappings_needed:
            new_mappings[data] = FirestoreUuidTable.generate_new_uuid(self._uuid_prefix)
        
        # Batch write the new mappings
        total_count_to_write = len(new_mappings)
        i = 0
        batch_counter = 0
        batch = self._client.batch()
        for data in new_mappings.keys():
            # ensure in single read that the data doesn't exist
            uuid_doc = self._client.document(f"tables/{self._table_name}/mappings/{data}").get()
            exists = uuid_doc.exists
            if exists:
                log.warning("Attempted to set mapping for data which was already in the datastore. "
                            "Continuing without overwriting")
                existing_mappings[uuid_doc.id] = uuid_doc.get(_UUID_KEY_NAME)
                continue
            
            i += 1
            batch.set(
                self._client.document(f"tables/{self._table_name}/mappings/{data}"),
                {
                    _UUID_KEY_NAME: new_mappings[data]
                })
            batch_counter += 1
            if batch_counter >= BATCH_SIZE:
                batch.commit()
                log.info(f"Batch of {batch_counter} mappings committed, progress: {i} / {total_count_to_write}")
                batch_counter = 0
                batch = self._client.batch()
        
        if batch_counter > 0:
            batch.commit()
            log.info(f"Final batch of {batch_counter} mappings committed")
        
        existing_mappings.update(new_mappings)
        self._mappings_cache.update(existing_mappings)
        
        ret = dict()
        for data_requested in set_of_data_requested:
            ret[data_requested] = existing_mappings[data_requested]
        
        return ret

    def data_to_uuid(self, data):
        # Check if data mapping exists
        # If it does return the UUID
        # If it doesn't, create a new UUID, store it
        # block until the store completes return the new UUID
        uuid_doc_ref = self._client.document(f"tables/{self._table_name}/mappings/{data}").get()

        exists = uuid_doc_ref.exists

        log.info(f"Ref: {uuid_doc_ref}, exists: {exists}")

        if not exists:
            new_uuid = FirestoreUuidTable.generate_new_uuid(self._uuid_prefix)
            log.info(f"No mapping found for: {data}, assigning UUID: {new_uuid}")
            self._client.document(f"tables/{self._table_name}/mappings/{data}").set(
                {
                    _UUID_KEY_NAME: new_uuid
                }
            )
        else:
            new_uuid = uuid_doc_ref.get(_UUID_KEY_NAME)

        return new_uuid
    
    def uuid_to_data(self, uuid_to_lookup):
        # Search for the UUID
        # return the data or fail
        uuid_col_ref = self._client.collection(f"tables/{self._table_name}/mappings")

        # Create a query against the collection
        query_ref = uuid_col_ref.where(_UUID_KEY_NAME, u"==", uuid_to_lookup)

        # Execute the query, and return the first uuid found
        # The API doesn't have a get first method, so this
        # unusual iterator extractor is needed 
        for result in query_ref.get():
            return result.id
        raise LookupError() 

    def uuid_to_data_batch(self, uuids_to_lookup):
        # Check if the request can be served entirely from the cache
        uuids_to_lookup = set(uuids_to_lookup)
        if len(uuids_to_lookup - set(self._mappings_cache.values())) == 0:
            log.info(f"Looking up the data for {len(uuids_to_lookup)} uuids from cache...")
            return {uuid: data for data, uuid in self._mappings_cache if uuid in uuids_to_lookup}

        # Search for the UUID
        # Return a mapping data for the uuids that were in the collection
        log.info(f"Looking up the data for {len(uuids_to_lookup)} uuids from Firestore...")
        reverse_mappings = dict()
        for mapping in self._client.collection(f"tables/{self._table_name}/mappings").get():
            self._mappings_cache[mapping.id] = mapping.get(_UUID_KEY_NAME)
            reverse_mappings[mapping.get(_UUID_KEY_NAME)] = mapping.id
        
        log.info(f"Loaded {len(reverse_mappings)} mappings")

        results = {}
        for uuid_lookup in uuids_to_lookup:
            if uuid_lookup in reverse_mappings.keys():
                results[uuid_lookup] = reverse_mappings[uuid_lookup]

        log.info(f"Found keys for {len(results)} out of {len(uuids_to_lookup)} requests")
        return results

    @staticmethod
    def generate_new_uuid(prefix):
        return prefix + str(uuid.uuid4())
