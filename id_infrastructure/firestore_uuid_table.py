import json

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import uuid

BATCH_SIZE = 500

class FirestoreUuidTable(object):
    def __init__(self, table_name, crypto_token_path, uuid_prefix):
        cred = credentials.Certificate(crypto_token_path)
        firebase_admin.initialize_app(cred)
        self._client = firestore.client()
        self._table_name = table_name
        self._uuid_prefix = uuid_prefix

    def data_to_uuid_batch(self, list_of_data_requested):
        # Steam the datastore to a local copy
        # Seperate out the mappings of existing items
        # Compute new mappings
        # Bulk update the data store
        # Return the mapping table

        existing_mappings = dict() 
        for mapping in self._client.collection(u'tables/{}/mappings'.format(self._table_name)).get():
            existing_mappings[mapping.id] = mapping.get("uuid")

        set_of_data_requested = set(list_of_data_requested)
        new_mappings_needed = set_of_data_requested.difference(
            set(existing_mappings.keys()))

        print ("new_mappings_needed: {}".format(len(new_mappings_needed)))

        new_mappings = dict()
        for data in new_mappings_needed:
            new_mappings[data] = FirestoreUuidTable.generate_new_uuid(self._uuid_prefix)
        
        # Batch write the new mappings
        total_count_to_write = len(new_mappings)
        i = 0
        batch_counter = 0
        batch = self._client.batch()
        for data in new_mappings.keys():
            i += 1
            batch.set(
                self._client.document(u'tables/{}/mappings/{}'.format(self._table_name, data)),
                {
                    "uuid" : new_mappings[data]
                })
            batch_counter += 1
            if batch_counter >= BATCH_SIZE:
                batch.commit()
                print ("Batch of {} messages committed, progress: {} / {}".format(batch_counter, i, total_count_to_write))
                batch_counter = 0
                batch = self._client.batch()
        
        if batch_counter > 0:
            batch.commit()
            print ("Final batch of {} messages committed".format(batch_counter))
        
        existing_mappings.update(new_mappings)
        
        ret = dict()
        for data_requested in set_of_data_requested:
            ret[data_requested] = existing_mappings[data_requested]
        
        return ret

    def data_to_uuid(self, data):
        # Check if data mapping exists
        # If it does return the UUID
        # If it doesn't, create a new UUID, store it
        # block until the store completes return the new UUID
        uuid_doc_ref = self._client.document(u'tables/{}/mappings/{}'.format(self._table_name, data)).get()

        exists = uuid_doc_ref.exists

        print ("Ref: {}, exists: {}".format(uuid_doc_ref, exists))

        if exists == False:
            print ("No mapping found for: {}".format(data))
            new_uuid = FirestoreUuidTable.generate_new_uuid(self._uuid_prefix)
            self._client.document(u'tables/{}/mappings/{}'.format(self._table_name, data)).set(
                {
                    "uuid" : new_uuid
                }
            )
        else:
            new_uuid = uuid_doc_ref.get("uuid")

        return new_uuid
    
    def uuid_to_data(self, uuid_to_lookup):
        # Search for the UUID
        # return the data or fail
        # Create a reference to the cities collection
        uuid_col_ref = self._client.collection(u'tables/{}/mappings'.format(self._table_name))

        # Create a query against the collection
        query_ref = uuid_col_ref.where(u'uuid', u'==', uuid_to_lookup)
        if not query_ref.exists:
            raise LookupError()
        return query_ref.get()

    @staticmethod
    def generate_new_uuid(prefix):
        return prefix + str(uuid.uuid4())
