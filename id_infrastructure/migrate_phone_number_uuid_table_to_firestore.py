import argparse

from core_data_modules.logging import Logger
from core_data_modules.util import PhoneNumberUuidTable

from firestore_uuid_table import FirestoreUuidTable

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrates a phone number <-> uuid table from Core Data Modules "
                                                 "to Firestore")

    parser.add_argument("core_data_phone_number_uuid_table_path", metavar="core-data-phone-number-uuid-table-path",
                        help="JSON file containing the core_data_modules.util.PhoneNumberUuidTable to migrate")
    parser.add_argument("firestore_credentials_file_path", metavar="firestore-credentials-file-path",
                        help="Path to the Firebase service account credentials file for the Firestore instance "
                             "that the data will be migrated to")
    parser.add_argument("firestore_target_table_name", metavar="firestore-target-table-name",
                        help="Name of the Firestore table to migrate the data to")

    args = parser.parse_args()

    core_data_phone_number_uuid_table_path = args.core_data_phone_number_uuid_table_path
    firestore_credentials_file_path = args.firestore_credentials_file_path
    firestore_target_table_name = args.firestore_target_table_name

    log.info("Loading Phone Number <-> UUID Table...")
    with open(core_data_phone_number_uuid_table_path, "r") as f:
        core_data_phone_number_uuid_table = PhoneNumberUuidTable.load(f)
    log.info(f"Loaded {len(core_data_phone_number_uuid_table.numbers())} phone number <-> uuid mappings")

    log.info(f"Initialising the Firestore table {firestore_target_table_name}...")
    firestore_phone_number_uuid_table = FirestoreUuidTable(
        firestore_target_table_name,
        firestore_credentials_file_path,
        "avf-phone-uuid-"
    )
    log.info("Firestore table initialised")

    log.info("Preparing the dict of Firestore mappings to set...")
    firestore_mappings = dict()
    for number in core_data_phone_number_uuid_table.numbers():
        firestore_mappings[number] = core_data_phone_number_uuid_table.get_uuid(number)
    log.info(f"Prepared {len(firestore_mappings)} for upload")

    log.info(f"Uploading the mappings to Firestore table...")
    firestore_phone_number_uuid_table.update_data_to_uuid_mappings(firestore_mappings)
    log.info("Migration complete")
