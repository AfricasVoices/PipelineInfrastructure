import drive_client_wrapper as dcw

import sys

if (len(sys.argv) != 4):
    print ("Usage python upload.py service_account_credentials_file file_to_upload to_drive_folder_path")
    print ("Example script for uploads a file to Google Drive under the specified path, replacing")
    print ("the file with the same name if it exists.")

    exit(1)

SERVICE_ACCOUNT_CREDENTIALS_FILE = sys.argv[1]
dcw.init_client(SERVICE_ACCOUNT_CREDENTIALS_FILE)

FILE_TO_UPLOAD = sys.argv[2]
DRIVE_FOLDER_PATH = sys.argv[3]

dcw.update_or_create(FILE_TO_UPLOAD, DRIVE_FOLDER_PATH, recursive=True, target_folder_is_shared_with_me=True)
