import drive_client_wrapper as dcw

import sys

if (len(sys.argv) != 4):
    print ("Usage python upload.py auth_credentials file_to_upload to_drive_folder_path")
    print ("Uploads a new file to Google Drive under the specified path, replacing")
    print ("the file with the same name if it exists")

    exit(1)

AUTH_CREDENTIALS = sys.argv[1]
dcw.init_client(AUTH_CREDENTIALS)

FILE_TO_UPLOAD = sys.argv[2]
DRIVE_FOLDER_PATH = sys.argv[3]

dcw.create_or_update_file(FILE_TO_UPLOAD, DRIVE_FOLDER_PATH)