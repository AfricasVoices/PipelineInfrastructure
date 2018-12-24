import drive_client_wrapper as dcw

import sys

if (len(sys.argv) != 5):
    print ("Usage python upload.py auth_credentials auth_token file_to_upload to_drive_folder_path")
    print ("Uploads a new file to Google Drive under the specified path, replacing")
    print ("the file with the same name if it exists")

    exit(1)

AUTH_CREDENTIALS = sys.argv[1]
AUTH_TOKEN = sys.argv[2]
dcw.init_client(AUTH_CREDENTIALS, AUTH_TOKEN)

FILE_TO_UPLOAD = sys.argv[3]
DRIVE_FOLDER_PATH = sys.argv[4]

dcw.update_or_create(FILE_TO_UPLOAD, DRIVE_FOLDER_PATH, recursive=True)
