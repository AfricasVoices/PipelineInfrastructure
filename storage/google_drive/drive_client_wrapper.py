import os
import time

import google.oauth2.service_account
import googleapiclient.discovery
from core_data_modules.logging import Logger
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]

_drive_service = None

log = Logger(__name__)


def init_client_from_file(service_account_credentials_file):
    global _drive_service

    credentials = google.oauth2.service_account.Credentials.from_service_account_file(service_account_credentials_file,
                                                                                      scopes=SCOPES)
    if not credentials:
        log.error(f"Failed to get credentials from file '{service_account_credentials_file}'")
        exit(1)

    _drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)


def init_client_from_info(service_account_credentials_info):
    global _drive_service

    credentials = google.oauth2.service_account.Credentials.from_service_account_info(service_account_credentials_info,
                                                                                      scopes=SCOPES)
    if not credentials:
        log.error("Failed to get credentials from dict")
        exit(1)

    _drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)


def _get_root_id():
    log.info("Getting id of drive root folder...")
    return _drive_service.files().get(fileId='root').execute().get('id')


def _list_folder_id(folder_id):
    """Returns a list with map elements with the following structure:
    {
        'name': ''
        'id': '',
        'mimeType': '',
    }
    """
    children = []
    page_token = None

    log.info(f"Getting children of folder with id '{folder_id}'...")
    page_count = 1
    while True:
        log.info(f"Getting children of folder with id '{folder_id}' - got page {page_count}")
        response = _drive_service.files().list(
            q=f"'{folder_id}' in parents",
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token).execute()
        for file in response.get("files", []):
            children.append(file)
        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break
    log.info(f"Getting children of folder with id '{folder_id}' - done. {len(children)} children")
    return children


def _get_folder_id(name, parent_id, recursive=False):
    log.info('Getting id of folder "{}" under parent with id "{}"...'.format(name, parent_id))
    response = _drive_service.files().list(
        q="name='{}' and '{}' in parents and mimeType='application/vnd.google-apps.folder'".format(name, parent_id),
        spaces='drive',
        fields='files(id)').execute()
    files = response.get('files', [])
    if len(files) == 0:
        if not recursive:
            log.error(f"Folder '{name}' not found under parent with id {parent_id}.")
            exit(1)
        # Create folder
        files.append({"id": _add_folder(name, parent_id)})
    if len(files) > 1:
        log.error(f"Multiple folders with name '{name}' found under parent with id {parent_id}.")
        exit(1)
    assert (len(files) == 1)
    folder = files[0]
    folder_id = folder.get("id")
    log.info(
        f"Getting id of folder '{name}' under parent with id '{parent_id}' - done. Folder id is '{folder_id}'")
    return folder_id


def _get_shared_folder_id(name):
    log.info(f"Getting id of shared-with-me folder '{name}'...")
    response = _drive_service.files().list(
        q=f"name='{name}' and sharedWithMe=true and mimeType='application/vnd.google-apps.folder'",
        spaces="drive",
        fields="files(id)").execute()
    files = response.get("files", [])
    if len(files) == 0:
        log.error(f"Folder '{name}' not found in shared-with-me category.")
        exit(1)
    if len(files) > 1:
        log.error(f"Multiple folders with name '{name}' found in shared-with-me category.")
        exit(1)
    assert (len(files) == 1)
    folder = files[0]
    folder_id = folder.get('id')
    log.info(f"Getting id of shared-with-me folder '{name}' - done. Folder id is '{folder_id}'")
    return folder_id


def _get_path_id(path, recursive=False, target_folder_is_shared_with_me=False):
    folders = _split_path(path)

    if target_folder_is_shared_with_me:
        if len(folders) == 0:
            log.error("Missing target folder name which necessary when looking for a shared-with-me type folder")
            exit(1)
        folder_id = _get_shared_folder_id(folders[0])
        folders.remove(folders[0])
    else:
        folder_id = _get_root_id()

    for folder in folders:
        folder_id = _get_folder_id(folder, folder_id, recursive)
    return folder_id


def _split_path(path):
    folders = []
    while path != "":
        path, folder = os.path.split(path)
        if folder != "":
            folders.append(folder)
    folders.reverse()
    return folders


def _add_folder(name, parent_id):
    log.info(f"Creating folder '{name}' under parent with id '{parent_id}'...")
    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    file = _drive_service.files().create(body=file_metadata,
                                         fields="id").execute()
    log.info(f"Creating folder '{name}' under parent with id '{parent_id}' - done. Folder id is '{file.get('id')}'")
    return file.get('id')


def _update_file(source_file_path, target_file_id):
    media = MediaFileUpload(source_file_path,
                            resumable=True)

    log.info(f"Updating file with ID '{target_file_id}' with source file '{source_file_path}'...")
    file = _drive_service.files().update(fileId=target_file_id,
                                         media_body=media,
                                         fields="name").execute()

    log.info(
        f"Updating file with ID '{target_file_id}' with source file '{source_file_path}' - done. File name was "
        f"'{file.get('name')}'"
    )


def _create_file(source_file_path, target_folder_id, target_file_name=None):
    if target_file_name is None:
        target_file_name = os.path.basename(source_file_path)

    file_metadata = {
        "name": target_file_name,
        "parents": [target_folder_id]
    }
    media = MediaFileUpload(source_file_path,
                            resumable=True)

    log.info(f"Creating file '{target_file_name}' in folder with ID '{target_folder_id}' "
             f"with source file '{source_file_path}'...")
    file = _drive_service.files().create(body=file_metadata,
                                         media_body=media,
                                         fields="id").execute()
    log.info(f"Creating file '{target_file_name}' in folder with ID '{target_folder_id}' with source file "
             f"'{source_file_path}' - done. File id is '{file.get('id')}'")


def update_or_create(source_file_path, target_folder_path, target_file_name=None, recursive=False,
                     target_folder_is_shared_with_me=False, max_retries=2, backoff_seconds=1):
    try:
        if target_file_name is None:
            target_file_name = os.path.basename(source_file_path)

        target_folder_id = _get_path_id(target_folder_path, recursive, target_folder_is_shared_with_me)
        files = _list_folder_id(target_folder_id)
        files_with_upload_name = list(filter(lambda file: file.get('name') == target_file_name, files))

        if len(files_with_upload_name) > 1:
            log.error("Multiple files with the same name found in Drive folder.")
            log.error("I don't know which to update, aborting.")
            exit(1)

        if len(files_with_upload_name) == 1:
            existing_file = files_with_upload_name[0]
            # Make sure it's not a folder
            if existing_file.get("mimetype") == "application/vnd.google-apps.folder":
                log.error(f"Attempting to replace a folder with a file with name '{target_file_name}'")
                exit(1)
            _update_file(source_file_path, existing_file.get("id"))
            return

        _create_file(source_file_path, target_folder_id, target_file_name)
    except HttpError as ex:
        # Handle 500/503 errors with exponentiated back-off, as is recommended by the Drive docs for this error.
        if ex.resp.status not in {500, 503}:
            raise ex

        log.warning(f"Upload failed with HttpError {ex.resp.status}")
        if max_retries > 0:
            log.info(f"Retrying up to {max_retries} more times, after {backoff_seconds} seconds...")
            time.sleep(backoff_seconds)
            update_or_create(source_file_path, target_folder_path, target_file_name, recursive,
                             target_folder_is_shared_with_me, max_retries - 1, backoff_seconds * 2)
        else:
            log.error("Retried the maximum number of times")
            raise ex
