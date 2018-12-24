from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaFileUpload

import logging
import os
import sys

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/drive'

_drive_service = None

log = logging.getLogger('dcw')
log.setLevel(logging.INFO)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
log.addHandler(consoleHandler)

def init_client(credentials_path, token_path):
    global _drive_service

    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    token_store = file.Storage(token_path)
    creds = token_store.get()

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(credentials_path, SCOPES)

        # Parse cmd line arguments in case some of them are for the oauth flow.
        flags, unknown = tools.argparser.parse_known_args(sys.argv)

        # Opens a web browser page asking the user to grant access to the data in SCOPES
        # and afte the user has granted access, stores the access token in [token_path]
        # so that it can be reused on future calls to the Drive API.
        creds = tools.run_flow(flow, token_store, flags=flags)

    _drive_service = build('drive', 'v3', http=creds.authorize(Http()))

def _get_root_id():
    log.info('Getting id of drive root folder...')
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

    log.info('Getting children of folder with id "{}"...'.format(folder_id))
    page_count = 1
    while True:
        log.info('Getting children of folder with id "{}" - got page {}'.format(folder_id, page_count))
        response = _drive_service.files().list(
            q="'{}' in parents".format(folder_id),
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token).execute()
        for file in response.get('files', []):
            children.append(file)
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    log.info('Getting children of folder with id "{}" - done. {} children'.format(folder_id, len(children)))
    return children

def _get_folder_id(name, parent_id, recursive=False):
    log.info('Getting id of folder "{}" under parent with id "{}"...'.format(name, parent_id))
    response = _drive_service.files().list(
        q="name='{}' and '{}' in parents and mimeType='application/vnd.google-apps.folder'".format(name, parent_id),
        spaces='drive',
        fields='files(id)').execute()
    files = response.get('files', [])
    if (len(files) == 0):
        if (recursive == False):
            log.error('Folder "{}" not found under parent with id {}.'.format(name, parent_id))
            exit(1)
        # Create folder
        files.append({'id': _add_folder(name, parent_id)})
    if (len(files) > 1):
        log.error('Multiple folders with name "{}" found under parent with id {}.'.format(name, parent_id))
        exit(1)
    log.info('Getting id of folder "{}" under parent with id "{}" - done. Folder id is "{}"'.format(name, parent_id, files[0].get('id')))
    return files[0].get('id')

def _get_path_id(path, recursive=False):
    folders = _split_path(path)
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
    log.info('Creating folder "{}" under parent with id "{}"...'.format(name, parent_id))
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    files = _drive_service.files().create(body=file_metadata,
                                        fields='id').execute()
    log.info('Creating folder "{}" under parent with id "{}" - done. Folder id is "{}"'.format(name, parent_id, files[0].get('id')))
    return files[0].get('id')

def _update_file(source_file_path, target_file_id):
    media = MediaFileUpload(source_file_path,
                            resumable=True)

    log.info('Updating file with ID "{}" with source file "{}"...'.format(target_file_id, source_file_path))
    file = _drive_service.files().update(fileId=target_file_id,
                                    media_body=media,
                                    fields='name').execute()

    log.info('Updating file with ID "{}" with source file "{}" - done. File name was "{}"'.format(target_file_id, source_file_path, file.get('name')))

def _create_file(source_file_path, target_folder_path, target_file_name=None):
    if target_file_name == None:
        target_file_name = os.path.basename(source_file_path)

    target_folder_id = _get_path_id(target_folder_path)
    file_metadata = {
        'name': target_file_name,
        'parents': [target_folder_id]
    }
    media = MediaFileUpload(source_file_path,
                            resumable=True)

    log.info('Creating file "{}" in folder "{}" with source file "{}"...'.format(target_file_name, target_folder_path, source_file_path))
    file = _drive_service.files().create(body=file_metadata,
                                    media_body=media,
                                    fields='id').execute()
    log.info('Creating file "{}" in folder "{}" with source file "{}" - done. File id is "{}"'.format(target_file_name, target_folder_path, source_file_path, file.get('id')))

def update_or_create(source_file_path, target_folder_path, target_file_name=None, recursive=False):
    if target_file_name == None:
        target_file_name = os.path.basename(source_file_path)

    target_folder_id = _get_path_id(target_folder_path, recursive)
    files = _list_folder_id(target_folder_id)
    files_with_upload_name = list(filter(lambda file: file.get('name') == target_file_name, files))

    if (len(files_with_upload_name) > 1):
        log.error('Multiple files with the same name found in Drive folder.')
        log.error('I don\'t know which to update, aborting.')
        exit(1)

    if (len(files_with_upload_name) == 1):
        existing_file = files_with_upload_name[0]
        # Make sure it's not a folder
        if (existing_file.get('mimetype') == 'application/vnd.google-apps.folder'):
            log.error('Attempting to replace a folder with a file with name "{}"'.format(target_file_name))
            exit(1)
        _update_file(source_file_path, existing_file.get('id'))
        return

    _create_file(source_file_path, target_folder_path, target_file_name)
