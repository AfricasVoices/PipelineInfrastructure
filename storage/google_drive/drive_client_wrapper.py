from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaFileUpload

import mimetypes
import os

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/drive'

drive_service = None
drive_folder_root = None

def init_client(auth_credentials_path):
    global drive_service
    global drive_folder_root
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    drive_service = build('drive', 'v3', http=creds.authorize(Http()))
    drive_folder_root = drive_service.files()

def get_root_id():
    return drive_folder_root.get(fileId='root').execute().get('id')

def get_children(folder_id):
    """Returns a list with map elements with the following structure:
    {
        'name': ''
        'id': '',
        'mimeType': '',
    }
    """
    children = []
    page_token = None
    while True:
        response = drive_folder_root.list(
            q="'{}' in parents".format(folder_id),
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType)',
            pageToken=page_token).execute()
        for file in response.get('files', []):
            children.append(file)
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return children

def get_folder_id(name, parent_id):
    response = drive_folder_root.list(
        q="name='{}' and '{}' in parents and mimeType='application/vnd.google-apps.folder'".format(name, parent_id),
        spaces='drive',
        fields='files(id)').execute()
    files = response.get('files', [])
    if (len(files) == 0):
        raise LookupError('Folder "{}" not found under parent with id {}.'.format(name, parent_id))
    if (len(files) > 1):
        raise LookupError('Multiple folders with name "{}" found under parent with id {}.'.format(name, parent_id))
    return files[0].get('id')

def get_path_id(path):
    folders = _split_path(path)
    folder_id = get_root_id()
    for folder in folders:
        folder_id = get_folder_id(folder, folder_id)
    return folder_id

def _split_path(path):
    folders = []
    while path != "":
        path, folder = os.path.split(path)
        if folder != "":
            folders.append(folder)
    folders.reverse()
    return folders

def update_file(source_file_path, file_id, mimetype):
    media = MediaFileUpload(source_file_path,
                            mimetype=mimetype,
                            resumable=True)

    file = drive_folder_root.update(fileId=file_id,
                                    media_body=media,
                                    fields='name').execute()
    
    print('File "{}" with ID: {} updated'.format(file.get('name'), file_id))

def create_file(source_file_path, mimetype, parent_folder_id, uploaded_file_name=None):
    if uploaded_file_name == None:
        uploaded_file_name = os.path.basename(source_file_path)

    file_metadata = {
        'name': uploaded_file_name,
        'parents': [parent_folder_id]
    }
    media = MediaFileUpload(source_file_path,
                            mimetype=mimetype,
                            resumable=True)

    file = drive_folder_root.create(body=file_metadata,
                                    media_body=media,
                                    fields='id').execute()
    print('File "{}" created with ID: {}'.format(uploaded_file_name, file.get('id')))

def create_or_update_file(source_file_path, drive_folder_path, uploaded_file_name=None):
    if uploaded_file_name == None:
        uploaded_file_name = os.path.basename(source_file_path)

    (mimetype, _) = mimetypes.guess_type(uploaded_file_name)
    if (mimetype == None):
        mimetype = 'application/octet-stream'
    
    parent_folder_id = get_path_id(drive_folder_path)
    children = get_children(parent_folder_id)
    print(children)
    children_with_upload_name = list(filter(lambda file: file.get('name') == uploaded_file_name, children))
    
    if (len(children_with_upload_name) > 1):
        print('Multiple files with the same name found in Drive folder.')
        print('I don\'t know which to update, aborting.')
        exit(1)

    if (len(children_with_upload_name) == 1):
        existing_file = children_with_upload_name[0]
        # Make sure it's not a folder
        if (existing_file.get('mimetype') == 'application/vnd.google-apps.folder'):
            print('Attempting to replace a folder with a file with name "{}"'.format(uploaded_file_name))
            exit(1)
        update_file(source_file_path, existing_file.get('id'), mimetype)
        return

    create_file(source_file_path, mimetype, parent_folder_id, uploaded_file_name)
