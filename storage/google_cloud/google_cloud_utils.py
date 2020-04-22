from urllib.parse import urlparse

from google.cloud import storage
from core_data_modules.logging import Logger

log = Logger(__name__)

BLOB_CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB


def _blob_at_url(storage_client, blob_url):
    parsed_blob_url = urlparse(blob_url)
    assert parsed_blob_url.scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                           "URL (i.e. of the form gs://bucket-name/blob-name)"
    bucket_name = parsed_blob_url.netloc
    blob_name = parsed_blob_url.path.lstrip("/")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob


def download_blob_to_string(bucket_credentials_file_path, blob_url):
    """
    Downloads the contents of a Google Cloud Storage blob to a string.

    :param bucket_credentials_file_path: Path to a credentials file for accessing the bucket.
    :type bucket_credentials_file_path: str
    :param blob_url: gs URL to the blob to download (i.e. of the form gs://<bucket-name>/<blob-name>).
    :type blob_url: str
    :return: Contents of the requested blob.
    :rtype: str
    """
    log.info(f"Downloading blob '{blob_url}' to string...")
    storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
    blob = _blob_at_url(storage_client, blob_url)
    blob_contents = blob.download_as_string().decode("utf-8")
    log.info(f"Downloaded blob to string ({len(blob_contents)} characters).")

    return blob_contents


def upload_string_to_blob(bucket_credentials_file_path, target_blob_url, string):
    """
    Uploads a string to a Google Cloud Storage blob.

    :param bucket_credentials_file_path: Path to a credentials file for accessing the bucket.
    :type bucket_credentials_file_path: str
    :param target_blob_url: gs URL to the blob to upload to (i.e. of the form gs://<bucket-name>/<blob-name>).
    :type target_blob_url: str
    :param string: String to upload
    :type string: str
    """
    log.info(f"Uploading string to blob '{target_blob_url}' ({len(string)} characters)...")
    storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
    blob = _blob_at_url(storage_client, target_blob_url)
    blob.chunk_size = BLOB_CHUNK_SIZE
    blob.upload_from_string(string)
    log.info("Uploaded string to blob.")


def download_blob_to_file(bucket_credentials_file_path, blob_url, f):
    """
    Downloads a Google Cloud Storage blob to a file.

    :param bucket_credentials_file_path: Path to a credentials file for accessing the bucket.
    :type bucket_credentials_file_path: str
    :param blob_url: gs URL to the blob to download (i.e. of the form gs://<bucket-name>/<blob-name>).
    :type blob_url: str
    :param f: File to download the blob to, opened in binary mode.
    :type f: file-like
    """
    log.info(f"Downloading blob '{blob_url}' to file...")
    storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
    blob = _blob_at_url(storage_client, blob_url)
    blob.download_to_file(f)
    log.info(f"Downloaded blob to file")


def upload_file_to_blob(bucket_credentials_file_path, target_blob_url, f):
    """
    Uploads a file to a Google Cloud Storage blob.

    :param bucket_credentials_file_path: Path to a credentials file for accessing the bucket.
    :type bucket_credentials_file_path: str
    :param target_blob_url: gs URL to the blob to upload to (i.e. of the form gs://<bucket-name>/<blob-name>).
    :type target_blob_url: str
    :param f: File to upload, opened in binary mode.
    :type f: file-like
    """
    log.info(f"Uploading file to blob '{target_blob_url}'...")
    storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
    blob = _blob_at_url(storage_client, target_blob_url)
    blob.chunk_size = BLOB_CHUNK_SIZE
    blob.upload_from_file(f)
    log.info(f"Uploaded file to blob")
