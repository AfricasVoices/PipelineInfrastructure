from urllib.parse import urlparse

from google.cloud import storage
from core_data_modules.logging import Logger
from requests import ConnectionError, Timeout
import socket

log = Logger(__name__)


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


def upload_file_to_blob(bucket_credentials_file_path, target_blob_url, f, max_retries=4, blob_chunk_size=100 * 1024):
    """
    Uploads a file to a Google Cloud Storage blob.

    :param bucket_credentials_file_path: Path to a credentials file for accessing the bucket.
    :type bucket_credentials_file_path: str
    :param target_blob_url: gs URL to the blob to upload to (i.e. of the form gs://<bucket-name>/<blob-name>).
    :type target_blob_url: str
    :param f: File to upload, opened in binary mode.
    :type f: file-like
    :param max_retries: Maximum number of times to retry uploading the file.
    :type max_retries: int
    :param blob_chunk_size: The chunk size to use for resumable uploads, in KiB.
    :type blob_chunk_size: float
    """
    try:
        log.info(f"Uploading file to blob '{target_blob_url}'...")
        storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
        blob = _blob_at_url(storage_client, target_blob_url)
        blob.chunk_size = int(blob_chunk_size * 1024) # resumable expects an integer
        blob.upload_from_file(f)
        log.info(f"Uploaded file to blob")

    except (ConnectionError, socket.timeout, Timeout) as ex:
        log.warning("Failed to upload due to connection/timeout error")

        if max_retries <= 0:
            log.error(f"Failed to upload file to blob")
            raise ex

        if blob_chunk_size / 2 < 256:
            log.error(f"Not retrying because the next blob_chunk_size {blob_chunk_size / 2} is below the minimum "
                      f"allowed (256KB)")
            raise ex

        log.info(f"Retrying up to {max_retries} more times with a reduced chunk_size of {blob_chunk_size / 2}KB")
        # lower the chunk size and start uploading from beginning because resumable_media requires so
        f.seek(0)
        upload_file_to_blob(bucket_credentials_file_path, target_blob_url, f,
                            max_retries - 1, blob_chunk_size / 2)


def list_blobs( bucket_credentials_file_path, bucket_url, prefix):
    """
    Lists names of blobs in a bucket.

    :param bucket_credentials_file_path: Path to a credentials file for accessing the bucket.
    :type bucket_credentials_file_path: str
    :param bucket_url: gs URL to the bucket in which to look for objects names. (i.e. of the form gs://<bucket-name>).
    :type bucket_url: str
    :param prefix: Filter results to objects whose names begin with this prefix. (e.g a directory inside the bucket)
    :type prefix: str
    :return: a list of blob objects names.
    :rtype: list
    """
    storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
    parsed_bucket_url = urlparse(bucket_url)
    bucket_name = parsed_bucket_url.netloc
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    blob_names = [blob.name for blob in blobs]

    return blob_names
