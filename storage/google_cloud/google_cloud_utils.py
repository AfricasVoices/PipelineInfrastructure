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


def upload_file_to_blob(bucket_credentials_file_path, target_blob_url, f, max_retries=2, blob_chunk_size=100):
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
    :param blob_chunk_size: The chunk size to use for resumable uploads, in MiB
    :type blob_chunk_size: float
    """
    try:
        log.info(f"Uploading file to blob '{target_blob_url}'...")
        storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
        blob = _blob_at_url(storage_client, target_blob_url)

        # Check if blob_chunk_size is below the minimum threshold
        if blob_chunk_size > 0.256:
            blob.chunk_size = blob_chunk_size * 1024 * 1024
        else:
            blob.chunk_size = 0.256 * 1024 * 1024

        blob.upload_from_file(f)
        log.info(f"Uploaded file to blob")

    except (ConnectionError, socket.timeout, Timeout) as ex:
        log.warning("Failed to upload due to connection/timeout error")
        if max_retries > 0:
            log.info(f"Retrying up to{max_retries} more times with a reduced chunk_size of {int(round(blob_chunk_size/2))}MiB")
            # lower the chunk size and start uploading from beginning because resumable_media requires so
            f.seek(0)
            upload_file_to_blob(bucket_credentials_file_path, target_blob_url, f,
                                max_retries - 1, int(round(blob_chunk_size/2)))
        else:
            log.error(f"Failed to upload after retrying 3 times")
            raise ex
