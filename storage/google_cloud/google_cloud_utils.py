from urllib.parse import urlparse

from google.cloud import storage


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
    parsed_blob_url = urlparse(blob_url)
    assert parsed_blob_url.scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                           "URL (i.e. of the form gs://bucket-name/blob-name)"
    bucket_name = parsed_blob_url.netloc
    blob_name = parsed_blob_url.path.lstrip("/")

    print(f"Downloading blob '{blob_name}' in bucket '{bucket_name}' to string...")
    storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob_contents = blob.download_as_string().decode("utf-8")
    print(f"Downloaded blob ({len(blob_contents)} characters).")

    return blob_contents
