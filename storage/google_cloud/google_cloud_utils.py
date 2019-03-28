from urllib.parse import urlparse

from google.cloud import storage


def download_blob_to_string(bucket_credentials_file_path, blob_url):
    """
    Downloads the contents of a Google Cloud blob to a string.

    :param bucket_credentials_file_path:
    :type bucket_credentials_file_path: str
    :param blob_url:
    :type blob_url: str`
    :return: Contents of 
    :rtype: str
    """
    parsed_blob_url = urlparse(blob_url)
    assert parsed_blob_url.scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                           "URL (i.e. of the form gs://bucket-name/file)"
    bucket_name = parsed_blob_url.netloc
    blob_name = parsed_blob_url.path.lstrip("/")

    print(f"Downloading blob '{blob_name}' in bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob_contents = blob.download_as_string().decode("utf-8")
    print("Downloaded blob.")

    return blob_contents
