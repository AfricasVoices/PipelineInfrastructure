import firebase_admin
from core_data_modules.logging import Logger
from firebase_admin import credentials, firestore


log = Logger(__name__)


class FirestorePipelinesLogsTable(object):

    def __init__(self, cert):
        """
        :param cert: Path to a certificate file or a dict representing the contents of a certificate.
        :type cert: str or dict
        """
        cred = credentials.Certificate(cert)
        firebase_admin.initialize_app(cred, "firestore_pipeline_logs_table")
        self.client = firestore.client()


    def _get_pipeline_log_doc_ref(self, pipeline_name, timestamp_string):
        return self.client.document(f"metrics/pipeline_logs/{pipeline_name}/{timestamp_string}")


    def update_pipeline_logs(self, pipeline_name, timestamp_string, pipeline_logs):
        """
        Updates the pipeline logs for the given a pipeline run.

        :param pipeline_name: Name of pipeline to update the pipeline logs of.
        :type pipeline_name: str
        :param timestamp_string: timestamp string to update the pipeline logs of.
        :type timestamp_string: str
        :param pipeline_logs: Pipeline logs to update with.
        :type pipeline_logs: dict
        """
        log.info(f"Updating Pipeline Logs for project {pipeline_name} at time {timestamp_string}...")
        self._get_pipeline_log_doc_ref(pipeline_name, timestamp_string).set(pipeline_logs)
