import firebase_admin
from core_data_modules.logging import Logger
from firebase_admin import credentials, firestore
from datetime import datetime


log = Logger(__name__)


class FirestorePipelineLogger(object):

    def __init__(self, cert, pipeline_name, timestamp, run_id, event):
        """
        :param cert: Path to a certificate file or a dict representing the contents of a certificate.
        :type cert: str or dict
        :param pipeline_name: Name of pipeline to update the pipeline logs of.
        :type pipeline_name: str
        :param timestamp: timestamp string to update the pipeline logs of.
        :type timestamp: str
        :param run_id: Identifier of this pipeline run.
        :type run_id: str
        :param event: event log name.
        :type event: str

        """
        self.pipeline_name = pipeline_name
        self.timestamp = timestamp
        self.run_id = run_id
        self.event = event
        cred = credentials.Certificate(cert)
        firebase_admin.initialize_app(cred)
        self.client = firestore.client()

    def _compute_current_timestamp(self):

        timestamp = datetime.now()
        timestamp = timestamp.strftime("%d/%m/%Y %H:%M:%S")

        return timestamp

    def _get_pipeline_log_doc_ref(self):
        return self.client.document(f"metrics/pipelines/pipeline_logs/{self._compute_current_timestamp()}")

    def _log_event(self):
        """
        Returns a dict of timestamp, run_id and event
        """
        pipeline_log = {"pipeline_name": self.pipeline_name,
                        "timestamp": self.timestamp,
                         "run_id": self.run_id,
                         "event": self.event}

        return pipeline_log

    def update_pipeline_logs(self):
        """
        Updates the pipeline logs for the given pipeline run.
        """

        pipeline_log = self._log_event()

        log.info(f"Updating Pipeline Logs for project {self.pipeline_name} at time {self._compute_current_timestamp()}...")
        self._get_pipeline_log_doc_ref().set(pipeline_log)
