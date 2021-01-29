import firebase_admin
from core_data_modules.logging import Logger
from firebase_admin import credentials, firestore


log = Logger(__name__)


class FirestorePipelineLogger(object):

    def __init__(self,  pipeline_name, run_id, cert):
        """
        :param pipeline_name: Name of pipeline to update the pipeline logs of.
        :type pipeline_name: str
        :param run_id: Identifier of this pipeline run.
        :type run_id: str
        :param cert: Path to a certificate file or a dict representing the contents of a certificate.
        :type cert: str or dict

        """
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        cred = credentials.Certificate(cert)
        firebase_admin.initialize_app(cred)
        self.client = firestore.client()

    def _get_pipeline_log_doc_ref(self):
        return self.client.document(f"metrics/pipelines/pipeline_logs/{self.run_id}")


    def log_event(self, event_timestamp, event_name):
        """
        Updates the pipeline logs for the given pipeline run.
        """

        pipeline_log = {"pipeline_name": self.pipeline_name,
                        "run_id": self.run_id,
                        "timestamp": event_timestamp,
                        "event": event_name}

        log.info(f"Updating Pipeline Logs for project {event_name} at time {event_timestamp}...")
        self._get_pipeline_log_doc_ref().set(pipeline_log)
