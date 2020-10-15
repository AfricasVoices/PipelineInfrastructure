import pytz

class PipelineLogs(object):
    def __init__(self, timestamp, run_id, event):
        """
        :param datetime: timestamp that the stats in this object cover.
        :type datetime: datetime.datetime
        :param run_id: Identifier of this pipeline run.
        :type run_id: string
        :param event: pipeline status/log to report.
        :type event: string
        """
        self.timestamp = timestamp
        self.run_id = run_id
        self.event = event

    def to_dict(self):
        return {
            "timestamp": self.timestamp.astimezone(pytz.utc).isoformat(),
            "run_id": self.run_id,
            "event": self.event
        }
