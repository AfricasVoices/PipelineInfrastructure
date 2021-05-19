import uuid

from core_data_modules.data_models import Label


class MessageStatuses(object):
    LIVE = "live"              # Message is part of the 'live' data, and should be used in analysis.
    STALE = "stale"            # Message is stale and we should attempt to get a newer answer if we can. This message
                               # can be used until we get a newer answer.
    ARCHIVED = "archived"      # This message is no longer relevant and should be ignored by analysis.
    ANONYMISED = "anonymised"  # Message is anonymised, so personally identifiable details have been removed.

    VALUES = {LIVE, STALE, ARCHIVED, ANONYMISED}


class MessageDirections(object):
    IN = "in"                  # Message sent from a participant to AVF.
    OUT = "out"                # Message sent from AVF to a participant.

    VALUES = {IN, OUT}


class Message(object):
    def __init__(self, text, timestamp, participant_uuid, direction, channel_operator, status, dataset, labels,
                 message_id=None, coda_id=None, last_updated=None):
        """
        Represents a message sent to or received from a participant.

        :param text: Raw text of this message.
        :type text: str
        :param timestamp: Time that this message was sent/received.
        :type timestamp: datetime.datetime
        :param participant_uuid: Id of the participant who sent this message
        :type participant_uuid: str
        :param direction: One of `MessageDirections.VALUES`.
        :type direction: str
        :param channel_operator: Operator of the channel that wired this message e.g. hormuud, telegram.
        :type channel_operator: str
        :param status: One of `MessageStatus.VALUES`.
        :type status: str
        :param dataset: Dataset that this message belongs to e.g. "age", "healthcare_s01e01"
        :type dataset: str
        :param labels: Labels assigned to this message.
        :type labels: list of core_data_modules.data_models.Label
        :param message_id: Id of this message. If None, a message id will automatically be generated in the constructor.
        :type message_id: str | None
        :param coda_id: Id to use to look-up this message in Coda, optional.
        :type coda_id: str | None
        :param last_updated: Timestamp this message was last updated in Firestore, or None if it does not yet exist.
        :type last_updated: datetime.datetime | None
        """
        if message_id is None:
            message_id = str(uuid.uuid4())

        assert status in MessageStatuses.VALUES, status
        assert direction in MessageDirections.VALUES, direction

        self.text = text
        self.timestamp = timestamp
        self.participant_uuid = participant_uuid
        self.direction = direction
        self.channel_operator = channel_operator
        self.status = status
        self.dataset = dataset
        self.labels = labels
        self.message_id = message_id
        self.coda_id = coda_id
        self.last_updated = last_updated

    def to_dict(self):
        d = {
            "text": self.text,
            "timestamp": self.timestamp,
            "participant_uuid": self.participant_uuid,
            "direction": self.direction,
            "channel_operator": self.channel_operator,
            "status": self.status,
            "dataset": self.dataset,
            "labels": [label.to_dict() for label in self.labels],
            "message_id": self.message_id,
            "last_updated": self.last_updated
        }

        if self.coda_id is not None:
            d["coda_id"] = self.coda_id

        return d

    @classmethod
    def from_dict(cls, d):
        return cls(
            text=d["text"],
            timestamp=d["timestamp"],
            participant_uuid=d["participant_uuid"],
            direction=d["direction"],
            channel_operator=d["channel_operator"],
            status=d["status"],
            dataset=d["dataset"],
            labels=[Label.from_dict(label) for label in d["labels"]],
            message_id=d.get("message_id"),
            coda_id=d.get("coda_id"),
            last_updated=d["last_updated"]
        )

    def copy(self):
        return Message.from_dict(self.to_dict())
