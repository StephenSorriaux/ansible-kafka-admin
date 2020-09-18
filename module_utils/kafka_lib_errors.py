from kafka.errors import KafkaError


class KafkaManagerError(KafkaError):
    """
    Custom Kafka error raised by the manager
    """
    pass


class UndefinedController(KafkaManagerError):
    pass


class ReassignPartitionsTimeout(KafkaManagerError):
    """
    Raised when the reassignment znode is still present after all retries
    """
    pass


class UnableToRefreshState(KafkaManagerError):
    pass


class IncompatibleVersion(KafkaManagerError):
    """
    Raised when using an unsupported broker version
    """
    pass


class MissingConfiguration(KafkaManagerError):
    pass


class ZookeeperBroken(KafkaManagerError):
    pass
