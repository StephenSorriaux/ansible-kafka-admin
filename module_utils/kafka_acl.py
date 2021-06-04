from kafka.errors import IllegalArgumentError
# enum in stdlib as of py3.4
try:
    from enum import IntEnum  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor.enum34 import IntEnum


class ACLOperation(IntEnum):
    """An enumerated type of acl operations"""

    ANY = 1,
    ALL = 2,
    READ = 3,
    WRITE = 4,
    CREATE = 5,
    DELETE = 6,
    ALTER = 7,
    DESCRIBE = 8,
    CLUSTER_ACTION = 9,
    DESCRIBE_CONFIGS = 10,
    ALTER_CONFIGS = 11,
    IDEMPOTENT_WRITE = 12

    @staticmethod
    def from_name(name):
        if not isinstance(name, str):
            raise ValueError("%r is not a valid ACLOperation" % name)

        if name.lower() == "any":
            return ACLOperation.ANY
        elif name.lower() == "all":
            return ACLOperation.ALL
        elif name.lower() == "read":
            return ACLOperation.READ
        elif name.lower() == "write":
            return ACLOperation.WRITE
        elif name.lower() == "create":
            return ACLOperation.CREATE
        elif name.lower() == "delete":
            return ACLOperation.DELETE
        elif name.lower() == "alter":
            return ACLOperation.ALTER
        elif name.lower() == "describe":
            return ACLOperation.DESCRIBE
        elif name.lower() == "cluster_action":
            return ACLOperation.CLUSTER_ACTION
        elif name.lower() == "describe_configs":
            return ACLOperation.DESCRIBE_CONFIGS
        elif name.lower() == "alter_configs":
            return ACLOperation.ALTER_CONFIGS
        elif name.lower() == "idempotent_write":
            return ACLOperation.IDEMPOTENT_WRITE
        else:
            raise ValueError("%r is not a valid ACLOperation" % name)


class ACLPermissionType(IntEnum):
    """An enumerated type of permissions"""

    ANY = 1,
    DENY = 2,
    ALLOW = 3

    @staticmethod
    def from_name(name):
        if not isinstance(name, str):
            raise ValueError("%r is not a valid ACLPermissionType" % name)

        if name.lower() == "any":
            return ACLPermissionType.ANY
        elif name.lower() == "deny":
            return ACLPermissionType.DENY
        elif name.lower() == "allow":
            return ACLPermissionType.ALLOW
        else:
            raise ValueError("%r is not a valid ACLPermissionType" % name)


class ACLResourceType(IntEnum):
    """An enumerated type of config resources"""

    ANY = 1,
    CLUSTER = 4,
    DELEGATION_TOKEN = 6,
    GROUP = 3,
    TOPIC = 2,
    TRANSACTIONAL_ID = 5

    @staticmethod
    def from_name(name):
        if not isinstance(name, str):
            raise ValueError("%r is not a valid ACLResourceType" % name)

        if name.lower() == "any":
            return ACLResourceType.ANY
        elif name.lower() in ("broker", "cluster"):
            return ACLResourceType.CLUSTER
        elif name.lower() == "delegation_token":
            return ACLResourceType.DELEGATION_TOKEN
        elif name.lower() == "group":
            return ACLResourceType.GROUP
        elif name.lower() == "topic":
            return ACLResourceType.TOPIC
        elif name.lower() == "transactional_id":
            return ACLResourceType.TRANSACTIONAL_ID
        else:
            raise ValueError("%r is not a valid ACLResourceType" % name)


class ACLPatternType(IntEnum):
    """An enumerated type of pattern type for ACLs"""

    ANY = 1,
    MATCH = 2,
    LITERAL = 3,
    PREFIXED = 4

    @staticmethod
    def from_name(name):
        if not isinstance(name, str):
            raise ValueError("%r is not a valid ACLPatternType" % name)

        if name.lower() == "any":
            return ACLPatternType.ANY
        elif name.lower() == "match":
            return ACLPatternType.MATCH
        elif name.lower() == "literal":
            return ACLPatternType.LITERAL
        elif name.lower() == "prefixed":
            return ACLPatternType.PREFIXED
        else:
            raise ValueError("%r is not a valid ACLPatternType" % name)


class ACLResource(object):
    """A class for specifying config resources.
    Arguments:
        resource_type (ConfigResourceType): the type of kafka resource
        name (string): The name of the kafka resource
        configs ({key : value}): A  maps of config keys to values.
    """

    def __init__(
            self,
            resource_type,
            operation,
            permission_type,
            pattern_type=None,
            name=None,
            principal=None,
            host=None,
    ):
        if not isinstance(resource_type, ACLResourceType):
            raise IllegalArgumentError("resource_param must be of type "
                                       "ACLResourceType")
        self.resource_type = resource_type
        if not isinstance(operation, ACLOperation):
            raise IllegalArgumentError("operation must be of type "
                                       "ACLOperation")
        self.operation = operation
        if not isinstance(permission_type, ACLPermissionType):
            raise IllegalArgumentError("permission_type must be of type "
                                       "ACLPermissionType")
        self.permission_type = permission_type
        if pattern_type is not None and not isinstance(pattern_type,
                                                       ACLPatternType):
            raise IllegalArgumentError("pattern_type must be of type "
                                       "ACLPatternType")
        self.pattern_type = pattern_type
        self.name = name
        self.principal = principal
        self.host = host

    def __repr__(self):
        return (
            "ACLResource(resource_type: %s, operation: %s, "
            "permission_type: %s, name: %s, principal: %s, host: %s, "
            "pattern_type: %s)" % (
                self.resource_type, self.operation,
                self.permission_type, self.name, self.principal, self.host,
                self.pattern_type
            )
        )

    def __eq__(self, other):
        if not isinstance(other, ACLResource):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return (
            self.resource_type.value == other.resource_type.value and
            self.operation.value == other.operation.value and
            self.permission_type.value == other.permission_type.value and
            self.name == other.name and
            self.principal == other.principal and
            self.host == other.host and
            self.pattern_type.value == other.pattern_type.value
        )

    def __hash__(self):
        return (hash(self.resource_type.value) ^
                hash(self.operation.value) ^
                hash(self.permission_type.value) ^
                hash(self.name) ^
                hash(self.principal) ^
                hash(self.host) ^
                hash(self.pattern_type.value))

    def to_dict(self):
        return {
            'resource_type': self.resource_type.name.lower(),
            'operation': self.operation.name.lower(),
            'permission_type': self.permission_type.name.lower(),
            'resource_name': self.name,
            'principal': self.principal,
            'host': self.host,
            'pattern_type': self.pattern_type.name.lower()
        }
