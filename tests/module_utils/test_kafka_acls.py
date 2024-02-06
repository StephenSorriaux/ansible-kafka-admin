import module_utils.kafka_fix_import  # noqa
from module_utils.kafka_acl import ACLResourceType as rs
from module_utils.kafka_acl import ACLOperation as op
from module_utils.kafka_acl import (
    ACLResource, ACLPermissionType, ACLPatternType
)


def test_acl_operations_equality():
    assert op.ANY == op.WRITE
    assert op.WRITE == op.WRITE
    assert op.READ != op.WRITE


def test_acl_resource_equality():
    commons = {
        'pattern_type': ACLPatternType.LITERAL,
        'name': 'my-topic',
        'principal': 'User:alice',
        'host': '*'
    }
    r1 = ACLResource(rs.TOPIC, op.WRITE, ACLPermissionType.ALLOW, **commons)
    r2 = ACLResource(rs.TOPIC, op.ANY, ACLPermissionType.ALLOW, **commons)

    assert r1 == r2
    assert r1 in [r2]
