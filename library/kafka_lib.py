#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2018, Stephen SORRIAUX
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
"""
Ansible module for topic configuration management
"""
from __future__ import absolute_import, division, print_function
__metaclass__ = type

# import module snippets
import os
import time
import itertools
import json
import tempfile
from pkg_resources import parse_version

from kafka.client import KafkaClient
from kafka.protocol.api import Request, Response
from kafka.protocol.metadata import MetadataRequest_v1
from kafka.protocol.admin import (
    CreatePartitionsResponse_v0,
    CreateTopicsRequest_v0,
    DeleteTopicsRequest_v0,
    CreateAclsRequest_v0,
    DeleteAclsRequest_v0,
    DescribeAclsRequest_v0)
from kafka.protocol.types import (
    Array, Boolean, Int8, Int16, Int32, Schema, String
)
import kafka.errors
from kafka.errors import IllegalArgumentError
from kazoo.client import KazooClient

# enum in stdlib as of py3.4
try:
    from enum import IntEnum  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor.enum34 import IntEnum

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.pycompat24 import get_exception

ANSIBLE_METADATA = {'metadata_version': '1.0'}


DOCUMENTATION = '''
---
module: kafka_lib
short_description: Manage Kafka topic or ACL
description:
     - Configure Kafka topic or ACL.
     - Not compatible avec Kafka version < 0.11.0.
author:
    - Stephen SORRIAUX
options:
  resource:
    description:
      - 'managed resource type.'
    default: topic
    choices: [topic, acl] (more to come)
  name:
    description:
      - 'when resource = topic, name of the topic.'
      - 'when resource = acl, name of the `acl_resource_type` or * for'
      - 'all resources of type `acl_resource_type`.'
    required: True
  partition:
    description:
      - 'when resource = topic, number of partitions for this resource.'
  replica_factor:
    description:
      - 'when resource = topic, number of replica for the partitions of '
      - 'this resource.'
  state:
    description:
      - 'state of the managed resource.'
    default: present
    choices: [present, absent]
  options:
    description:
      - 'a dict with all options wanted for the managed resource'
      - 'Example: retention.ms: 7594038'
    type: dict
  acl_resource_type:
    description:
      - 'the resource type the ACL applies to.'
    default: topic
    choices: [topic, broker, delegation_token, group, transactional_id]
  acl_principal:
    description:
      - 'the principal the ACL applies to.'
      - 'Example: User:Alice'
  acl_operation:
    description:
      - 'the operation the ACL controls.'
    choices: [all, alter, alter_configs, cluster_actions, create, delete,
                describe, describe_configs, idempotent_write, read, write]
  acl_permission:
    description:
      - 'should the ACL allow or deny the operation.'
    default: allow
    choices: [allow, deny]
  acl_host:
    description:
      - 'the client host the ACL applies to.'
    default: *
  zookeeper:
    description:
      - 'the zookeeper connection.'
  zookeeper_auth_scheme:
    description:
      - 'when zookeeper is configured to use authentication, schema used to '
      - 'connect to zookeeper.'
      default: 'digest'
      choices: [digest, sasl]
  zookeeper_auth_value:
    description:
      - 'when zookeeper is configured to use authentication, value used to '
      - 'connect.'
  zookeeper_ssl_check_hostname:
    description:
      - 'when using ssl for zookeeper, check if certificate for hostname is '
      - 'correct.'
    default: True
  zookeeper_ssl_cafile:
    description:
      - 'when using ssl for zookeeper, content of ca cert file or path to '
      - 'ca cert file.'
  zookeeper_ssl_certfile:
    description:
      - 'when using ssl for zookeeper, content of cert file or path to '
      - 'server cert file.'
  zookeeper_ssl_keyfile:
    description:
      - 'when using ssl for zookeeper, content of keyfile or path to '
      - 'server cert key file.'
  zookeeper_ssl_password:
    description:
      - 'when using ssl for zookeeper, password for ssl_keyfile.'
  zookeeper_sleep_time:
    description:
      - 'when updating number of partitions and while checking for'
      - 'the ZK node, the time to sleep (in seconds) between'
      - 'each checks.'
      default: 5
  zookeeper_max_retries:
    description:
      - 'when updating number of partitions and while checking for'
      - 'the ZK node, maximum of try to do before failing'
      default: 5
  bootstrap_servers:
    description:
      - 'kafka broker connection.'
      - 'format: host1:port,host2:port'
    required: True
  security_protocol:
    description:
      - 'how to connect to Kafka.'
     default: PLAINTEXT
     choices: [PLAINTEXT, SASL_PLAINTEXT, SSL, SASL_SSL]
  api_version:
    description:
      - 'kafka version'
      - 'format: major.minor.patch. Examples: 0.11.0 or 1.0.1'
      - 'if not set, will launch an automatic version discovery but can '
      - 'trigger stackstraces on Kafka server.'
    default: auto
  ssl_check_hostname:
    description:
      - 'when using ssl for Kafka, check if certificate for hostname is '
      - 'correct.'
    default: True
  ssl_cafile:
    description:
      - 'when using ssl for Kafka, content of ca cert file or path to ca '
      - 'cert file.'
  ssl_certfile:
    description:
      - 'when using ssl for Kafka, content of cert file or path to server '
      - 'cert file.'
  ssl_keyfile:
    description:
      - 'when using ssl for kafka, content of keyfile or path to server '
      - 'cert key file.'
  ssl_password:
    description:
      - 'when using ssl for Kafka, password for ssl_keyfile.'
  ssl_crlfile:
    description:
      - 'when using ssl for Kafka, content of crl file or path to cert '
      - 'crl file.'
  sasl_mechanism:
    description:
      - 'when using sasl, whether use PLAIN or GSSAPI.'
    default: PLAIN
    choices: [PLAIN, GSSAPI]
  sasl_plain_username:
    description:
      - 'when using security_protocol = ssl, username to use.'
  sasl_plain_password:
    description:
      - 'when using security_protocol = ssl, password for '
      - 'sasl_plain_username.'
  sasl_kerberos_service_name:
    description:
      - 'when using kerberos, service name.'
'''

EXAMPLES = '''

    # creates a topic 'test' with provided configuation for plaintext
    # configured Kafka and Zookeeper
    - name: create topic
      kafka_lib:
        resource: 'topic'
        api_version: "1.0.1"
        name: 'test'
        partitions: 2
        replica_factor: 1
        options:
          retention.ms: 574930
          flush.ms: 12345
        state: 'present'
        zookeeper: >
          "{{ hostvars['zk']['ansible_eth0']['ipv4']['address'] }}:2181"
        bootstrap_servers: >
          "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,
          {{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

    # creates a topic for a sasl_ssl configured Kafka and plaintext Zookeeper
    - name: create topic
      kafka_lib:
        resource: 'topic'
        api_version: "1.0.1"
        name: 'test'
        partitions: 2
        replica_factor: 1
        options:
          retention.ms: 574930
          flush.ms: 12345
        state: 'present'
        zookeeper: >
          "{{ hostvars['zk']['ansible_eth0']['ipv4']['address'] }}:2181"
        bootstrap_servers: >
          "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,
          {{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"
        security_protocol: 'SASL_SSL'
        sasl_plain_username: 'username'
        sasl_plain_password: 'password'
        ssl_cafile: '{{ content_of_ca_cert_file_or_path_to_ca_cert_file }}'

    # creates a topic for a plaintext configured Kafka and a digest
    # authentication Zookeeper
    - name: create topic
      kafka_lib:
        resource: 'topic'
        api_version: "1.0.1"
        name: 'test'
        partitions: 2
        replica_factor: 1
        options:
          retention.ms: 574930
          flush.ms: 12345
        state: 'present'
        zookeeper: >
          "{{ hostvars['zk']['ansible_eth0']['ipv4']['address'] }}:2181"
        zookeeper_auth_scheme: "digest"
        zookeeper_auth_value: "username:password"
        bootstrap_servers: >
          "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,
          {{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

    # deletes a topic
    - name: delete topic
      kafka_lib:
        resource: 'topic'
        api_version: "1.0.1"
        name: 'test'
        state: 'absent'
        zookeeper: >
          "{{ hostvars['zk']['ansible_eth0']['ipv4']['address'] }}:2181"
        bootstrap_servers: >
          "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,
          {{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

    # deletes a topic using automatic api_version discovery
    - name: delete topic
      kafka_lib:
        resource: 'topic'
        name: 'test'
        state: 'absent'
        zookeeper: >
          "{{ hostvars['zk']['ansible_eth0']['ipv4']['address'] }}:2181"
        bootstrap_servers: >
          "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,
          {{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

    # create an ACL for all topics
    - name: create acl
      kafka_lib:
        resource: 'acl'
        acl_resource_type: "topic"
        name: "*"
        acl_principal: "User:Alice"
        acl_operation: "write"
        acl_permission: "allow"
        state: "present"
        bootstrap_servers: >
          "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,
          {{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

    # delete an ACL for a single topic `test`
    - name: delete acl
      kafka_lib:
        resource: 'acl'
        acl_resource_type: "topic"
        name: "test"
        acl_principal: "User:Bob"
        acl_operation: "write"
        acl_permission: "allow"
        state: "absent"
        bootstrap_servers: >
          "{{ hostvars['kafka1']['ansible_eth0']['ipv4']['address'] }}:9092,
          {{ hostvars['kafka2']['ansible_eth0']['ipv4']['address'] }}:9092"

'''

# KAFKA PROTOCOL RESPONSES DEFINITION


class AlterConfigsResponse_v0(Response):
    """
    AlterConfigs version 0 from Kafka protocol
    Response serialization
    """
    API_KEY = 33
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('resources', Array(
            ('error_code', Int16),
            ('error_message', String('utf-8')),
            ('resource_type', Int8),
            ('resource_name', String('utf-8'))))
    )


class DescribeConfigsResponse_v0(Response):
    """
    DescribeConfigs version 0 from Kafka protocol
    Response serialization
    """
    API_KEY = 32
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('resources', Array(
            ('error_code', Int16),
            ('error_message', String('utf-8')),
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_entries', Array(
                ('config_name', String('utf-8')),
                ('config_value', String('utf-8')),
                ('read_only', Boolean),
                ('is_default', Boolean),
                ('is_sensitive', Boolean)))))
    )


class DescribeConfigsResponse_v1(Response):
    """
    DescribeConfigs version 1 from Kafka protocol
    Response serialization
    """
    API_KEY = 32
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('resources', Array(
            ('error_code', Int16),
            ('error_message', String('utf-8')),
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_entries', Array(
                ('config_name', String('utf-8')),
                ('config_value', String('utf-8')),
                ('read_only', Boolean),
                ('config_source', Int8),
                ('is_sensitive', Boolean),
                ('config_synonyms', Array(
                    ('config_name', String('utf-8')),
                    ('config_value', String('utf-8')),
                    ('config_source', Int8)
                ))))))
    )


class LeaderAndIsrResponse_v0(Response):
    """
    LeaderAndIsr version 0 from Kafka protocol
    Response serialization
    """
    API_KEY = 4
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('partitions', Array(
            ('topic', String('utf-8')),
            ('partition', Int32),
            ('error_code', Int16)))
    )


class LeaderAndIsrResponse_v1(Response):
    """
    LeaderAndIsr version 1 from Kafka protocol
    Reponse serialization
    """
    API_KEY = 4
    API_VERSION = 1
    SCHEMA = LeaderAndIsrResponse_v0.SCHEMA

# KAFKA PROTOCOL REQUESTS DEFINITION


class DescribeConfigsRequest_v0(Request):
    """
    DescribeConfigs version 0 from Kafka protocol
    Request serialization
    """
    API_KEY = 32
    API_VERSION = 0
    RESPONSE_TYPE = DescribeConfigsResponse_v0
    SCHEMA = Schema(
        ('resources', Array(
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_names', Array(String('utf-8')))))
    )


class DescribeConfigsRequest_v1(Request):
    """
    DescribeConfigs version 1 from Kafka protocol
    Request serialization
    """
    API_KEY = 32
    API_VERSION = 1
    RESPONSE_TYPE = DescribeConfigsResponse_v1
    SCHEMA = Schema(
        ('resources', Array(
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_names', Array(String('utf-8'))))),
        ('include_synonyms', Boolean)
    )


class AlterConfigsRequest_v0(Request):
    """
    AlterConfigs version 0 from Kafka protocol
    Request serialization
    """
    API_KEY = 33
    API_VERSION = 0
    RESPONSE_TYPE = AlterConfigsResponse_v0
    SCHEMA = Schema(
        ('resources', Array(
            ('resource_type', Int8),
            ('resource_name', String('utf-8')),
            ('config_entries', Array(
                ('config_name', String('utf-8')),
                ('config_value', String('utf-8')))))),
        ('validate_only', Boolean)
    )


class LeaderAndIsrRequest_v0(Request):
    """
    LeaderAndIsr version 0 from Kafka protocol
    Request serialization
    """
    API_KEY = 4
    API_VERSION = 0
    RESPONSE_TYPE = LeaderAndIsrResponse_v0
    SCHEMA = Schema(
        ('controller_id', Int32),
        ('controller_epoch', Int32),
        ('partition_states', Array(
            ('topic', String('utf-8')),
            ('partition', Int32),
            ('controller_epoch', Int32),
            ('leader', Int32),
            ('leader_epoch', Int32),
            ('isr', Int32),
            ('zk_version', Int32),
            ('replicas', Int32))),
        ('live_leaders', Array(
            ('id', Int32),
            ('host', String('utf-8')),
            ('port', Int32)))
    )


class LeaderAndIsrRequest_v1(Request):
    """
    LeaderAndIsr version 1 from Kafka protocol
    Request serialization
    """
    API_KEY = 4
    API_VERSION = 1
    RESPONSE_TYPE = LeaderAndIsrResponse_v1
    SCHEMA = Schema(
        ('controller_id', Int32),
        ('controller_epoch', Int32),
        ('partition_states', Array(
            ('topic', String('utf-8')),
            ('partition', Int32),
            ('controller_epoch', Int32),
            ('leader', Int32),
            ('leader_epoch', Int32),
            ('isr', Int32),
            ('zk_version', Int32),
            ('replicas', Int32),
            ('is_new', Boolean))),
        ('live_leaders', Array(
            ('id', Int32),
            ('host', String('utf-8')),
            ('port', Int32)))
    )


class CreatePartitionsRequest_v0(Request):
    """
    CreatePartitionsRequest version 0 from Kafka protocol
    Request serialization
    kafka-python's class is wrong (fixed in 1.4.3)
    """
    API_KEY = 37
    API_VERSION = 0
    RESPONSE_TYPE = CreatePartitionsResponse_v0
    SCHEMA = Schema(
        ('topic_partitions', Array(
            ('topic', String('utf-8')),
            ('new_partitions', Schema(
                ('count', Int32),
                ('assignment', Array(Array(Int32))))))),
        ('timeout', Int32),
        ('validate_only', Boolean)
    )


def generate_ssl_object(module, ssl_cafile, ssl_certfile, ssl_keyfile,
                        ssl_crlfile=None):
    """
    Generates a dict object that is used when dealing with ssl connection.
    When values given are file content, it takes care of temp file creation.
    """

    ssl_files = {
        'cafile': {'path': ssl_cafile, 'is_temp': False},
        'certfile': {'path': ssl_certfile, 'is_temp': False},
        'keyfile': {'path': ssl_keyfile, 'is_temp': False},
        'crlfile': {'path': ssl_crlfile, 'is_temp': False}
    }

    for key, value in ssl_files.items():
        if value['path'] is not None:
            # TODO is that condition sufficient?
            if value['path'].startswith("-----BEGIN"):
                # value is a content, need to create a tempfile
                fd, path = tempfile.mkstemp(prefix=key)
                with os.fdopen(fd, 'w') as tmp:
                    tmp.write(value['path'])
                ssl_files[key]['path'] = path
                ssl_files[key]['is_temp'] = True
            elif not os.path.exists(os.path.dirname(value['path'])):
                # value is not a content, but path does not exist,
                # fails the module
                module.fail_json(
                    msg='\'%s\' is not a content and provided path does not '
                        'exist, please check your SSL configuration.' % key
                )

    return ssl_files


class ACLResourceType(IntEnum):
    """An enumerated type of config resources"""

    ANY = 1,
    BROKER = 4,
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
        elif name.lower() == "broker":
            return ACLResourceType.BROKER
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
        self.name = name
        self.principal = principal
        self.host = host

    def __repr__(self):
        return "ACLResource(resource_type: %s, operation: %s, " \
               "permission_type: %s, name: %s, principal: %s, host: %s)"\
               % (self.resource_type, self.operation,
                  self.permission_type, self.name, self.principal, self.host)


class KafkaManager:
    """
    A class used to interact with Kafka and Zookeeper
    and easily retrive useful information
    """

    MAX_RETRY = 10
    MAX_POLL_RETRIES = 3
    MAX_ZK_RETRIES = 5
    TOPIC_RESOURCE_ID = 2
    DEFAULT_TIMEOUT = 15000
    SUCCESS_CODE = 0
    ZK_REASSIGN_NODE = '/admin/reassign_partitions'
    ZK_TOPIC_PARTITION_NODE = '/brokers/topics/'
    ZK_TOPIC_CONFIGURATION_NODE = '/config/topics/'

    # Not used yet.
    ZK_TOPIC_DELETION_NODE = '/admin/delete_topics/'

    def __init__(self, module, **configs):
        self.module = module
        self.zk_client = None
        self.client = KafkaClient(**configs)

    def init_zk_client(self, **configs):
        """
        Zookeeper client initialization
        """
        self.zk_client = KazooClient(**configs)
        self.zk_client.start()

    def close_zk_client(self):
        """
        Closes Zookeeper client
        """
        self.zk_client.stop()

    def close(self):
        """
        Closes Kafka client
        """
        self.client.close()

    def create_topic(self, name, partitions, replica_factor,
                     replica_assignment=[], config_entries=[],
                     timeout=None):
        """
        Creates a topic
        Usable for Kafka version >= 0.10.1
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        request = CreateTopicsRequest_v0(
            create_topic_requests=[(
                name, partitions, replica_factor, replica_assignment,
                config_entries
            )],
            timeout=timeout
        )
        response = self.send_request_and_get_response(request)

        for topic, error_code in response.topic_error_codes:
            if error_code != self.SUCCESS_CODE:
                self.close()
                self.module.fail_json(
                    msg='Error while creating topic %s. '
                    'Error key is %s, %s.' % (
                        topic, kafka.errors.for_code(error_code).message,
                        kafka.errors.for_code(error_code).description
                    )
                )

    def delete_topic(self, name, timeout=None):
        """
        Deletes a topic
        Usable for Kafka version >= 0.10.1
        Need to know which broker is controller for topic
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        request = DeleteTopicsRequest_v0(topics=[name], timeout=timeout)
        response = self.send_request_and_get_response(request)

        for topic, error_code in response.topic_error_codes:
            if error_code != self.SUCCESS_CODE:
                self.close()
                self.module.fail_json(
                    msg='Error while deleting topic %s. '
                    'Error key is: %s, %s. '
                    'Is option \'delete.topic.enable\' set to true on '
                    ' your Kafka server?' % (
                        topic, kafka.errors.for_code(error_code).message,
                        kafka.errors.for_code(error_code).description
                    )
                )

    @staticmethod
    def _convert_create_acls_resource_request_v0(acl_resource):
        if acl_resource.operation == ACLOperation.ANY:
            raise IllegalArgumentError("operation must not be ANY")
        if acl_resource.permission_type == ACLPermissionType.ANY:
            raise IllegalArgumentError("permission_type must not be ANY")

        return (
            acl_resource.resource_type,
            acl_resource.name,
            acl_resource.principal,
            acl_resource.host,
            acl_resource.operation,
            acl_resource.permission_type
        )

    @staticmethod
    def _convert_delete_acls_resource_request_v0(acl_resource):
        return (
            acl_resource.resource_type,
            acl_resource.name,
            acl_resource.principal,
            acl_resource.host,
            acl_resource.operation,
            acl_resource.permission_type
        )

    def describe_acls(self, acl_resource):
        """Describe a set of ACLs
        """

        request = DescribeAclsRequest_v0(
            resource_type=acl_resource.resource_type,
            resource_name=acl_resource.name,
            principal=acl_resource.principal,
            host=acl_resource.host,
            operation=acl_resource.operation,
            permission_type=acl_resource.permission_type
        )

        response = self.send_request_and_get_response(request)

        if response.error_code != self.SUCCESS_CODE:
            self.close()
            self.module.fail_json(
                msg='Error while describing ACL %s. '
                    'Error %s: %s.' % (
                        acl_resource, response.error_code,
                        response.error_message
                    )
            )

        return response.resources

    def create_acls(self, acl_resources):
        """Create a set of ACLs"""

        request = CreateAclsRequest_v0(
            creations=[self._convert_create_acls_resource_request_v0(
                acl_resource) for acl_resource in acl_resources]
        )

        response = self.send_request_and_get_response(request)

        for error_code, error_message in response.creation_responses:
            if error_code != self.SUCCESS_CODE:
                self.close()
                self.module.fail_json(
                    msg='Error while creating ACL %s. '
                    'Error %s: %s.' % (
                        acl_resources, error_code, error_message
                    )
                )

    def delete_acls(self, acl_resources):
        """Delete a set of ACLSs"""

        request = DeleteAclsRequest_v0(
            filters=[self._convert_delete_acls_resource_request_v0(
                acl_resource) for acl_resource in acl_resources]
        )

        response = self.send_request_and_get_response(request)

        for error_code, error_message, _ in response.filter_responses:
            if error_code != self.SUCCESS_CODE:
                self.close()
                self.module.fail_json(
                    msg='Error while deleting ACL %s. '
                    'Error %s: %s.' % (
                        acl_resources, error_code, error_message
                    )
                )

    def send_request_and_get_response(self, request):
        """
        Sends a Kafka protocol request and returns
        the associated response
        """
        try:
            node_id = self.get_controller()
        except Exception:
            self.module.fail_json(
                msg='Cannot determine a controller for your current Kafka '
                'server. Is your Kafka server running and available on '
                '\'%s\' with security protocol \'%s\'?' % (
                    self.client.config['bootstrap_servers'],
                    self.client.config['security_protocol']
                )
            )

        if self.connection_check(node_id):
            future = self.client.send(node_id, request)
            self.client.poll(future=future)
            if future.succeeded():
                return future.value
            else:
                self.close()
                self.module.fail_json(
                    msg='Error while sending request %s to Kafka server: %s.'
                    % (request, future.exception)
                )
        else:
            self.close()
            self.module.fail_json(
                msg='Connection is not ready, please check your client '
                'and server configurations.'
            )

    def get_controller(self):
        """
        Returns the current controller
        """
        node_id, _host, _port, _rack = self.client.cluster.controller
        return node_id

    def get_controller_id_for_topic(self, topic_name):
        """
        Returns current controller for topic
        """
        request = MetadataRequest_v1(topics=[topic_name])
        response = self.send_request_and_get_response(request)
        return response.controller_id

    def get_config_for_topic(self, topic_name, config_names):
        """
        Returns responses with configuration
        Usable with Kafka version >= 0.11.0
        """
        request = DescribeConfigsRequest_v0(
            resources=[(self.TOPIC_RESOURCE_ID, topic_name, config_names)]
        )
        return self.send_request_and_get_response(request)

    def get_responses_from_client(self, connection_sleep=1):
        """
        Obtains response from server using poll()
        It may need some times to get the response, so we had some retries
        """
        retries = 0
        if self.get_awaiting_request() > 0:
            while retries < self.MAX_POLL_RETRIES:
                resp = self.client.poll()
                if resp:
                    return resp
                time.sleep(connection_sleep)
                retries += 1
            self.close()
            self.module.fail_json(
                msg='Error while getting responses : no response to request '
                'was obtained, please check your client and server '
                'configurations.'
            )
        else:
            self.close()
            self.module.fail_json(
                msg='No pending request, please check your client and server '
                'configurations.'
            )

    def get_topics(self):
        """
        Returns the topics list
        """
        return self.client.cluster.topics()

    def get_total_partitions_for_topic(self, topic):
        """
        Returns the number of partitions for topic
        """
        return len(self.client.cluster.partitions_for_topic(topic))

    def get_partitions_for_topic(self, topic):
        """
        Returns all partitions for topic, with information
        TODO do not use private property anymore
        """
        return self.client.cluster._partitions[topic]

    def get_total_brokers(self):
        """
        Returns number of brokers available
        """
        return len(self.client.cluster.brokers())

    def get_brokers(self):
        """
        Returns all brokers
        """
        return self.client.cluster.brokers()

    def get_api_version(self):
        """
        Returns Kafka server version
        """
        major, minor, patch = self.client.config['api_version']
        return '%s.%s.%s' % (major, minor, patch)

    def get_awaiting_request(self):
        """
        Returns the number of requests currently in the queue
        """
        return self.client.in_flight_request_count()

    def connection_check(self, node_id, connection_sleep=0.1):
        """
        Checks that connection with broker is OK and that it is possible to
        send requests
        Since the _maybe_connect() function used in ready() is 'async', we
        need to manually call it several time to make the connection
        """
        retries = 0
        if not self.client.ready(node_id):
            while retries < self.MAX_RETRY:
                if self.client.ready(node_id):
                    return True
                time.sleep(connection_sleep)
                retries += 1
            return False
        return True

    def is_topic_configuration_need_update(self, topic_name, topic_conf):
        """
        Checks whether topic's options need to be updated or not.
        Since the DescribeConfigsRequest does not give all current
        configuration entries for a topic, we need to use Zookeeper.
        Requires zk connection.
        """
        current_config, _zk_stats = self.zk_client.get(
            self.ZK_TOPIC_CONFIGURATION_NODE + topic_name
        )
        current_config = json.loads(current_config)['config']

        if len(topic_conf) != len(current_config.keys()):
            return True
        else:
            for conf_name, conf_value in topic_conf:
                if (
                        conf_name not in current_config.keys() or
                        str(conf_value) != str(current_config[conf_name])
                ):
                    return True

        return False

    def is_topic_partitions_need_update(self, topic_name, partitions):
        """
        Checks whether topic's partitions need to be updated or not.
        """
        total_partitions = self.get_total_partitions_for_topic(topic_name)
        need_update = False

        if partitions != total_partitions:
            if partitions > total_partitions:
                # increasing partition number
                need_update = True
            else:
                # decreasing partition number, which is not possible
                self.close()
                self.module.fail_json(
                    msg='Can\'t update \'%s\' topic partition from %s to %s :'
                    'only increase is possible.' % (
                        topic_name, total_partitions, partitions
                        )
                )

        return need_update

    def is_topic_replication_need_update(self, topic_name, replica_factor):
        """
        Checks whether a topic replica needs to be updated or not.
        """
        need_update = False
        for _id, part in self.get_partitions_for_topic(topic_name).items():
            _topic, _partition, _leader, replicas, _isr, _error = part
            if len(replicas) != replica_factor:
                need_update = True

        return need_update

    def update_topic_partitions(self, topic_name, partitions):
        """
        Updates the topic partitions
        Usable for Kafka version >= 1.0.0
        Requires to be the sended to the current controller of the Kafka
        cluster.
        The request requires to precise the total number of partitions and
        broker assignment for each new partition without forgeting replica.
        See NewPartitions class for explanations
        apache/kafka/clients/admin/NewPartitions.java#L53
        """
        brokers = []
        for node_id, _, _, _ in self.get_brokers():
            brokers.append(int(node_id))
        brokers_iterator = itertools.cycle(brokers)
        topic, _, _, replicas, _, _ = (
            self.get_partitions_for_topic(topic_name)[0]
        )
        total_replica = len(replicas)
        old_partition = self.get_total_partitions_for_topic(topic_name)
        assignments = []
        for _new_partition in range(partitions - old_partition):
            assignment = []
            for _replica in range(total_replica):
                assignment.append(next(brokers_iterator))
            assignments.append(assignment)

        request = CreatePartitionsRequest_v0(
            topic_partitions=[(topic_name, (partitions, assignments))],
            timeout=self.DEFAULT_TIMEOUT,
            validate_only=False
        )
        response = self.send_request_and_get_response(request)
        for topic, error_code, _error_message in response.topic_errors:
            if error_code != self.SUCCESS_CODE:
                self.close()
                self.module.fail_json(
                    msg='Error while updating topic \'%s\' partitions. '
                    'Error key is %s, %s. Request was %s.' % (
                        topic, kafka.errors.for_code(error_code).message,
                        kafka.errors.for_code(error_code).description,
                        str(request)
                    )
                )

    def update_topic_configuration(self, topic_name, topic_conf):
        """
        Updates the topic configuration
        Usable for Kafka version >= 0.11.0
        Requires to be the sended to the current controller of the Kafka
        cluster.
        """
        request = AlterConfigsRequest_v0(
            resources=[(self.TOPIC_RESOURCE_ID, topic_name, topic_conf)],
            validate_only=False
        )
        response = self.send_request_and_get_response(request)

        for error_code, _, _, resource_name in response.resources:
            if error_code != self.SUCCESS_CODE:
                self.close()
                self.module.fail_json(
                    msg='Error while updating topic \'%s\' configuration. '
                    'Error key is %s, %s' % (
                        resource_name,
                        kafka.errors.for_code(error_code).message,
                        kafka.errors.for_code(error_code).description
                    )
                )

    def get_assignment_for_replica_factor_update(self, topic_name,
                                                 replica_factor):
        """
        Generates a json assignment based on replica_factor given to update
        replicas for a topic.
        Uses all brokers available and distributes them as replicas using
        a round robin method.
        """
        all_replicas = []
        assign = {'partitions': [], 'version': 1}

        if replica_factor > self.get_total_brokers():
            self.close()
            self.close_zk_client()
            self.module.fail_json(
                msg='Error while updating topic \'%s\' replication factor : '
                'replication factor \'%s\' is more than available brokers '
                '\'%s\'' % (
                    topic_name,
                    replica_factor,
                    self.get_total_brokers()
                )
            )
        else:
            for node_id, _, _, _ in self.get_brokers():
                all_replicas.append(node_id)
            brokers_iterator = itertools.cycle(all_replicas)
            for _, part in self.get_partitions_for_topic(topic_name).items():
                _, partition, _, _, _, _ = part
                assign_tmp = {
                    'topic': topic_name,
                    'partition': partition,
                    'replicas': []
                }
                for _i in range(replica_factor):
                    assign_tmp['replicas'].append(next(brokers_iterator))
                assign['partitions'].append(assign_tmp)

            return bytes(str(json.dumps(assign)).encode('ascii'))

    def get_assignment_for_partition_update(self, topic_name, partitions):
        """
        Generates a json assignment based on number of partitions given to
        update partitions for a topic.
        Uses all brokers available and distributes them among partitions
        using a round robin method.
        """
        all_brokers = []
        assign = {'partitions': {}, 'version': 1}

        _, _, _, replicas, _, _ = self.get_partitions_for_topic(topic_name)[0]
        total_replica = len(replicas)

        for node_id, _host, _port, _rack in self.get_brokers():
            all_brokers.append(node_id)
        brokers_iterator = itertools.cycle(all_brokers)

        for i in range(partitions):
            assign_tmp = []
            for _j in range(total_replica):
                assign_tmp.append(next(brokers_iterator))
            assign['partitions'][str(i)] = assign_tmp

        return bytes(str(json.dumps(assign)).encode('ascii'))

    def update_admin_assignment(self, json_assignment, zk_sleep_time,
                                zk_max_retries):
        """
Updates the topic replica factor using a json assignment
Cf core/src/main/scala/kafka/admin/ReassignPartitionsCommand.scala#L580
 1 - Send AlterReplicaLogDirsRequest to allow broker to create replica in
     the right log dir later if the replica has not been created yet.

  2 - Create reassignment znode so that controller will send
      LeaderAndIsrRequest to create replica in the broker
      def path = "/admin/reassign_partitions" ->
      zk.create("/admin/reassign_partitions", b"a value")
  case class ReplicaAssignment(
    @BeanProperty @JsonProperty("topic") topic: String,
    @BeanProperty @JsonProperty("partition") partition: Int,
    @BeanProperty @JsonProperty("replicas") replicas: java.util.List[Int])
  3 - Send AlterReplicaLogDirsRequest again to make sure broker will start
      to move replica to the specified log directory.
     It may take some time for controller to create replica in the broker
     Retry if the replica has not been created.
 It may be possible that the node '/admin/reassign_partitions' is already
 there for another topic. That's why we need to check for its existence
 and wait for its consumption if it is already present.
 Requires zk connection.
        """
        retries = 0
        while (
                self.zk_client.exists(self.ZK_REASSIGN_NODE) and
                retries < zk_max_retries
        ):
            retries += 1
            time.sleep(zk_sleep_time)
        if retries >= zk_max_retries:
            self.close()
            self.close_zk_client()
            self.module.fail_json(
                msg='Error while updating assignment: zk node %s is already '
                'there after %s retries and not yet consumed, giving up. '
                'You should consider increasing the "zookeeper_max_retries" '
                'and/or "zookeeper_sleep_time" parameters.' % (
                    self.ZK_REASSIGN_NODE,
                    zk_max_retries
                )
            )
        self.zk_client.create(self.ZK_REASSIGN_NODE, json_assignment)

    def update_topic_assignment(self, json_assignment, zknode):
        """
 Updates the topic partition assignment using a json assignment
 Used when Kafka version < 1.0.0
 Requires zk connection.
        """
        if not self.zk_client.exists(zknode):
            self.close()
            self.close_zk_client()
            self.module.fail_json(
                msg='Error while updating assignment: zk node %s missing. '
                'Is the topic name correct?' % (zknode)
            )
        self.zk_client.set(zknode, json_assignment)


def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def main():
    """
    Module usage
    """

    module = AnsibleModule(
        argument_spec=dict(
            # resource managed, more to come (acl,broker)
            resource=dict(choices=['topic', 'acl'], default='topic'),

            # resource name
            name=dict(type='str', required=True),

            partitions=dict(type='int', required=False, default=0),

            replica_factor=dict(type='int', required=False, default=0),

            acl_resource_type=dict(choices=['topic', 'broker',
                                            'delegation_token', 'group',
                                            'transactional_id'],
                                   default='topic'),

            acl_principal=dict(type='str', required=False),

            acl_operation=dict(choices=['all', 'alter', 'alter_configs',
                                        'cluster_actions', 'create', 'delete',
                                        'describe', 'describe_configs',
                                        'idempotent_write', 'read', 'write'],
                               required=False),

            acl_permission=dict(choices=['allow', 'deny'], default='allow'),

            acl_host=dict(type='str', required=False, default="*"),

            state=dict(choices=['present', 'absent'], default='present'),

            options=dict(required=False, type='dict', default=None),

            zookeeper=dict(type='str', required=False),

            zookeeper_auth_scheme=dict(
                choices=['digest', 'sasl'],
                default='digest'
            ),

            zookeeper_auth_value=dict(
                type='str',
                no_log=True,
                required=False,
                default=''
            ),

            zookeeper_ssl_check_hostname=dict(
                default=True,
                type='bool',
                required=False
            ),

            zookeeper_ssl_cafile=dict(
                required=False,
                default=None,
                type='path'
            ),

            zookeeper_ssl_certfile=dict(
                required=False,
                default=None,
                type='path'
            ),

            zookeeper_ssl_keyfile=dict(
                required=False,
                default=None,
                no_log=True,
                type='path'
            ),

            zookeeper_ssl_password=dict(
                type='str',
                no_log=True,
                required=False
            ),

            zookeeper_sleep_time=dict(type='int', required=False, default=5),

            zookeeper_max_retries=dict(type='int', required=False, default=5),

            bootstrap_servers=dict(type='str', required=True),

            security_protocol=dict(
                choices=['PLAINTEXT', 'SSL', 'SASL_SSL', 'SASL_PLAINTEXT'],
                default='PLAINTEXT'
            ),

            api_version=dict(type='str', required=True, default=None),

            ssl_check_hostname=dict(
                default=True,
                type='bool',
                required=False
            ),

            ssl_cafile=dict(required=False, default=None, type='path'),

            ssl_certfile=dict(required=False, default=None, type='path'),

            ssl_keyfile=dict(
                required=False,
                default=None,
                no_log=True,
                type='path'
            ),

            ssl_password=dict(type='str', no_log=True, required=False),

            ssl_crlfile=dict(required=False, default=None, type='path'),

            # only PLAIN is currently available
            sasl_mechanism=dict(choices=['PLAIN', 'GSSAPI'], default='PLAIN'),

            sasl_plain_username=dict(type='str', required=False),

            sasl_plain_password=dict(type='str', no_log=True, required=False),

            sasl_kerberos_service_name=dict(type='str', required=False),
        ),
        supports_check_mode=True
    )

    params = module.params

    resource = params['resource']
    name = params['name']
    partitions = params['partitions']
    replica_factor = params['replica_factor']
    state = params['state']
    zookeeper = params['zookeeper']
    zookeeper_auth_scheme = params['zookeeper_auth_scheme']
    zookeeper_auth_value = params['zookeeper_auth_value']
    zookeeper_ssl_check_hostname = params['zookeeper_ssl_check_hostname']
    zookeeper_ssl_cafile = params['zookeeper_ssl_cafile']
    zookeeper_ssl_certfile = params['zookeeper_ssl_certfile']
    zookeeper_ssl_keyfile = params['zookeeper_ssl_keyfile']
    zookeeper_ssl_password = params['zookeeper_ssl_password']
    zookeeper_sleep_time = params['zookeeper_sleep_time']
    zookeeper_max_retries = params['zookeeper_max_retries']
    bootstrap_servers = params['bootstrap_servers']
    security_protocol = params['security_protocol']
    ssl_check_hostname = params['ssl_check_hostname']
    ssl_cafile = params['ssl_cafile']
    ssl_certfile = params['ssl_certfile']
    ssl_keyfile = params['ssl_keyfile']
    ssl_password = params['ssl_password']
    ssl_crlfile = params['ssl_crlfile']
    sasl_mechanism = params['sasl_mechanism']
    sasl_plain_username = params['sasl_plain_username']
    sasl_plain_password = params['sasl_plain_password']
    sasl_kerberos_service_name = params['sasl_kerberos_service_name']
    acl_resource_type = params['acl_resource_type']
    acl_principal = params['acl_principal']
    acl_operation = params['acl_operation']
    acl_permission = params['acl_permission']
    acl_host = params['acl_host']

    api_version = tuple(
        int(p) for p in params['api_version'].strip(".").split(".")
    )

    options = []
    if params['options'] is not None:
        options = params['options'].items()

    kafka_ssl_files = generate_ssl_object(module, ssl_cafile,
                                          ssl_certfile, ssl_keyfile,
                                          ssl_crlfile)
    zookeeper_ssl_files = generate_ssl_object(module, zookeeper_ssl_cafile,
                                              zookeeper_ssl_certfile,
                                              zookeeper_ssl_keyfile)
    zookeeper_use_ssl = bool(
        zookeeper_ssl_files['keyfile']['path'] is not None and
        zookeeper_ssl_files['certfile']['path'] is not None
    )

    zookeeper_auth = []
    if zookeeper_auth_value != '':
        auth = (zookeeper_auth_scheme, zookeeper_auth_value)
        zookeeper_auth.append(auth)

    try:
        manager = KafkaManager(
            module=module, bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol, api_version=api_version,
            ssl_check_hostname=ssl_check_hostname,
            ssl_cafile=kafka_ssl_files['cafile']['path'],
            ssl_certfile=kafka_ssl_files['certfile']['path'],
            ssl_keyfile=kafka_ssl_files['keyfile']['path'],
            ssl_password=ssl_password,
            ssl_crlfile=kafka_ssl_files['crlfile']['path'],
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_kerberos_service_name=sasl_kerberos_service_name)
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Error while initializing Kafka client : %s ' % str(e)
        )

    changed = False

    if parse_version(manager.get_api_version()) < parse_version('0.11.0'):
        module.fail_json(
            msg='Current version of library is not compatible with '
            'Kafka < 0.11.0.'
        )

    msg = '%s \'%s\': ' % (resource, name)

    if resource == 'topic':
        if state == 'present':
            if name in manager.get_topics():
                # topic is already there
                if zookeeper != '' and partitions > 0 and replica_factor > 0:
                    try:
                        manager.init_zk_client(
                            hosts=zookeeper, auth_data=zookeeper_auth,
                            keyfile=zookeeper_ssl_files['keyfile']['path'],
                            use_ssl=zookeeper_use_ssl,
                            keyfile_password=zookeeper_ssl_password,
                            certfile=zookeeper_ssl_files['certfile']['path'],
                            ca=zookeeper_ssl_files['cafile']['path'],
                            verify_certs=zookeeper_ssl_check_hostname
                            )
                    except Exception:
                        e = get_exception()
                        module.fail_json(
                            msg='Error while initializing Zookeeper client : '
                            '%s. Is your Zookeeper server available and '
                            'running on \'%s\'?' % (str(e), zookeeper)
                        )

                    if manager.is_topic_configuration_need_update(name,
                                                                  options):
                        if not module.check_mode:
                            manager.update_topic_configuration(name, options)
                        changed = True

                    if manager.is_topic_replication_need_update(
                            name, replica_factor
                    ):
                        json_assignment = (
                            manager.get_assignment_for_replica_factor_update(
                                name, replica_factor
                            )
                        )
                        if not module.check_mode:
                            manager.update_admin_assignment(
                                json_assignment,
                                zookeeper_sleep_time,
                                zookeeper_max_retries
                            )
                        changed = True

                    if manager.is_topic_partitions_need_update(
                            name, partitions
                    ):
                        cur_version = parse_version(manager.get_api_version())
                        if not module.check_mode:
                            if cur_version < parse_version('1.0.0'):
                                json_assignment = (
                                    manager.get_assignment_for_partition_update
                                    (name, partitions)
                                )
                                zknode = '/brokers/topics/%s' % name
                                manager.update_topic_assignment(
                                    json_assignment,
                                    zknode
                                )
                            else:
                                manager.update_topic_partitions(name,
                                                                partitions)
                        changed = True
                    manager.close_zk_client()
                    if changed:
                        msg += 'successfully updated.'
                else:
                    module.fail_json(
                        msg='\'zookeeper\', \'partitions\' and '
                        '\'replica_factor\' parameters are needed when '
                        'parameter \'state\' is \'present\''
                    )
            else:
                # topic is absent
                if not module.check_mode:
                    manager.create_topic(name=name, partitions=partitions,
                                         replica_factor=replica_factor,
                                         config_entries=options)
                changed = True
                msg += 'successfully created.'
        elif state == 'absent':
            if name in manager.get_topics():
                # delete topic
                if not module.check_mode:
                    manager.delete_topic(name)
                changed = True
                msg += 'successfully deleted.'
    elif resource == 'acl':

        if not acl_operation:
            module.fail_json(msg="acl_operation is required")

        acl_resource = ACLResource(
                resource_type=ACLResourceType.from_name(acl_resource_type),
                operation=ACLOperation.from_name(acl_operation),
                permission_type=ACLPermissionType.from_name(acl_permission),
                name=name,
                principal=acl_principal,
                host=acl_host)

        acl_resource_found = manager.describe_acls(acl_resource)

        if state == 'present':
            if not acl_resource_found:
                if not module.check_mode:
                    manager.create_acls([acl_resource])
                changed = True
                msg += 'successfully created.'
        elif state == 'absent':
            if acl_resource_found:
                if not module.check_mode:
                    manager.delete_acls([acl_resource])
                changed = True
                msg += 'successfully deleted.'

    manager.close()
    for _key, value in merge_dicts(
        kafka_ssl_files, zookeeper_ssl_files
    ).items():
        if (
                value['path'] is not None and value['is_temp'] and
                os.path.exists(os.path.dirname(value['path']))
        ):
            os.remove(value['path'])

    if not changed:
        msg += 'nothing to do.'

    module.exit_json(changed=changed, msg=msg)


if __name__ == '__main__':
    main()
