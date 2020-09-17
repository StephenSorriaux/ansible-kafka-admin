#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for topic configuration management
"""
from __future__ import absolute_import, division, print_function
__metaclass__ = type

# import module snippets
import os
from pkg_resources import parse_version

# Init logging
import logging
import sys

from kafka.errors import IllegalArgumentError

# enum in stdlib as of py3.4
try:
    from enum import IntEnum  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor.enum34 import IntEnum


from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.pycompat24 import get_exception

from ansible.module_utils.ssl_utils import generate_ssl_object
from ansible.module_utils.acl_operation import ACLOperation
from ansible.module_utils.acl_permission_type import ACLPermissionType
from ansible.module_utils.kafka_lib_commons import (
  module_commons, DOCUMENTATION_COMMON, get_manager_from_params,
  maybe_clean_kafka_ssl_files
)

# Default logging
# TODO: refactor all this logging logic
log = logging.getLogger('kafka')
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.INFO)

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
      - '"broker" is deprecated in favour of "cluster".'
    default: topic
    choices: [topic, broker, delegation_token, group, transactional_id,
                cluster]
  acl_principal:
    description:
      - 'the principal the ACL applies to.'
      - 'Example: User:Alice'
  acl_operation:
    description:
      - 'the operation the ACL controls.'
    choices: [all, alter, alter_configs, cluster_action, create, delete,
                describe, describe_configs, idempotent_write, read, write]
  acl_pattern_type:
    description:
      - 'the pattern type of the ACL. Need Kafka version >= 2.0.0'
    choices: [any, match, literal, prefixed]
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
''' + DOCUMENTATION_COMMON

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
        return "ACLResource(resource_type: %s, operation: %s, " \
               "permission_type: %s, name: %s, principal: %s, host: %s, " \
               "pattern_type: %s)" \
               % (self.resource_type, self.operation,
                  self.permission_type, self.name, self.principal, self.host,
                  self.pattern_type)


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

            acl_resource_type=dict(choices=['topic', 'broker', 'cluster',
                                            'delegation_token', 'group',
                                            'transactional_id'],
                                   default='topic'),

            acl_principal=dict(type='str', required=False),

            acl_operation=dict(choices=['all', 'alter', 'alter_configs',
                                        'cluster_action', 'create', 'delete',
                                        'describe', 'describe_configs',
                                        'idempotent_write', 'read', 'write'],
                               required=False),
            acl_pattern_type=dict(choice=['any', 'match', 'literal',
                                          'prefixed'],
                                  required=False, default='literal'),

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

            **module_commons
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

    acl_resource_type = params['acl_resource_type']
    acl_principal = params['acl_principal']
    acl_operation = params['acl_operation']
    acl_permission = params['acl_permission']
    acl_pattern_type = params['acl_pattern_type']
    acl_host = params['acl_host']

    options = []
    if params['options'] is not None:
        options = params['options'].items()

    zookeeper_ssl_files = generate_ssl_object(
      module, zookeeper_ssl_cafile, zookeeper_ssl_certfile,
      zookeeper_ssl_keyfile
    )
    zookeeper_use_ssl = bool(
        zookeeper_ssl_files['keyfile']['path'] is not None and
        zookeeper_ssl_files['certfile']['path'] is not None
    )

    zookeeper_auth = []
    if zookeeper_auth_value != '':
        auth = (zookeeper_auth_scheme, zookeeper_auth_value)
        zookeeper_auth.append(auth)

    manager = get_manager_from_params(module, params)

    changed = False
    msg = '%s \'%s\': ' % (resource, name)

    if resource == 'topic':
        if state == 'present':
            if name in manager.get_topics():

                # topic is already there
                if zookeeper == '':
                    module.fail_json(
                        msg='\'zookeeper\', parameter is needed when '
                        'parameter \'state\' is \'present\' for resource '
                        '\'topic\'.'
                    )

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

                if partitions > 0 and replica_factor > 0:
                    # partitions and replica_factor are set
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
                    # 0 or "default" (-1)
                    module.warn(
                      "Current values of 'partitions' (%s) and "
                      "'replica_factor' (%s) does not let this lib to "
                      "perform any action related to partitions and "
                      "replication. SKIPPING." % (partitions, replica_factor)
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

        api_version = parse_version(manager.get_api_version())

        if acl_resource_type.lower() == 'broker':
            module.deprecate(
              'Usage of "broker" is deprecated, please use "cluster" instead'
            )

        acl_resource = ACLResource(
                resource_type=ACLResourceType.from_name(acl_resource_type),
                operation=ACLOperation.from_name(acl_operation),
                permission_type=ACLPermissionType.from_name(acl_permission),
                pattern_type=ACLPatternType.from_name(acl_pattern_type),
                name=name,
                principal=acl_principal,
                host=acl_host)

        acl_resource_found = manager.describe_acls(acl_resource, api_version)

        if state == 'present':
            if not acl_resource_found:
                if not module.check_mode:
                    manager.create_acls([acl_resource], api_version)
                changed = True
                msg += 'successfully created.'
        elif state == 'absent':
            if acl_resource_found:
                if not module.check_mode:
                    manager.delete_acls([acl_resource], api_version)
                changed = True
                msg += 'successfully deleted.'

    manager.close()
    maybe_clean_kafka_ssl_files(module, params)

    for _key, value in zookeeper_ssl_files.items():
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
