#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for topic configuration management
"""
from __future__ import absolute_import, division, print_function
__metaclass__ = type

# Init logging
import logging
import sys

from ansible.module_utils.basic import AnsibleModule

from ansible.module_utils.kafka_lib_acl import process_module_acl
from ansible.module_utils.kafka_lib_topic import process_module_topic

from ansible.module_utils.kafka_lib_commons import (
    module_commons, module_acl_commons, module_zookeeper_commons,
    module_topic_commons, DOCUMENTATION_COMMON
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
     - Deprecated
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
  kafka_sleep_time:
    description:
      - 'when updating number of partitions and while checking for'
      - 'kafka to applied, the time to sleep (in seconds) between'
      - 'each checks.'
      default: 5
  kafka_max_retries:
    description:
      - 'when updating number of partitions and while checking for'
      - 'kafka to applied, maximum of try to do before failing'
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


def main():
    """
    Module usage
    """
    spec = dict(
        # resource managed, more to come (broker)
        resource=dict(choices=['topic', 'acl'], default='topic'),

        # resource name
        name=dict(type='str', required=True),

        state=dict(choices=['present', 'absent'], default='present'),

        **module_commons
    )
    spec.update(module_topic_commons)
    spec.update(module_acl_commons)
    spec.update(module_zookeeper_commons)

    module = AnsibleModule(
        argument_spec=spec,
        supports_check_mode=True
    )

    params = module.params

    resource = params['resource']

    module.deprecate(
        'Usage of "kafka_lib" is deprecated, please use "kafka_topic"'
        'or "kafka_acl" instead'
    )

    if resource == 'topic':
        process_module_topic(module)
    elif resource == 'acl':
        process_module_acl(module)


if __name__ == '__main__':
    main()
