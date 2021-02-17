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

from ansible.module_utils.kafka_lib_topic import process_module_topic

from ansible.module_utils.kafka_lib_commons import (
    module_commons, module_zookeeper_commons, module_topic_commons,
    DOCUMENTATION_COMMON
)
# Default logging
# TODO: refactor all this logging logic
log = logging.getLogger('kafka')
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.INFO)

ANSIBLE_METADATA = {'metadata_version': '1.0'}


DOCUMENTATION = '''
---
module: kafka_topic
short_description: Manage Kafka topic
description:
     - Configure Kafka topic.
     - Not compatible avec Kafka version < 0.11.0.
author:
    - Stephen SORRIAUX
options:
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

'''


def main():
    """
    Module usage
    """
    spec = dict(
        # resource name
        name=dict(type='str', required=True),

        state=dict(choices=['present', 'absent'], default='present'),

        **module_commons
    )
    spec.update(module_topic_commons)
    spec.update(module_zookeeper_commons)

    module = AnsibleModule(
        argument_spec=spec,
        supports_check_mode=True
    )

    process_module_topic(module)


if __name__ == '__main__':
    main()
