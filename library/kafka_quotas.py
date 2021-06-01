#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for akfka quotas management
"""
from __future__ import absolute_import, division, print_function
__metaclass__ = type

# Init logging
import logging
import sys

from ansible.module_utils.basic import AnsibleModule

from ansible.module_utils.kafka_lib_quotas import process_module_quotas

from ansible.module_utils.kafka_lib_commons import (
    module_commons, module_zookeeper_commons,
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
module: kafka_quotas
short_description: Manage Kafka quotas
description:
     - Configure Kafka quotas.
     - Not compatible avec Kafka version < 0.11.0.
author:
    - Stephen SORRIAUX
    - ryarnyah
options:
  entries:
    description:
      - Entries to manage.
  mark_others_as_absent:
    description:
      - make non listed topics as absent, thus triggering the deletion
      - of topics absent from the `topics` listing
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

    # add quotas
    - name: create quotas
      kafka_quotas:
        entries:
        - entity:
            user: test
          quotas:
            producer_byte_rate: 1048576
            consumer_byte_rate: 1048576
            request_percentage: 55
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
        entries=dict(
            type='list',
            elements='dict',
            required=True,
            options=dict(
                entity=dict(
                    type='dict',
                    required=True,
                    required_one_of=[('user', 'client')],
                    options=dict(
                        user=dict(type='str'),
                        client=dict(type='str')
                    )
                ),
                quotas=dict(
                    type='dict',
                    required=True,
                    options=dict(
                        producer_byte_rate=dict(type='int'),
                        consumer_byte_rate=dict(type='int'),
                        request_percentage=dict(type='float')
                    ))
            )
        ),
        **module_commons
    )
    spec.update(module_zookeeper_commons)

    module = AnsibleModule(
        argument_spec=spec,
        supports_check_mode=True
    )

    process_module_quotas(module)


if __name__ == '__main__':
    main()
