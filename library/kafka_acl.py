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

from ansible.module_utils.kafka_lib_commons import (
    module_commons, module_acl_commons,
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
module: kafka_acl
short_description: Manage Kafka ACL
description:
     - Configure Kafka ACL.
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
  state:
    description:
      - 'state of the managed resource.'
    default: present
    choices: [present, absent]
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
''' + DOCUMENTATION_COMMON

EXAMPLES = '''

    # create an ACL for all topics
    - name: create acl
      kafka_acl:
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
      kafka_acl:
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
        # resource name
        name=dict(type='str', required=True),

        state=dict(choices=['present', 'absent'], default='present'),

        **module_commons
    )
    spec.update(module_acl_commons)

    module = AnsibleModule(
        argument_spec=spec,
        supports_check_mode=True
    )
    process_module_acl(module)


if __name__ == '__main__':
    main()
