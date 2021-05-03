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

from ansible.module_utils.kafka_lib_acl import process_module_acls

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
module: kafka_acls
short_description: Manage Kafka ACLs
description:
     - Configure Kafka ACLs.
     - Not compatible avec Kafka version < 0.11.0.
author:
    - Stephen SORRIAUX
    - ryarnyah
options:
  acls:
    description:
      - acls to manage. @See kafka_acl for options
  mark_others_as_absent:
    description:
      - make non listed acls as absent, thus triggering the deletion
      - of ACLs absent from the `acls` listing
''' + DOCUMENTATION_COMMON

EXAMPLES = '''

    # create an ACL for all topics
    - name: create acls
      kafka_acls:
        acls:
          - acl_resource_type: "topic"
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
      kafka_acls:
        acls:
          - acl_resource_type: "topic"
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
        mark_others_as_absent=dict(type='bool', default=False),
        acls=dict(
            type='list',
            elements='dict',
            required=True,
            options=dict(
                name=dict(type='str', required=True),
                state=dict(choices=['present', 'absent'], default='present'),
                **module_acl_commons
            )
        ),
        **module_commons
    )

    module = AnsibleModule(
        argument_spec=spec,
        supports_check_mode=True
    )
    process_module_acls(module)


if __name__ == '__main__':
    main()
