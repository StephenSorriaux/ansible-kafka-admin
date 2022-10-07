#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for users configuration management
"""
from __future__ import absolute_import, division, print_function
__metaclass__ = type

# Init logging
import logging
import sys

from ansible.module_utils.basic import AnsibleModule

from ansible.module_utils.kafka_lib_user import (
  process_module_users, module_user_spec_commons
)

from ansible.module_utils.kafka_lib_commons import (
    module_commons, DOCUMENTATION_COMMON
)
# Default logging
# TODO: refactor all this logging logic
log = logging.getLogger('kafka')
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.INFO)

log = logging.getLogger('kazoo.client')
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.INFO)

ANSIBLE_METADATA = {'metadata_version': '1.0'}


DOCUMENTATION = '''
---
module: kafka_users
short_description: Manage Kafka users
description:
     - Configure Kafka users.
     - Not compatible avec Kafka version < 0.11.0.
author:
    - Stephen SORRIAUX
    - ryarnyah
options:
  mark_others_as_absent:
    description:
      - make non listed users as absent, thus triggering the deletion
      - of users absent from the `users` listing
  users:
    description:
      - users to create. @See kafka_user for options
  ... tbc ...
''' + DOCUMENTATION_COMMON

EXAMPLES = '''

    # creates users deleting the others
    - name: create users
      kafka_users:
        mark_others_as_absent: yes
        users:
          - name: 'user1'
            password: changeit
            mechanism: SCRAM-SHA-512
            state: 'present'
          - name: 'user2'
            mechanism: SCRAM-SHA-512
            state: 'absent'
        bootstrap_servers: localhost:9092

'''


def main():
    """
    Module usage
    """
    spec = dict(
        mark_others_as_absent=dict(type='bool', default=False),
        users=dict(
            type='list',
            elements='dict',
            required=True,
            options=dict(
                **module_user_spec_commons
            )
        ),
        **module_commons
    )

    module = AnsibleModule(
        argument_spec=spec,
        supports_check_mode=True
    )

    process_module_users(module, None)


if __name__ == '__main__':
    main()
