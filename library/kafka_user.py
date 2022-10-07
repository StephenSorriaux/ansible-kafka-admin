#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for user configuration management
"""
from __future__ import absolute_import, division, print_function
__metaclass__ = type

# Init logging
import logging
import sys

from ansible.module_utils.basic import AnsibleModule

from ansible.module_utils.kafka_lib_user import (
  process_module_user, module_user_spec_commons
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
module: kafka_user
short_description: Manage Kafka Scram User
description:
     - Configure Kafka Scarm User.
     - Not compatible with Kafka version < 2.7.0.
author:
    - Simone Aiello
options:
  name:
    description:
      - 'name of the user'
    required: True
  password:
    description:
      - 'password of the user'
      - 'required when state is "present" or "update"'
    required: False
  mechanism:
    description:
      - 'SCRAM mechanism to use'
    required: True
  iterations:
    description:
      - 'iteration count for the selected hash function'
      - 'default to 4096'
    required: False
  state:
    description:
      - 'state of the user'
      - 'when "update" the user is always updated'
    default: present
    choices: [present, absent, update]
''' + DOCUMENTATION_COMMON

EXAMPLES = '''

    - name: "Create user 'alice' with defaults"
      kafka_user:
        name: "alice"
        password: "changeit"
        state: "present"
        bootstrap_servers: "localhost:9092"

    - name: "Create user 'bob' with several overrides"
      kafka_user:
        name: "bob"
        password: "changeit"
        mechanism: SCRAM-SHA-256
        iterations: 5098
        state: "present"
        bootstrap_servers: "localhost:9092"
'''


def main():
    """
    Module usage
    """
    spec = dict(
      **module_user_spec_commons
    )
    spec.update(module_commons)

    module = AnsibleModule(
        argument_spec=spec,
        supports_check_mode=True
    )

    process_module_user(module)


if __name__ == '__main__':
    main()
