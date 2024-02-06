#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for consumer group
"""
from __future__ import absolute_import, division, print_function

__metaclass__ = type

from pkg_resources import parse_version

# XXX: fix kafka-python import broken for Python 3.12
import ansible.module_utils.kafka_fix_import  # noqa

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.pycompat24 import get_exception
from kafka.errors import KafkaError

from ansible.module_utils.kafka_lib_commons import (
    module_commons, DOCUMENTATION_COMMON, get_manager_from_params,
    maybe_clean_kafka_ssl_files
)

ANSIBLE_METADATA = {'metadata_version': '1.0'}

DOCUMENTATION = '''
---
module: kafka_consumer_group
short_description: Interact with kafka consumer groups
description:
     - Interact with kafka consumer groups.
     - Not compatible with Kafka version < 2.4.0.
author:
    - Yassine MILHI
options:
  topics:
    descritption:
      - 'consumed topics partitions on which action will be performed'
    required: True
  consumer_group:
    description:
      - 'one consumer group name.'
    required: True
  action:
    description:
      - 'action to apply (alter / delete).'
    required: True
''' + DOCUMENTATION_COMMON

EXAMPLES = '''
    - name:  Delete offset for consumer group
    kafka_consumer_group:
        consumer_group: "{{ consumer_group | default('lolo_consumer')}}"
        topics:
        - name: lolo # mandatory
          partitions: [0, 1, 2] # Optional
        action: delete
        bootstrap_servers: "{{ ansible_ssh_host }}:9094"
        api_version: "{{ kafka_api_version }}"
        sasl_mechanism: "PLAIN"
        security_protocol: "SASL_SSL"
        sasl_plain_username: "admin"
        sasl_plain_password: "{{ kafka_admin_password }}"
        ssl_check_hostname: False
        ssl_cafile: "{{ kafka_cacert | default('/etc/ssl/certs/cacert.crt') }}"
'''


def main():
    module = AnsibleModule(
        argument_spec=dict(
            consumer_group=dict(type='str', required=True),
            topics=dict(
                type='list',
                elements='dict',
                required=True,
                options=dict(
                    name=dict(type='str', required=True),
                    partitions=dict(type='list', elements='int',
                                    required=False)
                )),
            action=dict(type='str', required=True,
                        required_one_of=[('delete')]),

            **module_commons
        ),
        supports_check_mode=True
    )

    params = module.params
    consumer_group = params['consumer_group']
    topics = params['topics']
    action = params['action']
    manager = None

    try:
        manager = get_manager_from_params(params)
        api_version = parse_version(manager.get_api_version())
        if api_version < parse_version('2.4.0'):
            module.fail_json(
                msg='Delete offset API provided on kafka 2.4.0 (KIP-496)'
                + ' current version %s' % str(manager.get_api_version())
            )
        if action == 'delete':
            changed = manager.delete_group_offset(consumer_group, topics,
                                                  module.check_mode)
    except KafkaError:
        e = get_exception()
        module.fail_json(
            msg='Error while deleting kafka consumer group offset: %s' % e
        )
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Something went wrong: %s ' % e
        )
    finally:
        if manager:
            manager.close()
        maybe_clean_kafka_ssl_files(params)

    if changed:
        msg = 'topics and partitions (%s) successfully deleted ' \
              'for consumer group (%s)' % (topics, consumer_group)
    else:
        msg = 'nothing to do for consumer group %s and topics ' \
              'partitions (%s)' % (consumer_group, topics)

    module.exit_json(changed=changed, msg=msg)


if __name__ == '__main__':
    main()
