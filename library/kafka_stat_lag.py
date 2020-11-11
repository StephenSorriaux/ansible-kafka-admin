#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for consumer group statistics
"""
from __future__ import absolute_import, division, print_function

__metaclass__ = type

# import module snippets
import json

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.pycompat24 import get_exception
from kafka.errors import KafkaError

from ansible.module_utils.kafka_consumer_lag import KafkaConsumerLag
from ansible.module_utils.kafka_lib_commons import (
    module_commons, DOCUMENTATION_COMMON, get_manager_from_params,
    maybe_clean_kafka_ssl_files
)

ANSIBLE_METADATA = {'metadata_version': '1.0'}

DOCUMENTATION = '''
---
module: kafka_stat_lag
short_description: Gather kafka statistics
description:
     - Gather kafka statistics.
     - Not compatible with Kafka version < 0.11.0.
author:
    - Yassine MILHI
options:
  ignore_empty_partition:
    descritption:
      - 'ignore empty partition when calculating global lag'
    default: False
  consummer_group:
    description:
      - 'one consumer group name.'
    required: True
''' + DOCUMENTATION_COMMON

EXAMPLES = '''
    - name: Get kafka consumers LAG stats
    kafka_stat_lag:
        consummer_group: "{{ consummer_group | default('pra-mirror')}}"
        bootstrap_servers: "{{ ansible_ssh_host }}:9094"
        api_version: "{{ kafka_api_version }}"
        sasl_mechanism: "PLAIN"
        security_protocol: "SASL_SSL"
        sasl_plain_username: "admin"
        sasl_plain_password: "{{ kafka_admin_password }}"
        ssl_check_hostname: False
        ssl_cafile: "{{ kafka_cacert | default('/etc/ssl/certs/cacert.crt') }}"
        ignore_empty_partition: True
    register: result
    until:  (result.msg | from_json).global_lag_count == 0
    retries: 60
    delay: 2
'''


def main():
    module = AnsibleModule(
        argument_spec=dict(
            consummer_group=dict(type='str', required=True),

            ignore_empty_partition=dict(type='bool', default=False),

            **module_commons
        )
    )

    params = module.params
    consummer_group = params['consummer_group']
    ignore_empty_partition = params['ignore_empty_partition']

    try:
        manager = get_manager_from_params(params)
        klag = KafkaConsumerLag(manager.client)
        results = klag.get_lag_stats(consummer_group, ignore_empty_partition)
    except KafkaError:
        e = get_exception()
        module.fail_json(
            msg='Error while getting lag from Kafka: %s' % e
        )
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Seomthing went wrong: %s ' % e
        )
    finally:
        manager.close()
        maybe_clean_kafka_ssl_files(params)

    # XXX: do we really need a JSON serialized value?
    module.exit_json(changed=True, msg=json.dumps(results))


if __name__ == '__main__':
    main()
