#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Ansible module for Kafka partition reassignment using JSON assignments
"""
from __future__ import absolute_import, division, print_function
__metaclass__ = type

# Init logging
import logging
import sys

# XXX: fix kafka-python import broken for Python 3.12
import ansible.module_utils.kafka_fix_import  # noqa

from ansible.module_utils.basic import AnsibleModule

from ansible.module_utils.kafka_lib_commons import (
    get_manager_from_params,
    maybe_clean_kafka_ssl_files,
    maybe_clean_zk_ssl_files,
    module_commons,
    module_zookeeper_commons,
    DOCUMENTATION_COMMON
)

from ansible.module_utils.kafka_reassign_lib import ReassignmentManager

# Default logging
# TODO: refactor all this logging logic
# Redirect to stderr to avoid "junk after JSON data" warnings
log = logging.getLogger('kafka')
log.addHandler(logging.StreamHandler(sys.stderr))
log.setLevel(logging.WARNING)  # Only show warnings and errors

log = logging.getLogger('kazoo.client')
log.addHandler(logging.StreamHandler(sys.stderr))
log.setLevel(logging.WARNING)  # Only show warnings and errors

ANSIBLE_METADATA = {'metadata_version': '1.0'}


DOCUMENTATION = '''
---
module: kafka_reassign
short_description: Manage Kafka partition reassignment using JSON assignments
description:
     - Reassign Kafka topic partitions using custom JSON assignments.
     - Supports both Kafka >= 2.4.0 (new API) and older versions
       (ZooKeeper).
     - Useful for draining brokers, load balancing, or custom replica
       placement.
     - Not compatible with Kafka version < 0.11.0.
     - Zookeeper configuration useless if Kafka >= 2.4.0
author:
    - Stephen SORRIAUX
options:
  assignment:
    description:
      - 'JSON assignment for partition reassignment.'
      - 'Format: {"partitions": [{"topic": "topic_name", "partition": 0, '
        '"replicas": [1001, 1002]}]}'
      - 'Each partition must specify topic, partition number, and list of '
        'replica broker IDs.'
    required: True
    type: json
  wait_for_completion:
    description:
      - 'whether to wait for reassignment to complete before returning.'
      - 'when True, the module will wait until the reassignment is finished.'
    default: True
    type: bool
  validate_only:
    description:
      - 'when True, only validate the assignment without applying it.'
      - 'useful for pre-flight checks of assignment validity.'
    default: False
    type: bool
  cancel:
    description:
      - 'when True, cancel ongoing partition reassignments for the '
        'specified partitions.'
      - 'to cancel a reassignment, the replicas field should be set to '
        'None or omitted.'
      - 'only partitions with active reassignments will be affected.'
      - 'requires Kafka >= 2.4.0.'
    default: False
    type: bool
''' + DOCUMENTATION_COMMON

EXAMPLES = '''
    # Reassign partitions using JSON assignment
    - name: Reassign topic partitions
      kafka_reassign:
        bootstrap_servers: "kafka1:9092,kafka2:9092"
        assignment:
          partitions:
            - topic: "my_topic"
              partition: 0
              replicas: [1001, 1002]
            - topic: "my_topic"
              partition: 1
              replicas: [1002, 1003]
        state: present
        wait_for_completion: true

    # Drain a broker by moving all its partitions to other brokers
    - name: Drain broker 1001
      kafka_reassign:
        bootstrap_servers: "kafka1:9092,kafka2:9092"
        assignment:
          partitions:
            - topic: "important_topic"
              partition: 0
              replicas: [1002, 1003]  # Moved from [1001, 1002]
            - topic: "important_topic"
              partition: 1
              replicas: [1003, 1002]  # Moved from [1001, 1003]
        state: present
        wait_for_completion: true

    # Validate assignment without applying
    - name: Validate reassignment
      kafka_reassign:
        bootstrap_servers: "kafka1:9092,kafka2:9092"
        assignment:
          partitions:
            - topic: "test_topic"
              partition: 0
              replicas: [1001, 1002]
        validate_only: true

    # Use with ZooKeeper (older Kafka versions)
    - name: Reassign with ZooKeeper
      kafka_reassign:
        bootstrap_servers: "kafka1:9092,kafka2:9092"
        zookeeper: "zk1:2181,zk2:2181"
        assignment:
          partitions:
            - topic: "legacy_topic"
              partition: 0
              replicas: [1001, 1002]
        wait_for_completion: true

    # Check reassignment status
    - name: Get reassignment status
      kafka_reassign:
        bootstrap_servers: "kafka1:9092,kafka2:9092"
        assignment:
          partitions: []  # Empty assignment to just check status
          register: reassign_status

        # Cancel ongoing reassignment
        - name: Cancel partition reassignment
          kafka_reassign:
            bootstrap_servers: "kafka1:9092,kafka2:9092"
            assignment:
              partitions:
                - topic: "my_topic"
                  partition: 0
                  replicas: null  # null indicates cancellation
            cancel: true
            wait_for_completion: true

        # Cancel multiple reassignments
        - name: Cancel multiple partition reassignments
          kafka_reassign:
            bootstrap_servers: "kafka1:9092,kafka2:9092"
            assignment:
              partitions:
                - topic: "topic1"
                  partition: 0
                  replicas: null
                - topic: "topic1"
                  partition: 1
                  replicas: null
                - topic: "topic2"
                  partition: 0
                  replicas: null
            cancel: true
    '''


def main():
    """
    Module usage
    """
    spec = dict(
        # Reassignment-specific parameters
        assignment=dict(type='json', required=True),
        wait_for_completion=dict(type='bool', default=True),
        validate_only=dict(type='bool', default=False),
        cancel=dict(type='bool', default=False),

        **module_commons
    )
    spec.update(module_zookeeper_commons)

    module = AnsibleModule(
        argument_spec=spec,
        supports_check_mode=True
    )

    assignment = module.params['assignment']
    wait_for_completion = module.params['wait_for_completion']
    validate_only = module.params['validate_only']
    cancel = module.params['cancel']
    check_mode = module.check_mode

    changed = False
    msg = ''
    warn = None
    changes = {}
    manager = None

    try:
        manager = get_manager_from_params(module.params)
        reassign_manager = ReassignmentManager(manager)

        # Validate assignment format
        validated_assignment = reassign_manager.validate_assignment(
            assignment, module)

        # Handle validate_only mode
        if validate_only:
            msg = 'JSON assignment validation passed successfully'
            changes = {
                'validated_assignment': validated_assignment,
                'partition_count': len(validated_assignment['partitions'])
            }
            module.exit_json(changed=False, msg=msg, changes=changes)

        # Check if we're just checking status (empty assignment)
        if len(validated_assignment['partitions']) == 0:
            status = reassign_manager.get_assignment_status()
            msg = 'Reassignment status retrieved successfully'
            changes = {
                'reassignment_status': status
            }
            module.exit_json(changed=False, msg=msg, changes=changes)

        # Apply the reassignment or cancellation
        if not check_mode:
            if cancel:
                reassign_manager.cancel_assignment(
                    validated_assignment, wait_for_completion)
            else:
                reassign_manager.apply_assignment(
                    validated_assignment, wait_for_completion)

        changed = True
        partition_count = len(validated_assignment['partitions'])
        topics_affected = list(
            set(part['topic'] for part in validated_assignment['partitions']))

        if cancel:
            msg = 'Successfully cancelled reassignment for %d partitions ' \
                  'across %d topics' % (partition_count, len(topics_affected))
        else:
            msg = 'Successfully initiated reassignment for %d partitions ' \
                  'across %d topics' % (partition_count, len(topics_affected))

        changes = {
            'partitions_reassigned': partition_count,
            'topics_affected': topics_affected,
            'assignment_applied': validated_assignment,
            'wait_for_completion': wait_for_completion,
            'cancelled': cancel
        }

    except Exception as e:
        import traceback
        module.fail_json(
            msg='Something went wrong: (%s) %s' % (e, traceback.format_exc()),
            changes=changes
        )
    finally:
        if manager:
            manager.close()
        # Use cached SSL files from manager to avoid recreating them
        maybe_clean_kafka_ssl_files(
            module.params, getattr(manager, 'kafka_ssl_files', None))
        maybe_clean_zk_ssl_files(
            module.params, getattr(manager, 'zookeeper_ssl_files', None))

    if warn is not None and len(warn) > 0:
        module.warn(warn)

    module.exit_json(changed=changed, msg=msg, changes=changes)


if __name__ == '__main__':
    main()
