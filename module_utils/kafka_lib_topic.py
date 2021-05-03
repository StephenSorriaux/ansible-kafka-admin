# -*- coding: utf-8 -*-
import collections

from kafka.errors import KafkaError

from ansible.module_utils.pycompat24 import get_exception

from ansible.module_utils.kafka_lib_commons import (
    get_manager_from_params,
    maybe_clean_kafka_ssl_files,
    maybe_clean_zk_ssl_files
)


def process_module_topics(module, params=None):
    params = params if params is not None else module.params

    topics = params['topics']
    mark_others_as_absent = params.get('mark_others_as_absent', False)

    # Check for duplicated topics
    duplicated_topics = [topic for topic, count in collections.Counter(
        [topic['name'] for topic in topics]
    ).items() if count > 1]

    if len(duplicated_topics) > 0:
        module.fail_json(
            msg='Got duplicated topics in \'topics\': %s' % duplicated_topics
        )
        return

    changed = False
    msg = ''
    warn = None

    try:
        manager = get_manager_from_params(params)
        current_topics = manager.get_topics()

        topics_to_create = [
            topic for topic in topics
            if (topic['state'] == 'present' and
                topic['name'] not in current_topics)
        ]
        if len(topics_to_create) > 0:
            if not module.check_mode:
                manager.create_topics(topics_to_create)
            changed = True
            msg += ''.join(['topic %s successfully created. ' %
                            topic['name'] for topic in topics_to_create])

        topics_to_maybe_update = [
            topic for topic in topics
            if (topic['state'] == 'present' and
                topic['name'] in current_topics)
        ]
        if len(topics_to_maybe_update) > 0:
            if not module.check_mode:
                topics_changed, warn = manager.ensure_topics(
                    topics_to_maybe_update
                )
                changed = len(topics_changed) > 0
                if changed:
                    msg += ''.join(['topic %s successfully updated. ' %
                                    topic for topic in topics_changed])
        topics_to_delete = [
            topic for topic in topics
            if (topic['state'] == 'absent' and
                topic['name'] in current_topics)
        ]
        # Cleanup existing if necessary
        if mark_others_as_absent:
            defined_topics = [topic['name'] for topic in topics]
            for existing_topic in set(current_topics) - set(defined_topics):
                topics_to_delete.append({
                    'name': existing_topic,
                    'state': 'absent'
                })
        if len(topics_to_delete) > 0:
            if not module.check_mode:
                manager.delete_topics(topics_to_delete)
            changed = True
            msg += ''.join(['topic %s successfully deleted. ' %
                           topic['name'] for topic in topics_to_delete])
    except KafkaError:
        e = get_exception()
        module.fail_json(
            msg='Unable to initialize Kafka manager: %s' % e
        )
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Something went wrong: %s' % e
        )
    finally:
        manager.close()
        maybe_clean_kafka_ssl_files(params)
        maybe_clean_zk_ssl_files(params)

    if not changed:
        msg += 'nothing to do.'

    if warn is not None and len(warn) > 0:
        module.warn(warn)

    module.exit_json(changed=changed, msg=msg)


def process_module_topic(module):
    params = module.params.copy()
    params['topics'] = [{
        'name': params['name'],
        'partitions': params['partitions'],
        'replica_factor': params['replica_factor'],
        'state': params['state'],
        'options': params['options']
    }]

    process_module_topics(module, params)
