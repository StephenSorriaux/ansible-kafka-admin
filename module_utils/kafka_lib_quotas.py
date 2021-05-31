# -*- coding: utf-8 -*-
from kafka.errors import KafkaError

from ansible.module_utils.pycompat24 import get_exception

from ansible.module_utils.kafka_lib_commons import (
    get_manager_from_params,
    maybe_clean_kafka_ssl_files,
    maybe_clean_zk_ssl_files
)


def _map_entity(entity):
    e = []
    if 'user' in entity and entity['user']:
        e.append({
            'entity_type': 'user',
            'entity_name': entity['user']
        })
    if 'client' in entity and entity['client']:
        e.append({
            'entity_type': 'client-id',
            'entity_name': entity['client']
        })
    return e


def _map_entries(entries):
    return [
        {
            'entity': _map_entity(entry['entity']),
            'quotas': entry['quotas']
        }
        for entry in entries
    ]


def process_module_quotas(module, params=None):
    params = params if params is not None else module.params

    entries = _map_entries(params['entries'])

    changed = False
    msg = ''
    changes = dict()

    manager = None
    try:
        manager = get_manager_from_params(params)
        current_entries = manager.describe_quotas()

        alter_entries = []
        for entry in entries:
            found = False
            entry_quotas = {key: value for key, value
                            in entry['quotas'].items() if value is not None}
            for current_entry in current_entries:
                current_entry_entity_sorted = \
                    sorted(current_entry['entity'],
                           key=lambda e: e['entity_type'])
                entry_entity_sorted = sorted(entry['entity'],
                                             key=lambda e: e['entity_type'])
                if current_entry_entity_sorted == entry_entity_sorted:
                    found = True
                    if current_entry['quotas'] != entry_quotas:
                        keys_to_add = {key: value for key, value in
                                       entry_quotas.items() if
                                       key not in current_entry['quotas']}
                        keys_to_delete = {key: value for key, value in
                                          current_entry['quotas'].items() if
                                          key not in entry_quotas}
                        keys_to_alter = {key: value for key, value in
                                         entry_quotas.items() if
                                         key in current_entry['quotas'] and
                                         current_entry['quotas'][key] != value}
                        alter_entries.append({
                            'entity': entry['entity'],
                            'quotas_to_add': keys_to_add,
                            'quotas_to_alter': keys_to_alter,
                            'quotas_to_delete': keys_to_delete
                        })
            if not found and len(entry_quotas) > 0:
                alter_entries.append({
                    'entity': entry['entity'],
                    'quotas_to_add': entry_quotas,
                    'quotas_to_delete': dict(),
                    'quotas_to_alter': dict()
                })
        if len(alter_entries) > 0:
            if not module.check_mode:
                manager.alter_quotas(alter_entries)
            changed = True
            msg = 'entries altered'
            changes = alter_entries
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
        if manager:
            manager.close()
        maybe_clean_kafka_ssl_files(params)
        maybe_clean_zk_ssl_files(params)

    if not changed:
        msg += 'nothing to do.'

    module.exit_json(changed=changed, msg=msg, changes=changes)
