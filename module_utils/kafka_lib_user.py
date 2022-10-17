# -*- coding: utf-8 -*-
import collections
import operator
import traceback

from kafka.errors import KafkaError

from ansible.module_utils.kafka_scram import get_mechanism_from_int
from ansible.module_utils.pycompat24 import get_exception
from ansible.module_utils.kafka_lib_commons import (
    get_manager_from_params,
    maybe_clean_kafka_ssl_files,
    maybe_clean_zk_ssl_files
)


module_user_spec_commons = dict(
    name=dict(
        type='str',
        required=True
    ),
    password=dict(
        type='str',
        required=False,
        no_log=True
    ),
    mechanism=dict(
        choices=['SCRAM-SHA-256', 'SCRAM-SHA-512'],
        default='SCRAM-SHA-512'
    ),
    iterations=dict(
        type='int',
        required=False,
        default=4096
    ),
    state=dict(
        choices=['present', 'absent', 'updated'],
        default='present'
    ),
)


def process_module_users(module, params):
    params = params if params is not None else module.params
    manager = None
    try:
        manager = get_manager_from_params(params)
        run_module_users(manager, module, params)
    except KafkaError:
        e = get_exception()
        module.fail_json(
            msg='Unable to initialize Kafka manager: %s' % e
        )
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Something went wrong: (%s) %s' % (e, traceback.format_exc(e))
        )
    finally:
        if manager:
            manager.close()
        maybe_clean_kafka_ssl_files(params)
        maybe_clean_zk_ssl_files(params)


def run_module_users(manager, module, params):

    mark_others_as_absent = params.get('mark_others_as_absent', False)

    users = params['users']

    uniq_user_attrs = operator.itemgetter('name', 'mechanism', 'iterations')
    uniq_user_counter = collections.Counter(map(uniq_user_attrs, users))
    duplicated_users = [u for u, c in uniq_user_counter.items() if c > 1]

    if len(duplicated_users) > 0:
        module.fail_json(
            msg='Got duplicated users in \'users\': %s' % duplicated_users
        )
        return

    describe_results = manager.describe_scram_credentials()

    current_users = collections.defaultdict(dict)
    for username, err_c, err_m, cred_infos, ts in describe_results:
        for mechanism, iterations, tags in cred_infos:
            mechanism_name = get_mechanism_from_int(mechanism).name
            current_users[username][mechanism_name] = iterations

    users_to_create = []
    users_to_delete = []
    users_to_update = []

    for user in users:
        username = user['name']
        state = user['state']

        if username not in current_users:
            if state == 'present' or state == 'updated':
                users_to_create.append(user)
        else:
            if state == 'absent':
                users_to_delete.append(user)
            if state == 'updated':
                users_to_update.append(user)
            if state == 'present':
                c_user = current_users[username]
                if user['mechanism'] not in c_user \
                        or c_user[user['mechanism']] != user['iterations']:
                    users_to_update.append(user)

    users_to_upsert = users_to_create + users_to_update

    if mark_others_as_absent:

        users_triples = [(user['name'], user['mechanism'], user['iterations'])
                         for user in users if user['state'] != 'absent']

        users_to_delete = [
            {'name': u, 'mechanism': m, 'iterations': i}
            for (u, ms) in current_users.items()
            for (m, i) in ms.items() if (u, m, i) not in users_triples]

    if len(users_to_upsert) == 0 and len(users_to_delete) == 0:
        module.exit_json(changed=False, msg="All users are already up-to-date")
    else:
        if not module.check_mode:
            manager.alter_scram_users(users_to_delete, users_to_upsert)

        changes = {
            'created': [
                {'name': user['name'], 'mechanism': user['mechanism']}
                for user in users_to_create
            ],
            'updated': [
                {'name': user['name'], 'mechanism': user['mechanism']}
                for user in users_to_update
            ],
            'deleted': [
                {'name': user['name'], 'mechanism': user['mechanism']}
                for user in users_to_delete
            ]
        }

        module.exit_json(changed=True, msg="Users updated", changes=changes)


def process_module_user(module):
    params = module.params.copy()
    params['users'] = [
        dict([(k, params[k]) for k in module_user_spec_commons.keys()])
    ]
    process_module_users(module, params)
