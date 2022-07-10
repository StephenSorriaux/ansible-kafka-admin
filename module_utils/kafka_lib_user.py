# -*- coding: utf-8 -*-
import collections

from ansible.module_utils import kafka_scram
from ansible.module_utils.kafka_lib_commons import (
    get_manager_from_params
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
        required=False
    ),
    state=dict(
        choices=['present', 'absent', 'update'],
        default='present'
    ),
)


def process_module_users(module, params):
    params = params if params is not None else module.params

    mark_others_as_absent = params.get('mark_others_as_absent', False)

    users = params['users']

    # set default mechanism iterations if None
    for user in users:
        if user['iterations'] is None:
            mechanism = kafka_scram.get_mechanism_from_name(user['mechanism'])
            user['iterations'] = mechanism.default_iterations

    # TODO check for duplicate users
    manager = get_manager_from_params(params)

    describe_results = manager.describe_scram_credentials()

    current_users = collections.defaultdict(dict)
    for username, err_c, err_m, cred_infos, ts in describe_results:
        for mechanism, iterations, tags in cred_infos:
            mechanism_name = kafka_scram.get_mechanism_from_int(mechanism).name
            current_users[username][mechanism_name] = iterations

    users_to_create = []
    users_to_delete = []
    users_to_update = []

    for user in users:
        username = user['name']
        state = user['state']

        if username not in current_users:
            if state == 'present' or state == 'update':
                users_to_create.append(user)
        else:
            if state == 'absent':
                users_to_delete.append(user)
            if state == 'update':
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
