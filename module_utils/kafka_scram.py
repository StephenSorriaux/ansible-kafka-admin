import os
import hashlib

from collections import namedtuple
# https://github.com/apache/kafka/blob/be60fe56cc1b68244c1f46a77678275a54e05668/clients/src/main/java/org/apache/kafka/clients/admin/ScramMechanism.java
# 1 > SCRAM-SHA-256
# 2 > SCRAM-SHA-512

ScramMechanism = namedtuple('ScramMechanism', [
    'name',
    'int_representation',
    'hashname',
    'min_iterations']
)

all_mechanism = [
    ScramMechanism('SCRAM-SHA-256', 1, 'sha256', 4096),
    ScramMechanism('SCRAM-SHA-512', 2, 'sha512', 4096)
]

_name_mechanism_map = dict([(m.name, m) for m in all_mechanism])
_int_mechanism_map = dict([m.int_representation, m] for m in all_mechanism)


def get_mechanism_from_int(int_representation):

    if int_representation not in _int_mechanism_map:
        raise ValueError('%s is not a valid mechanism int'
                         % int_representation)

    return _int_mechanism_map[int_representation]


def get_mechanism_from_name(mechanism_name):

    if mechanism_name not in _name_mechanism_map:
        raise ValueError('%s is not a valid mechanism name' % mechanism_name)

    return _name_mechanism_map[mechanism_name]


def create_random_salt():
    return os.urandom(16)


def create_salted_password(mechanism_name, password, salt, iterations=None):
    mechanism = get_mechanism_from_name(mechanism_name)

    if iterations < mechanism.min_iterations:
        raise ValueError('Invalid iterations: %d  - Min required %d '
                         % (iterations, mechanism.min_iterations))

    password = password.encode('utf-8')

    salted_password = hashlib.pbkdf2_hmac(
        mechanism.hashname, password, salt, iterations)

    return salted_password, salt, iterations
