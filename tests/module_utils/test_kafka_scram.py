import pytest

from module_utils import kafka_scram


def test_get_mechanism_from_name_should_return_same_as_int():
    mechanism = kafka_scram.get_mechanism_from_name('SCRAM-SHA-256')
    assert mechanism == kafka_scram.get_mechanism_from_int(mechanism.int_representation)


def test_create_salted_password_should_raise_exception_when_unknown_mechanism():
    with pytest.raises(ValueError):
        kafka_scram.create_salted_password('NOT-EXISTING-MECHANISM', 's3cr3t', b'r4nd0mS4lt!')


def test_create_salted_password_should_raise_exception_when_not_min_iterations():
    with pytest.raises(ValueError):
        kafka_scram.create_salted_password('NOT-EXISTING-MECHANISM', 's3cr3t', b'r4nd0mS4lt!', 4095)



def test_create_salted_password_should_salt_password():
    s1 = kafka_scram.create_salted_password('SCRAM-SHA-512', 's3cr3t', b'r4nd0mS4lt!')
    s2 = kafka_scram.create_salted_password('SCRAM-SHA-512', 's3cr3t', b'r4nd0mS4lt!')
    assert s1 == s2
    


       