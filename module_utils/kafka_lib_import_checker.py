from pkg_resources import parse_version

import kafka
import kazoo

parsed_kafka_python_version = parse_version(kafka.version.__version__)
if not (
    parse_version('2.0.0')
    <= parsed_kafka_python_version
    <= parse_version('2.1.0')
):
    raise ImportError(
        'Used version of kafka-python is incorrect and might lead to '
        'unexpected errors. Please fix it by using the provided '
        'requirements.txt file.'
    )

parsed_kazoo_version = parse_version(kazoo.version.__version__)
if parse_version('2.6.1') > parsed_kazoo_version:
    raise ImportError(
        'Used version of kazoo is incorrect and might lead to '
        'unexpected errors. Please fix it by using the provided '
        'requirements.txt file.'
    )
