#!/bin/bash

VERSION=$(git describe --abbrev=0 --tags)
NAMESPACE=StephenSorriaux
NAME=ansible_kafka_admin

cd build

ansible-galaxy collection publish ${NAMESPACE}-${NAME}-${VERSION}.tar.gz --api-key=${API_KEY}
