#!/bin/bash

VERSION=$(git describe --abbrev=0 --tags)
CURRENT_MODULES=$(find module_utils -type f -iname "*.py" -execdir basename {} .py ';')
NAMESPACE=$1
NAME=ansible_kafka_admin

mkdir -p build/plugins/{modules,module_utils}
cp -r library/* build/plugins/modules
cp -r module_utils/* build/plugins/module_utils
cp README.md build/

# Migrate imports
for CURRENT_MODULE in ${CURRENT_MODULES}
do
    find build -iname "*.py" -exec sed -i "s/ansible\.module_utils\.${CURRENT_MODULE}/ansible_collections\.${NAMESPACE}\.${NAME}\.plugins\.module_utils\.${CURRENT_MODULE}/g" {} \;
done

cat > build/galaxy.yml <<EOF
namespace: ${NAMESPACE}
name: ${NAME}
version: ${VERSION}
readme: README.md

authors:
- Stephen SORRIAUX

description: Manage Kafka resources (topic, broker, acl)

license:
- apache v2

tags: ['kafka']

dependencies: {}

repository: https://github.com/StephenSorriaux/ansible-kafka-admin.git
documentation: https://github.com/StephenSorriaux/ansible-kafka-admin.git
homepage: https://github.com/StephenSorriaux/ansible-kafka-admin.git
issues: https://github.com/StephenSorriaux/ansible-kafka-admin.git
EOF
cd build
ansible-galaxy collection build

