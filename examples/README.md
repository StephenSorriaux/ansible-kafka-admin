# Library usage examples
## Common
For each example, you will find:
* a `docker-compose.yml` file to bring up the minimum services required to use the example
* a `playbook.yml` that performs some actions using the library

## Usage
Make sure to `cd` in the folder `examples`.
1. Create a virtualenv and source it
```bash
python3 -m venv venv && source venv/bin/activate
```
2. Install requirements
```bash
pip install -r ../requirements.txt
```
3. Install Ansible
```bash
pip install ansible
```
4. Install roles
```bash
ansible-galaxy install --force -r requirements.yml
```
5. Bring up the needed stack
```bash
docker-compose -f ${folder}/docker-compose.yml up -d
```
6. Trigger the playbook
```bash
ansible-playbook ${folder}/playbook.yml -v
```
## Examples
### acl-creation
Create 2 different ACLs.

### acl-creation-multiops
Create and delete multiple different ACLs.

### scram-user-configuration
Create and delete multiple Kafka users.

### topic-creation
Create a topic.

### topic-partition-update
Create a topic and update its number of partitions.

### topic-replica-update
Create a topic and update its number of replica.

### topic-options-update
Create a topic and update the `retention.ms` configuration.
