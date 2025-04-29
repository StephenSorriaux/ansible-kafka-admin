#!/bin/bash
set -ev

rm -fr ca intermediate server client keystore cacert.pem server.pem client.pem
mkdir -p ca/newcerts intermediate/newcerts server client keystore zk
touch ca/index.txt
echo 1000 > ca/serial

touch intermediate/index.txt
echo 1000 > intermediate/serial

openssl genrsa -out ca/ca.key.pem 2048
chmod 400 ca/ca.key.pem
openssl req -config openssl-ca.cnf \
        -key ca/ca.key.pem \
        -new -x509 -days 36500 -sha256 -extensions v3_ca \
        -out ca/ca.cert.pem

openssl genrsa -out intermediate/intermediate.key.pem 2048
chmod 400 intermediate/intermediate.key.pem
openssl req -config openssl-intermediate-ca.cnf \
        -key intermediate/intermediate.key.pem \
        -new -sha256 \
        -out intermediate/intermediate.csr.pem
openssl ca -batch -config openssl-ca.cnf -extensions v3_intermediate_ca \
        -days 36500 -notext -md sha256 \
        -in intermediate/intermediate.csr.pem \
        -out intermediate/intermediate.cert.pem

cat intermediate/intermediate.cert.pem > cacert.pem
cat ca/ca.cert.pem >> cacert.pem

openssl genrsa -out server/server.key.pem 2048
chmod 400 server/server.key.pem
openssl req -config openssl-intermediate-ca.cnf -extensions server_cert \
        -subj '/C=FR/ST=France/L=Paris/O=Alice Ltd/OU=Alice Ltd/CN=server' \
        -addext "subjectAltName = DNS:kafka1-01103,DNS:kafka2-01103,DNS:kafka1-111,DNS:kafka2-111,DNS:kafka1-282,DNS:kafka2-282,DNS:kafka1-370,DNS:kafka2-370,DNS:kafka1-400,DNS:kafka2-400,IP:172.18.1.3,IP:172.18.1.4,IP:172.18.2.3,IP:172.18.2.4,IP:172.18.3.3,IP:172.18.3.4,IP:172.18.4.3,IP:172.18.4.4,IP:172.18.5.3,IP:172.18.5.4,DNS:172.18.1.3,DNS:172.18.1.4,DNS:172.18.2.3,DNS:172.18.2.4,DNS:172.18.3.3,DNS:172.18.3.4,DNS:172.18.4.3,DNS:172.18.4.4,DNS:172.18.5.3,DNS:172.18.5.4" \
        -key server/server.key.pem \
        -new -sha256 \
        -out server/server.csr.pem
openssl ca -batch -config openssl-intermediate-ca.cnf \
        -days 36500 -notext -md sha256 \
        -in server/server.csr.pem \
        -out server/server.cert.pem

set +e
openssl genrsa -out zk/server.key.pem 2048
chmod 400 zk/server.key.pem
set -e
openssl req -config openssl-intermediate-ca.cnf -extensions server_cert \
        -subj '/C=FR/ST=France/L=Paris/O=Alice Ltd/OU=Alice Ltd/CN=zk' \
        -addext "subjectAltName = DNS:zookeeper-01103,DNS:zookeeper-111,DNS:zookeeper-282,DNS:zookeeper-370,IP:172.18.1.2,IP:172.18.2.2,IP:172.18.3.2,IP:172.18.4.2,DNS:172.18.1.2,DNS:172.18.2.2,DNS:172.18.3.2,DNS:172.18.4.2" \
        -key zk/server.key.pem \
        -new -sha256 \
        -out zk/server.csr.pem
openssl ca -batch -config openssl-intermediate-ca.cnf \
        -days 36500 -notext -md sha256 \
        -in zk/server.csr.pem \
        -out zk/server.cert.pem

set +e
openssl genrsa -out client/client.key.pem 2048
chmod 400 client/client.key.pem

set -e
openssl req -config openssl-intermediate-ca.cnf -extensions usr_cert \
        -subj '/C=FR/ST=France/L=Paris/O=Alice Ltd/OU=Alice Ltd/CN=admin' \
        -key client/client.key.pem \
        -new -sha256 \
        -out client/client.csr.pem
openssl ca -batch -config openssl-intermediate-ca.cnf \
        -days 36500 -notext -md sha256 \
        -in client/client.csr.pem \
        -out client/client.cert.pem

cat server/server.key.pem > server.pem
cat server/server.cert.pem >> server.pem
cat cacert.pem >> server.pem

openssl pkcs12 -export -in server.pem -out keystore/server-keystore.p12 -name server -noiter -nomaciter -passout pass:password

keytool -importkeystore -deststorepass password -destkeypass password -destkeystore keystore/server-keystore.jks -deststoretype JKS -srckeystore keystore/server-keystore.p12 -srcstoretype PKCS12 -srcstorepass password -alias server
keytool -import -noprompt -storepass password -keystore keystore/server-truststore.jks -alias intermediate -file intermediate/intermediate.cert.pem -storetype JKS

cat zk/server.key.pem > zk.pem
cat zk/server.cert.pem >> zk.pem
cat cacert.pem >> zk.pem

openssl pkcs12 -export -in zk.pem -out keystore/zk-keystore.p12 -name server -noiter -nomaciter -passout pass:password

keytool -importkeystore -deststorepass password -destkeypass password -destkeystore keystore/zk-keystore.jks -deststoretype JKS  -srckeystore keystore/zk-keystore.p12 -srcstoretype PKCS12 -srcstorepass password -alias server
keytool -import -noprompt -storepass password -keystore keystore/zk-truststore.jks -alias intermediate -file intermediate/intermediate.cert.pem -storetype JKS
