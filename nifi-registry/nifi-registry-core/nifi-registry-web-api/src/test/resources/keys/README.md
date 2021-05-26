<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Test Keys

The automated security tests require keys and certificates for TLS connections. 
The keys in this directory can be used for that purpose.

***

**NOTICE**: This directory contains keys and certificates for *development and testing* purposes only.

**Never use these keystores and truststores in a real-world scenario where actual security is needed.** 

The CA and private keys (including their protection passwords) have been published on the Internet, so they should never be trusted.

***  

## Directory Contents

### Certificate Authority (CA)

| Hostname / DN | File | Description | Format | Password |
| --- | --- | --- | --- | --- |
| - | ca-cert.pem | CA public cert | PEM (unencrypted) | N/A |
| - | ca-key.pem | CA private (signing) key | PEM | password |
| - | ca-ts.jks | CA cert truststore (shared by clients and servers) | JKS | password |
| - | ca-ts.p12 | CA cert truststore (shared by clients and servers) | PKCS12 | password |
| registry, localhost | registry-cert.pem | NiFi Registry server public cert | PEM (unencrypted) | N/A |
| registry, localhost | registry-key.pem | NiFi Registry server private key | PEM | password |
| registry, localhost | registry-ks.jks | NiFi Registry server key/cert keystore | JKS | password |
| registry, localhost | registry-ks.p12 | NiFi Registry server key/cert keystore | PKCS12 | password |
| proxy, localhost | proxy-cert.pem | Proxy server public cert | PEM (unencrypted) | N/A |
| proxy, localhost | proxy-key.pem | Proxy server private key | PEM | password |
| proxy, localhost | proxy-ks.jks | Proxy server key/cert keystore | JKS | password |
| proxy, localhost | proxy-ks.p12 | Proxy server key/cert keystore | PKCS12 | password |
| CN=user1, OU=nifi | user1-cert.pem | client (user="user1") public cert | PEM (unencrypted) | N/A |
| CN=user1, OU=nifi | user1-key.pem | client (user="user1") private key | PEM | password |
| CN=user1, OU=nifi | user1-ks.jks | client (user="user1") key/cert keystore | JKS | password |
| CN=user1, OU=nifi | user1-ks.p12 | client (user="user1") key/cert keystore | PKCS12 | password |

## Generating Additional Test Keys/Certs

If we need to add a service or user to our test environment that requires a cert signed by the same CA, here are the steps for generating additional keys for this directory that are signed by the same CA key.

Requirements:

- docker
- keytool (included with Java)
- openssl (included/available on most platforms)

If you do not have docker, you can substitute the nifi-toolkit binary, which is available for download from https://nifi.apache.org and should run on any platform with Java 1.8. 

### New Service Keys

The steps for generating a new service key/cert pair are (using `proxy` as the example service):

```
# make working directory
WD="/tmp/test-keys-$(date +"%Y%m%d-%H%M%S")"
mkdir "$WD"
cd "$WD"

# copy existing CA key/cert pair to working directory, rename to default tls-toolkit names
cp /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/ca-key.pem ./nifi-key.key
cp /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/ca-cert.pem ./nifi-cert.pem

# use NiFi Toolkit Docker image to generate new keys/certs
docker run -v "$WD":/tmp -w /tmp apache/nifi-toolkit:latest tls-toolkit standalone \
      --hostnames proxy \
      --subjectAlternativeNames localhost \
      --nifiDnSuffix ", OU=nifi" \
      --keyStorePassword password \
      --trustStorePassword password \
      --days 9999 \
      -O

# switch to output directory, create final output directory
cd "$WD"
mkdir keys

# copy new service key/cert to final output dir in all formats
keytool -importkeystore \
      -srckeystore proxy/keystore.jks -srcstoretype jks -srcstorepass password -srcalias nifi-key \
      -destkeystore keys/proxy-ks.jks -deststoretype jks -deststorepass password -destalias proxy-key
keytool -importkeystore \
      -srckeystore keys/proxy-ks.jks -srcstoretype jks -srcstorepass password \
      -destkeystore keys/proxy-ks.p12 -deststoretype pkcs12 -deststorepass password
openssl pkcs12 -in keys/proxy-ks.p12 -passin pass:password -out keys/proxy-key.pem -passout pass:password
openssl pkcs12 -in keys/proxy-ks.p12 -passin pass:password -out keys/proxy-cert.pem -nokeys

echo
echo "New keys written to ${WD}/keys"
echo "Copy to NiFi Registry test keys dir by running: "
echo "    cp \"$WD/keys/*\" /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/"
```

You can verify the contents of the new keystore (and that the signature is done by the correct CA) using the following command:

    keytool -list -v -keystore "$WD/keys/proxy-ks.jks" -storepass password

If you are satisfied with the results, you can copy the files from `/tmp/test-keys-YYYYMMDD-HHMMSS/keys` to this directory:
 
    cp "$WD/keys/*" /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/

### New Client or User Keys

The steps for generating a new user key/cert pair are (using `user2` as the example user):

```
# make working directory
WD="/tmp/test-keys-$(date +"%Y%m%d-%H%M%S")"
mkdir "$WD"
cd "$WD"

# copy existing CA key/cert pair to working directory, rename to default tls-toolkit names
cp /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/ca-key.pem ./nifi-key.key
cp /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/ca-cert.pem ./nifi-cert.pem

# use NiFi Toolkit Docker image to generate new keys/certs
docker run -v "$WD":/tmp -w /tmp apache/nifi-toolkit:latest tls-toolkit standalone \
      --clientCertDn "CN=user2, OU=nifi" \
      --clientCertPassword password \
      --days 9999 \
      -O

# switch to output directory, create final output directory
cd "$WD"
mkdir keys

# transform tls-toolkit output to final output
keytool -importkeystore \
      -srckeystore CN=user2_OU=nifi.p12 -srcstoretype PKCS12 -srcstorepass password -srcalias nifi-key \
      -destkeystore keys/user2-ks.jks -deststoretype JKS -deststorepass password -destalias user2-key
keytool -importkeystore \
      -srckeystore keys/user2-ks.jks -srcstoretype jks -srcstorepass password \
      -destkeystore keys/user2-ks.p12 -deststoretype pkcs12 -deststorepass password
openssl pkcs12 -in keys/user2-ks.p12 -passin pass:password -out keys/user2-key.pem -passout pass:password
openssl pkcs12 -in keys/user2-ks.p12 -passin pass:password -out keys/user2-cert.pem -nokeys

echo
echo "New keys written to ${WD}/keys"
echo "Copy to NiFi Registry test keys dir by running: "
echo "    cp \"$WD/keys/*\" /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/"
```

You can verify the contents of the new keystore (and that the signature is done by the correct CA) using the following command:

    keytool -list -v -keystore "$WD/keys/user2-ks.jks" -storepass password

If you are satisfied with the results, you can copy the files from `/tmp/test-keys-YYYYMMDD-HHMMSS/keys` to this directory:
 
    cp "$WD/keys/*" /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/


## Regenerating All Test Keys/Certs

In case you need to regenerate this entire directory, here are the steps that were used to first create it. 
Follow these steps in order to recreate it.

Requirements:

- docker
- keytool (included with Java)
- openssl (included/available on most platforms)

If you do not have docker, you can substitute the nifi-toolkit binary, which is available for download from https://nifi.apache.org and should run on any platform with Java 1.8. 

The steps for regenerating these test keys are:

```
# make working directory
WD="/tmp/test-keys-$(date +"%Y%m%d-%H%M%S")"
mkdir "$WD"
cd "$WD"

# use NiFi Toolkit Docker image to generate new keys/certs
docker run -v "$WD":/tmp -w /tmp apache/nifi-toolkit:latest tls-toolkit standalone \
      --certificateAuthorityHostname "Test CA (Do Not Trust)" \
      --hostnames registry \
      --subjectAlternativeNames localhost \
      --nifiDnSuffix ", OU=nifi" \
      --keyStorePassword password \
      --trustStorePassword password \
      --clientCertDn "CN=user1, OU=nifi" \
      --clientCertPassword password \
      --days 9999 \
      -O

# switch to output directory, create final output directory
cd "$WD"
mkdir keys

# copy CA key/cert to final output dir in all formats
cp nifi-key.key keys/ca-key.pem
cp nifi-cert.pem keys/ca-cert.pem
keytool -importkeystore \
      -srckeystore registry/truststore.jks -srcstoretype jks -srcstorepass password -srcalias nifi-cert \
      -destkeystore keys/ca-ts.jks -deststoretype jks -deststorepass password -destalias ca-cert
keytool -importkeystore \
      -srckeystore keys/ca-ts.jks -srcstoretype jks -srcstorepass password \
      -destkeystore keys/ca-ts.p12 -deststoretype pkcs12 -deststorepass password

# copy registry service key/cert to final output dir in all formats
keytool -importkeystore \
      -srckeystore registry/keystore.jks -srcstoretype jks -srcstorepass password -srcalias nifi-key \
      -destkeystore keys/registry-ks.jks -deststoretype jks -deststorepass password -destalias registry-key
keytool -importkeystore \
      -srckeystore keys/registry-ks.jks -srcstoretype jks -srcstorepass password \
      -destkeystore keys/registry-ks.p12 -deststoretype pkcs12 -deststorepass password
openssl pkcs12 -in keys/registry-ks.p12 -passin pass:password -out keys/registry-key.pem -passout pass:password
openssl pkcs12 -in keys/registry-ks.p12 -passin pass:password -out keys/registry-cert.pem -nokeys

# copy user1 client key/cert to final output dir in all formats
keytool -importkeystore \
      -srckeystore CN=user1_OU=nifi.p12 -srcstoretype PKCS12 -srcstorepass password -srcalias nifi-key \
      -destkeystore keys/user1-ks.jks -deststoretype JKS -deststorepass password -destkeypass password -destalias user1-key
keytool -importkeystore \
      -srckeystore keys/user1-ks.jks -srcstoretype jks -srcstorepass password \
      -destkeystore keys/user1-ks.p12 -deststoretype pkcs12 -deststorepass password
openssl pkcs12 -in keys/user1-ks.p12 -passin pass:password -out keys/user1-key.pem -passout pass:password
openssl pkcs12 -in keys/user1-ks.p12 -passin pass:password -out keys/user1-cert.pem -nokeys

echo
echo "New keys written to ${WD}/keys"
echo "Copy to NiFi Registry test keys dir by running: "
echo "    cp -f \"$WD/keys/*\" /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/"
```

You should now have a `/tmp/test-keys-YYYYMMDD-HHMMSS/keys` directory with all the necessary keys for testing with various tools.

You can verify the contents of a keystore using the following command:

    keytool -list -v -keystore "$WD/keys/registry-ks.jks" -storepass password

If you are satisfied with the results, you can copy the files from `/tmp/test-keys-YYYYMMDD-HHMMSS/keys` to this directory:

    cp -f "$WD/keys/*" /path/to/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys/
