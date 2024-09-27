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

# StandardHashiCorpVaultClientService

## Configuring the Bootstrap HashiCorp Vault Configuration File

The ./conf/bootstrap-hashicorp-vault.conf file that comes with Apache NiFi is a convenient way to configure this
controller service in a manner consistent with the HashiCorpVault sensitive property provider. Since this file is
already used for configuring the Vault client for protecting sensitive properties in the NiFi configuration files (see
the Administrator's Guide), it's a natural starting point for configuring the controller service as well.

An example configuration of this properties file is as follows:

```properties
# HTTP or HTTPS URI for HashiCorp Vault is required to enable the Sensitive Properties Provider
vault.uri=https://127.0.0.1:8200

# Optional file supports authentication properties described in the Spring Vault Environment Configuration
# https://docs.spring.io/spring-vault/docs/2.3.x/reference/html/#vault.core.environment-vault-configuration
#
# All authentication properties must be included in bootstrap-hashicorp-vault.conf when this property is not specified.
# Properties in bootstrap-hashicorp-vault.conf take precedence when the same values are defined in both files.
# Token Authentication is the default when the 'vault.authentication' property is not specified.
vault.authentication.properties.file=[full/path/to/vault-auth.properties]

# Optional Timeout properties
vault.connection.timeout=5 secs
vault.read.timeout=15 secs

# Optional TLS properties
vault.ssl.enabledCipherSuites=
vault.ssl.enabledProtocols=TLSv1.3
vault.ssl.key-store=[path/to/keystore.p12]
vault.ssl.key-store-type=PKCS12
vault.ssl.key-store-password=[keystore password]
vault.ssl.trust-store=[path/to/truststore.p12]
vault.ssl.trust-store-type=PKCS12
vault.ssl.trust-store-password=[truststore password]
```

In order to use this file in the StandardHashiCorpVaultClientService, specify the following properties:

* **Configuration Strategy** - Properties Files
* **Vault Properties Files** - ./conf/bootstrap-hashicorp-vault.conf

If your bootstrap configuration includes the vault.authentication.properties.file containing additional authentication
properties, this file will also need to be added to the Vault Properties Files property as a comma-separated value.

### Configuring the Client using Direct Properties

However, if you want to specify or override properties directly in the controller service, you may do this by specifying
a Configuration Strategy of 'Direct Properties'. This can be useful if you are reusing an SSLContextService or want to
parameterize the Vault configuration properties. Authentication-related properties can also be added as sensitive
dynamic properties, as seen in the examples below.

### Vault Authentication

Under the hood, the controller service uses Spring Vault, and directly supports the property keys specified
in [Spring Vault's documentation](https://docs.spring.io/spring-vault/docs/2.3.x/reference/html/#vault.core.environment-vault-configuration).
Following are some common examples of authentication with Vault.

#### Token Authentication

The simplest authentication scheme uses a rotating token, which is enabled by default in Vault. To specify this
mechanism, select "TOKEN" from the "Vault Authentication" property (the default). However, since the token should rotate
by nature, it is a best practice to use the 'Properties Files' Configuration Strategy, and keep the token value in an
external properties file, indicating this filename in the 'Vault Properties Files' property. Then an external process
can rotate the token in the file without updating NiFi configuration. In order to pick up the changed token, the
controller service must be disabled and re-enabled.

For testing purposes, however, it may be more convenient to specify the token directly in the controller service. To do
so, add a new Sensitive property named 'vault.token' and enter the token as the value.

#### Certificate Authentication

Certificate authentication must be enabled in the Vault server before it can be used from NiFi, but it uses the same TLS
settings as the actual client connection, so no additional authentication properties are required. While these TLS
settings can be provided in an external properties file, we will demonstrate configuring an SSLContextService instead.

First, create an SSLContextService controller service and configure the Filename, Password, and Type for both the
Keystore and Truststore. Enable it, and assign it as the SSL Context Service in the Vault controller service. Then,
simply specify "CERT" as the "Vault Authentication" property value.

#### Other Authentication Methods

To configure the other authentication methods, see the Spring Vault documentation linked above. All relevant properties
should be added either to the external properties files referenced in the "Vault Properties Files" property if using
the 'Properties Files' Configuration Strategy, or added as custom properties with the same name if using the 'Direct
Properties' Configuration Strategy. For example, for the Azure authentication mechanism, properties will have to be
added for 'vault.azure-msi.azure-path', 'vault.azure-msi.role', and 'vault.azure-msi.identity-token-service'.