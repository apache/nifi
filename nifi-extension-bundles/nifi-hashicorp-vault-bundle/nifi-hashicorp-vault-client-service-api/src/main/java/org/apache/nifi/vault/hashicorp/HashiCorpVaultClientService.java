/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.vault.hashicorp;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

/**
 * Provides a HashiCorpVaultCommunicationService.
 */
public interface HashiCorpVaultClientService extends ControllerService, VerifiableControllerService {

    AllowableValue DIRECT_PROPERTIES = new AllowableValue("direct-properties", "Direct Properties",
            "Use properties, including dynamic properties, configured directly in the Controller Service to configure the client");
    AllowableValue PROPERTIES_FILES = new AllowableValue("properties-files", "Properties Files",
            "Use one or more '.properties' files to configure the client");

    PropertyDescriptor CONFIGURATION_STRATEGY = new PropertyDescriptor.Builder()
            .displayName("Configuration Strategy")
            .name("configuration-strategy")
            .required(true)
            .allowableValues(DIRECT_PROPERTIES, PROPERTIES_FILES)
            .defaultValue(DIRECT_PROPERTIES.getValue())
            .description("Specifies the source of the configuration properties.")
            .build();

    PropertyDescriptor VAULT_URI = new PropertyDescriptor.Builder()
            .name("vault.uri")
            .displayName("Vault URI")
            .description("The URI of the HashiCorp Vault server (e.g., http://localhost:8200).  Required if not specified in the " +
                    "Bootstrap HashiCorp Vault Configuration File.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .dependsOn(CONFIGURATION_STRATEGY, DIRECT_PROPERTIES)
            .build();

    PropertyDescriptor VAULT_AUTHENTICATION = new PropertyDescriptor.Builder()
            .name("vault.authentication")
            .displayName("Vault Authentication")
            .description("Vault authentication method, as described in the Spring Vault Environment Configuration documentation " +
                    "(https://docs.spring.io/spring-vault/docs/2.3.x/reference/html/#vault.core.environment-vault-configuration).")
            .required(true)
            .allowableValues(VaultAuthenticationMethod.values())
            .defaultValue(VaultAuthenticationMethod.TOKEN.name())
            .dependsOn(CONFIGURATION_STRATEGY, DIRECT_PROPERTIES)
            .build();

    PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("vault.ssl.context.service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL connections to the " +
                    "HashiCorp Vault server.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .dependsOn(CONFIGURATION_STRATEGY, DIRECT_PROPERTIES)
            .build();

    PropertyDescriptor VAULT_PROPERTIES_FILES = new PropertyDescriptor.Builder()
            .name("vault.properties.files")
            .displayName("Vault Properties Files")
            .description("A comma-separated list of files containing HashiCorp Vault configuration properties, as described in the Spring Vault " +
                    "Environment Configuration documentation (https://docs.spring.io/spring-vault/docs/2.3.x/reference/html/#vault.core.environment-vault-configuration). " +
                    "All of the Spring property keys and authentication-specific property keys are supported.")
            .required(true)
            .dependsOn(CONFIGURATION_STRATEGY, PROPERTIES_FILES)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .build();

    PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("vault.connection.timeout")
            .displayName("Connection Timeout")
            .description("The connection timeout for the HashiCorp Vault client")
            .required(true)
            .defaultValue("5 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("vault.read.timeout")
            .displayName("Read Timeout")
            .description("The read timeout for the HashiCorp Vault client")
            .required(true)
            .defaultValue("15 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    /**
     *
     * @return A service for communicating with HashiCorp Vault.
     */
    HashiCorpVaultCommunicationService getHashiCorpVaultCommunicationService();

}
