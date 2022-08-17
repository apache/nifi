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
package org.apache.nifi.stateless.parameter;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.vault.hashicorp.HashiCorpVaultCommunicationService;
import org.apache.nifi.vault.hashicorp.StandardHashiCorpVaultCommunicationService;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultConfiguration;
import org.springframework.core.env.PropertySource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reads secrets from HashiCorp Vault to provide parameters.  An example of setting one such secret parameter value
 * using the Vault CLI would be:
 *
 * <code>vault kv put "${vault.kv.path}/[ParamContextName]" [Param1]=[ParamValue1] [Param2]=[ParamValue2]</code>
 *
 * Here, vault.kv.path is supplied by the file specified by the "Vault Configuration File" property.
 *
 * A standard configuration for this provider would be: <br/><br/>
 *
 * <code>
 *      nifi.stateless.parameter.provider.Vault.name=HashiCorp Vault Provider
 *      nifi.stateless.parameter.provider.Vault.type=org.apache.nifi.stateless.parameter.HashiCorpVaultParameterProvider
 *      nifi.stateless.parameter.provider.Vault.properties.vault-configuration-file=./conf/bootstrap-hashicorp-vault.conf
 * </code>
 */
public class HashiCorpVaultParameterValueProvider extends AbstractSecretBasedParameterValueProvider implements ParameterValueProvider {
    private static final String KEY_VALUE_PATH = "vault.kv.path";
    public static final PropertyDescriptor VAULT_CONFIG_FILE = new PropertyDescriptor.Builder()
            .displayName("Vault Configuration File")
            .name("vault-configuration-file")
            .required(true)
            .defaultValue("./conf/bootstrap-hashicorp-vault.conf")
            .description("Location of the bootstrap-hashicorp-vault.conf file that configures the Vault connection")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    private HashiCorpVaultCommunicationService vaultCommunicationService;
    private String path;

    @Override
    protected List<PropertyDescriptor> getAdditionalSupportedPropertyDescriptors() {
        return Collections.singletonList(VAULT_CONFIG_FILE);
    }

    @Override
    protected void additionalInit(final ParameterValueProviderInitializationContext context) {
        final String vaultBootstrapConfFilename = context.getProperty(VAULT_CONFIG_FILE).getValue();
        this.configure(vaultBootstrapConfFilename);
    }

    void configure(final String vaultBootstrapConfFilename) {
        try {
            final PropertySource<?> propertySource = HashiCorpVaultConfiguration.createPropertiesFileSource(vaultBootstrapConfFilename);
            vaultCommunicationService = new StandardHashiCorpVaultCommunicationService(propertySource);
            path = Objects.requireNonNull((String) propertySource.getProperty(KEY_VALUE_PATH), String.format("%s must be specified in %s", KEY_VALUE_PATH, vaultBootstrapConfFilename));
        } catch (final IOException e) {
            throw new IllegalStateException("Error configuring HashiCorpVaultCommunicationService", e);
        }
    }

    @Override
    protected String getSecretValue(final String secretName, final String keyName) {
        final Map<String, String> keyValues = vaultCommunicationService.readKeyValueSecretMap(path, secretName);
        return keyValues.get(keyName);
    }

    void setVaultCommunicationService(final HashiCorpVaultCommunicationService vaultCommunicationService) {
        this.vaultCommunicationService = vaultCommunicationService;
    }

    void setPath(final String path) {
        this.path = path;
    }
}
