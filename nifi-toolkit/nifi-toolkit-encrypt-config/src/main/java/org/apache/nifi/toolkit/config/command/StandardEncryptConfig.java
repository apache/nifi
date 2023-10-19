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
package org.apache.nifi.toolkit.config.command;

import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.properties.ApplicationProperties;
import org.apache.nifi.properties.ApplicationPropertiesProtector;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.properties.ProtectedNiFiProperties;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.toolkit.config.transformer.ApplicationPropertiesFileTransformer;
import org.apache.nifi.toolkit.config.transformer.FileTransformer;
import org.apache.nifi.toolkit.config.transformer.FlowConfigurationFileTransformer;
import org.apache.nifi.toolkit.config.transformer.XmlFileTransformer;
import org.apache.nifi.util.NiFiProperties;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Encrypt Config Standard implementation for NiFi configuration files
 */
@Command
public class StandardEncryptConfig extends SharedEncryptConfig implements Runnable {

    static final String BOOTSTRAP_ROOT_KEY_PROPERTY = "nifi.bootstrap.sensitive.key";

    private static final String DEFAULT_PROPERTIES_ALGORITHM = "NIFI_PBKDF2_AES_GCM_256";

    @Option(
            names = {"-m", "--migrate"},
            description = "Migrate configuration files from current protection configuration to new protection configuration"
    )
    boolean migrationRequested;

    @Option(
            names = {"-x", "--encryptFlowXmlOnly", "--encryptFlowJsonOnly"},
            description = "Process Flow Configuration [flow.json.gz] sensitive property values without modifying other configuration files"
    )
    boolean flowConfigurationRequested;

    @Option(
            names = {"-n", "--niFiProperties"},
            description = "Path to file containing Application Properties [nifi.properties] that will be updated unless the output argument is provided"
    )
    Path applicationPropertiesPath;

    @Option(
            names = {"-o", "--outputNiFiProperties"},
            description = "Path to output file for Application Properties [nifi.properties] with property protection applied"
    )
    Path outputApplicationPropertiesPath;

    @Option(
            names = {"-l", "--loginIdentityProviders"},
            description = "Path to file containing Login Identity Providers [login-identity-providers.xml] configuration that will be updated unless the output argument is provided"
    )
    Path loginIdentityProvidersPath;

    @Option(
            names = {"-i", "--outputLoginIdentityProviders"},
            description = "Path to output file for Login Identity Providers [login-identity-providers.xml] with property protection applied"
    )
    Path outputLoginIdentityProvidersPath;

    @Option(
            names = {"-f", "--flowConfiguration", "--flowJson", "--flowXml"},
            description = "Path to file containing Flow Configuration [flow.json.gz] that will be updated unless the output argument is provided"
    )
    Path flowConfigurationPath;

    @Option(
            names = {"-g", "--outputFlowConfiguration", "--outputFlowJson", "--outputFlowXml"},
            description = "Path to output file for Flow Configuration [flow.json.gz] with property protection applied"
    )
    Path outputFlowConfigurationPath;

    @Option(
            names = {"-s", "--propsKey"},
            description = "Properties Key [nifi.sensitive.props.key] from which to derive the key used to encrypt the sensitive values in the Flow Configuration",
            arity = "0..1",
            interactive = true
    )
    String sensitivePropertiesKey;

    @Option(
            names = {"-A", "--newFlowAlgorithm"},
            description = "Properties Algorithm [nifi.sensitive.props.algorithm] with which to encrypt the sensitive values in the Flow Configuration. Default is ${DEFAULT-VALUE}",
            defaultValue = DEFAULT_PROPERTIES_ALGORITHM
    )
    String newFlowAlgorithm;

    @Override
    public void run() {
        final ApplicationProperties applicationProperties = loadApplicationProperties();

        processBootstrapConf(BOOTSTRAP_ROOT_KEY_PROPERTY);
        processApplicationProperties(applicationProperties);
        processFlowConfiguration(applicationProperties);
        processAuthorizers();
        processLoginIdentityProviders();
    }

    @Override
    protected void processBootstrapConf(final String rootKeyProperty) {
        if (flowConfigurationRequested) {
            logger.info("Bootstrap Configuration [bootstrap.conf] not modified based on provided arguments");
        } else {
            super.processBootstrapConf(rootKeyProperty);
        }
    }

    @Override
    protected void processAuthorizers() {
        if (flowConfigurationRequested) {
            logger.info("Authorizers not modified based on provided arguments");
        } else {
            super.processAuthorizers();
        }
    }

    private ApplicationProperties loadApplicationProperties() {
        final ApplicationProperties applicationProperties;

        if (applicationPropertiesPath == null) {
            applicationProperties = new ApplicationProperties(Collections.emptyMap());
            if (verboseModeEnabled) {
                logger.info("Application Properties [nifi.properties] not specified");
            }
        } else if (Files.notExists(applicationPropertiesPath)) {
            throw new IllegalArgumentException(String.format("Application Properties [nifi.properties] not found [%s]", applicationPropertiesPath));
        } else {
            final NiFiPropertiesLoader propertiesLoader = new NiFiPropertiesLoader();

            final String rootKey = migrationRequested ? getInputRootKey() : getRootKey();
            propertiesLoader.setKeyHex(rootKey);

            applicationProperties = propertiesLoader.load(applicationPropertiesPath.toFile());
        }

        return applicationProperties;
    }

    private void processApplicationProperties(final ApplicationProperties applicationProperties) {
        if (applicationPropertiesPath == null) {
            if (verboseModeEnabled) {
                logger.info("Application Properties [nifi.properties] not specified");
            }
        } else if (flowConfigurationRequested) {
            logger.info("Application Properties [nifi.properties] not modified based on provided arguments");
        } else {
            logger.info("Started processing Application Properties [{}]", applicationPropertiesPath);

            final SensitivePropertyProviderFactory sensitivePropertyProviderFactory = getSensitivePropertyProviderFactory();
            final SensitivePropertyProvider sensitivePropertyProvider = sensitivePropertyProviderFactory.getProvider(protectionScheme);
            final Set<String> sensitivePropertyNames = getSensitivePropertyNames();
            final FileTransformer fileTransformer = new ApplicationPropertiesFileTransformer(applicationProperties, sensitivePropertyProvider, sensitivePropertyNames);
            runFileTransformer(fileTransformer, applicationPropertiesPath, outputApplicationPropertiesPath);

            logger.info("Completed processing Application Properties [{}]", applicationPropertiesPath);
        }
    }

    private void processFlowConfiguration(final ApplicationProperties applicationProperties) {
        if (flowConfigurationPath == null) {
            if (verboseModeEnabled) {
                logger.info("Flow Configuration not specified");
            }
        } else {
            logger.info("Started processing Flow Configuration [{}]", flowConfigurationPath);

            final PropertyEncryptor inputPropertyEncryptor = getInputPropertyEncryptor(applicationProperties);
            final PropertyEncryptor outputPropertyEncryptor = getOutputPropertyEncryptor(applicationProperties);
            final FileTransformer fileTransformer = new FlowConfigurationFileTransformer(inputPropertyEncryptor, outputPropertyEncryptor);
            runFileTransformer(fileTransformer, flowConfigurationPath, outputFlowConfigurationPath);

            logger.info("Completed processing Flow Configuration [{}]", flowConfigurationPath);
        }
    }

    private void processLoginIdentityProviders() {
        if (loginIdentityProvidersPath == null) {
            if (verboseModeEnabled) {
                logger.info("Login Identity Providers not specified");
            }
        } else if (flowConfigurationRequested) {
            logger.info("Login Identity Providers not modified based on provided arguments");
        } else {
            logger.info("Started processing Login Identity Providers [{}]", loginIdentityProvidersPath);

            final SensitivePropertyProvider inputSensitivePropertyProvider = getInputSensitivePropertyProvider();
            final SensitivePropertyProviderFactory sensitivePropertyProviderFactory = getSensitivePropertyProviderFactory();
            final FileTransformer fileTransformer = new XmlFileTransformer(inputSensitivePropertyProvider, sensitivePropertyProviderFactory, protectionScheme);
            runFileTransformer(fileTransformer, loginIdentityProvidersPath, outputLoginIdentityProvidersPath);

            logger.info("Completed processing Login Identity Providers [{}]", loginIdentityProvidersPath);
        }
    }

    private PropertyEncryptor getInputPropertyEncryptor(final ApplicationProperties applicationProperties) {
        final String applicationSensitivePropertiesKey = applicationProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY);
        if (applicationSensitivePropertiesKey == null) {
            throw new IllegalArgumentException(String.format("Sensitive Properties Key [%s] not found in Application Properties", NiFiProperties.SENSITIVE_PROPS_KEY));
        }

        final String sensitivePropertiesAlgorithm = applicationProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, DEFAULT_PROPERTIES_ALGORITHM);
        return new PropertyEncryptorBuilder(applicationSensitivePropertiesKey).setAlgorithm(sensitivePropertiesAlgorithm).build();
    }

    private PropertyEncryptor getOutputPropertyEncryptor(final ApplicationProperties applicationProperties) {
        if (sensitivePropertiesKey == null) {
            throw new IllegalArgumentException("Sensitive Properties Key not provided");
        }

        final String sensitivePropertiesAlgorithm = applicationProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, DEFAULT_PROPERTIES_ALGORITHM);
        final String outputPropertiesAlgorithm = Objects.requireNonNullElse(newFlowAlgorithm, sensitivePropertiesAlgorithm);

        if (verboseModeEnabled) {
            logger.info("Output Sensitive Properties Algorithm configured [{}]", outputPropertiesAlgorithm);
        }

        return new PropertyEncryptorBuilder(sensitivePropertiesKey).setAlgorithm(outputPropertiesAlgorithm).build();
    }

    private Set<String> getSensitivePropertyNames() {
        final ApplicationPropertiesProtector<ProtectedNiFiProperties, NiFiProperties> protector = new ApplicationPropertiesProtector<>(new ProtectedNiFiProperties());
        return Set.copyOf(protector.getSensitivePropertyKeys());
    }
}
