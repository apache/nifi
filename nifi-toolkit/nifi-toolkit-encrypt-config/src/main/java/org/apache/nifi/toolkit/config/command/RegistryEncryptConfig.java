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

import org.apache.nifi.properties.ApplicationProperties;
import org.apache.nifi.properties.ApplicationPropertiesProtector;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.properties.NiFiRegistryPropertiesLoader;
import org.apache.nifi.registry.properties.ProtectedNiFiRegistryProperties;
import org.apache.nifi.toolkit.config.transformer.ApplicationPropertiesFileTransformer;
import org.apache.nifi.toolkit.config.transformer.FileTransformer;
import org.apache.nifi.toolkit.config.transformer.XmlFileTransformer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

/**
 * Encrypt Config Registry implementation for NiFi Registry configuration files
 */
@Command
public class RegistryEncryptConfig extends SharedEncryptConfig implements Runnable {

    static final String BOOTSTRAP_ROOT_KEY_PROPERTY = "nifi.registry.bootstrap.sensitive.key";

    @Option(
            names = {"-r", "--nifiRegistryProperties"},
            description = "Path to file containing Application Properties [nifi-registry.properties] that will be updated unless the output argument is provided"
    )
    Path applicationPropertiesPath;

    @Option(
            names = {"-R", "--outputNiFiProperties"},
            description = "Path to output file for Application Properties [nifi-registry.properties] with property protection applied"
    )
    Path outputApplicationPropertiesPath;

    @Option(
            names = {"-i", "--identityProviders"},
            description = "Path to file containing Identity Providers [identity-providers.xml] configuration that will be updated unless the output argument is provided"
    )
    Path identityProvidersPath;

    @Option(
            names = {"-I", "--outputIdentityProviders"},
            description = "Path to output file for Identity Providers [identity-providers.xml] with property protection applied"
    )
    Path outputIdentityProvidersPath;

    @Override
    public void run() {
        final ApplicationProperties applicationProperties = loadApplicationProperties();

        processBootstrapConf(BOOTSTRAP_ROOT_KEY_PROPERTY);
        processApplicationProperties(applicationProperties);
        processAuthorizers();
        processIdentityProviders();
    }

    private ApplicationProperties loadApplicationProperties() {
        final ApplicationProperties applicationProperties;

        if (applicationPropertiesPath == null) {
            applicationProperties = new ApplicationProperties(Collections.emptyMap());
            if (verboseModeEnabled) {
                logger.info("Application Properties [nifi-registry.properties] not specified");
            }
        } else if (Files.notExists(applicationPropertiesPath)) {
            throw new IllegalArgumentException(String.format("Application Properties [nifi-registry.properties] not found [%s]", applicationPropertiesPath));
        } else {
            final NiFiRegistryPropertiesLoader propertiesLoader = new NiFiRegistryPropertiesLoader();

            final String inputRootKey = getInputRootKey();
            final String rootKey = inputRootKey == null ? getRootKey() : inputRootKey;
            propertiesLoader.setKeyHex(rootKey);

            applicationProperties = propertiesLoader.load(applicationPropertiesPath.toFile());
        }

        return applicationProperties;
    }

    private void processApplicationProperties(final ApplicationProperties applicationProperties) {
        if (applicationPropertiesPath == null) {
            if (verboseModeEnabled) {
                logger.info("Application Properties [nifi-registry.properties] not specified");
            }
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

    private void processIdentityProviders() {
        if (identityProvidersPath == null) {
            if (verboseModeEnabled) {
                logger.info("Identity Providers not specified");
            }
        } else {
            logger.info("Started processing Identity Providers [{}]", identityProvidersPath);

            final SensitivePropertyProvider inputSensitivePropertyProvider = getInputSensitivePropertyProvider();
            final SensitivePropertyProviderFactory sensitivePropertyProviderFactory = getSensitivePropertyProviderFactory();
            final FileTransformer fileTransformer = new XmlFileTransformer(inputSensitivePropertyProvider, sensitivePropertyProviderFactory, protectionScheme);
            runFileTransformer(fileTransformer, identityProvidersPath, outputIdentityProvidersPath);

            logger.info("Completed processing Identity Providers [{}]", identityProvidersPath);
        }
    }

    private Set<String> getSensitivePropertyNames() {
        final ApplicationPropertiesProtector<ProtectedNiFiRegistryProperties, NiFiRegistryProperties> protector = new ApplicationPropertiesProtector<>(new ProtectedNiFiRegistryProperties());
        return Set.copyOf(protector.getSensitivePropertyKeys());
    }
}
