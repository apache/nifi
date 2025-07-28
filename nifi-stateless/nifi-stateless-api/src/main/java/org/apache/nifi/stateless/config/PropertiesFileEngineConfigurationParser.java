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

package org.apache.nifi.stateless.config;

import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PropertiesFileEngineConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesFileEngineConfigurationParser.class);
    private static final String PREFIX = "nifi.stateless.";

    private static final String NAR_DIRECTORY = PREFIX + "nar.directory";
    private static final String EXTENSIONS_DIRECTORY = PREFIX + "extensions.directory";
    private static final String READONLY_EXTENSIONS_DIRECTORY = PREFIX + "readonly.extensions.directory.";
    private static final String WORKING_DIRECTORY = PREFIX + "working.directory";
    private static final String CONTENT_REPO_DIRECTORY = PREFIX + "content.repository.directory";
    private static final String STATUS_TASK_INTERVAL = PREFIX + "status.task.interval";

    private static final String COMPONENT_ENABLE_TIMEOUT = PREFIX + "component.enableTimeout";
    private static final String PROCESSOR_START_TIMEOUT = PREFIX + "processor.startTimeout";

    private static final String TRUSTSTORE_FILE = PREFIX + "security.truststore";
    private static final String TRUSTSTORE_TYPE = PREFIX + "security.truststoreType";
    private static final String TRUSTSTORE_PASSWORD = PREFIX + "security.truststorePasswd";
    private static final String KEYSTORE_FILE = PREFIX + "security.keystore";
    private static final String KEYSTORE_TYPE = PREFIX + "security.keystoreType";
    private static final String KEYSTORE_PASSWORD = PREFIX + "security.keystorePasswd";
    private static final String KEY_PASSWORD = PREFIX + "security.keyPasswd";

    private static final String SENSITIVE_PROPS_KEY = PREFIX + "sensitive.props.key";
    private static final String KRB5_FILE = PREFIX + "kerberos.krb5.file";

    private static final String DEFAULT_KRB5_FILENAME = "/etc/krb5.conf";

    private static final Pattern EXTENSION_CLIENT_PATTERN = Pattern.compile("\\Qnifi.stateless.extension.client.\\E(.*?)\\.(.+)");

    private static final int PROPERTIES_KEY_LENGTH = 24;

    public StatelessEngineConfiguration parseEngineConfiguration(final File propertiesFile) throws IOException, StatelessConfigurationException {
        if (!propertiesFile.exists()) {
            throw new FileNotFoundException("Could not find properties file " + propertiesFile.getAbsolutePath());
        }

        final Properties properties = new Properties();
        try (final InputStream in = new FileInputStream(propertiesFile)) {
            properties.load(in);
        }

        final File narDirectory = new File(getRequired(properties, NAR_DIRECTORY));
        if (!narDirectory.exists()) {
            throw new StatelessConfigurationException("NAR Directory " + narDirectory.getAbsolutePath() + " specified in properties file does not exist");
        }

        final File workingDirectory = new File(getRequired(properties, WORKING_DIRECTORY));
        if (!workingDirectory.exists() && !workingDirectory.mkdirs()) {
            throw new StatelessConfigurationException("Working Directory " + workingDirectory.getAbsolutePath() + " specified in properties file does not exist and could not be created");
        }

        final String extensionsDirectoryFilename = properties.getProperty(EXTENSIONS_DIRECTORY);
        final File extensionsDirectory = extensionsDirectoryFilename == null ? narDirectory : new File(extensionsDirectoryFilename);
        if (!extensionsDirectory.exists() && !extensionsDirectory.mkdirs()) {
            throw new StatelessConfigurationException("Extensions Directory " + narDirectory.getAbsolutePath() + " specified in properties file does not exist and could not be created");
        }

        final List<File> readOnlyExtensionsDirectories = getReadOnlyExtensionsDirectories(properties);

        final String contentRepoDirectoryFilename = properties.getProperty(CONTENT_REPO_DIRECTORY, "");
        final File contentRepoDirectory = contentRepoDirectoryFilename.isEmpty() ? null : new File(contentRepoDirectoryFilename);

        final String krb5Filename = properties.getProperty(KRB5_FILE, DEFAULT_KRB5_FILENAME);
        final File krb5File = new File(krb5Filename);

        final String sensitivePropsKey = getSensitivePropsKey(propertiesFile, properties);
        final SslContextDefinition sslContextDefinition = parseSslContextDefinition(properties);

        final List<ExtensionClientDefinition> extensionClients = parseExtensionClients(properties);

        final String statusTaskInterval = properties.getProperty(STATUS_TASK_INTERVAL, "1 min");

        final String processorStartTimeout = properties.getProperty(PROCESSOR_START_TIMEOUT, "10 secs");
        final String componentEnableTimeout = properties.getProperty(COMPONENT_ENABLE_TIMEOUT, "10 secs");

        return new StatelessEngineConfiguration() {
            @Override
            public File getWorkingDirectory() {
                return workingDirectory;
            }

            @Override
            public File getNarDirectory() {
                return narDirectory;
            }

            @Override
            public File getExtensionsDirectory() {
                return extensionsDirectory;
            }

            @Override
            public Collection<File> getReadOnlyExtensionsDirectories() {
                return readOnlyExtensionsDirectories;
            }

            @Override
            public File getKrb5File() {
                return krb5File;
            }

            @Override
            public Optional<File> getContentRepositoryDirectory() {
                return Optional.ofNullable(contentRepoDirectory);
            }

            @Override
            public SslContextDefinition getSslContext() {
                return sslContextDefinition;
            }

            @Override
            public String getSensitivePropsKey() {
                return sensitivePropsKey;
            }

            @Override
            public List<ExtensionClientDefinition> getExtensionClients() {
                return extensionClients;
            }

            @Override
            public String getStatusTaskInterval() {
                return statusTaskInterval;
            }

            @Override
            public String getProcessorStartTimeout() {
                return processorStartTimeout;
            }

            @Override
            public String getComponentEnableTimeout() {
                return componentEnableTimeout;
            }
        };
    }


    private List<File> getReadOnlyExtensionsDirectories(final Properties properties) {
        return properties.keySet().stream()
            .map(Object::toString)
            .filter(key -> key.startsWith(READONLY_EXTENSIONS_DIRECTORY))
            .map(properties::getProperty)
            .map(File::new)
            .collect(Collectors.toList());
    }

    private List<ExtensionClientDefinition> parseExtensionClients(final Properties properties) {
        final Map<String, ExtensionClientDefinition> extensionClientDefinitions = new LinkedHashMap<>();

        for (final String propertyName : properties.stringPropertyNames()) {
            final Matcher matcher = EXTENSION_CLIENT_PATTERN.matcher(propertyName);
            if (!matcher.matches()) {
                continue;
            }

            // For a property name like:
            // nifi.stateless.extension.client.c1.type=nexus
            // We consider 'c1' the <client key> and 'type' the <relative property name>
            final String clientKey = matcher.group(1);
            final ExtensionClientDefinition definition = extensionClientDefinitions.computeIfAbsent(clientKey, key -> new ExtensionClientDefinition());
            final String relativePropertyName = matcher.group(2);
            final String propertyValue = properties.getProperty(propertyName);

            switch (relativePropertyName) {
                case "type":
                    definition.setExtensionClientType(propertyValue);
                    break;
                case "baseUrl":
                    definition.setBaseUrl(propertyValue);
                    break;
                case "timeout":
                    definition.setCommsTimeout(propertyValue);
                    break;
                case "useSslContext":
                    definition.setUseSslContext(Boolean.parseBoolean(propertyValue));
                    break;
                default:
                    logger.warn("Encountered invalid property: <{}>. Will ignore this property.", propertyName);
                    break;
            }

        }

        return new ArrayList<>(extensionClientDefinitions.values());
    }

    private SslContextDefinition parseSslContextDefinition(final Properties properties) {
        final String truststoreFile = properties.getProperty(TRUSTSTORE_FILE);
        if (truststoreFile == null || truststoreFile.isBlank()) {
            return null;
        }

        final SslContextDefinition sslContextDefinition;
        sslContextDefinition = new SslContextDefinition();
        sslContextDefinition.setTruststoreFile(truststoreFile);
        sslContextDefinition.setTruststorePass(properties.getProperty(TRUSTSTORE_PASSWORD));
        sslContextDefinition.setTruststoreType(properties.getProperty(TRUSTSTORE_TYPE));

        final String keystoreFile = properties.getProperty(KEYSTORE_FILE);
        if (keystoreFile != null && !keystoreFile.isBlank()) {
            sslContextDefinition.setKeystoreFile(keystoreFile);
            sslContextDefinition.setKeystoreType(properties.getProperty(KEYSTORE_TYPE));

            final String keystorePass = properties.getProperty(KEYSTORE_PASSWORD);
            sslContextDefinition.setKeystorePass(keystorePass);

            final String explicitKeyPass = properties.getProperty(KEY_PASSWORD);
            final String keyPass = (explicitKeyPass == null || explicitKeyPass.isBlank()) ? keystorePass : explicitKeyPass;
            sslContextDefinition.setKeyPass(keyPass);
        }

        return sslContextDefinition;
    }

    private String getRequired(final Properties properties, final String key) throws StatelessConfigurationException {
        final String propertyValue = properties.getProperty(key);
        if (propertyValue == null || propertyValue.isBlank()) {
            throw new StatelessConfigurationException("Properties file is missing required property " + key);
        }

        return propertyValue.trim();
    }

    private String getSensitivePropsKey(final File propertiesFile, final Properties properties) {
        String sensitivePropsKey = properties.getProperty(SENSITIVE_PROPS_KEY);
        if (sensitivePropsKey == null || sensitivePropsKey.isEmpty()) {
            logger.warn("Generating Random Properties Encryption Key [{}]", SENSITIVE_PROPS_KEY);
            final SecureRandom secureRandom = new SecureRandom();
            final byte[] sensitivePropertiesKeyBinary = new byte[PROPERTIES_KEY_LENGTH];
            secureRandom.nextBytes(sensitivePropertiesKeyBinary);
            final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
            sensitivePropsKey = encoder.encodeToString(sensitivePropertiesKeyBinary);

            properties.put(SENSITIVE_PROPS_KEY, sensitivePropsKey);
            try (final OutputStream outputStream = new FileOutputStream(propertiesFile)) {
                properties.store(outputStream, StatelessEngineConfiguration.class.getSimpleName());
            } catch (final IOException e) {
                final String message = String.format("Store Configuration Properties [%s] Failed", propertiesFile);
                throw new UncheckedIOException(message, e);
            }
        }
        return sensitivePropsKey;
    }
}
