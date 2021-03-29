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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertiesFileEngineConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesFileEngineConfigurationParser.class);
    private static final String PREFIX = "nifi.stateless.";

    private static final String NAR_DIRECTORY = PREFIX + "nar.directory";
    private static final String EXTENSIONS_DIRECTORY = PREFIX + "extensions.directory";
    private static final String WORKING_DIRECTORY = PREFIX + "working.directory";

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
    private static final String DEFAULT_ENCRYPTION_PASSWORD = "nifi-stateless";

    private static final Pattern EXTENSION_CLIENT_PATTERN = Pattern.compile("\\Qnifi.stateless.extension.client.\\E(.*?)\\.(.+)");


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

        final String krb5Filename = properties.getProperty(KRB5_FILE, DEFAULT_KRB5_FILENAME);
        final File krb5File = new File(krb5Filename);

        final String sensitivePropsKey = properties.getProperty(SENSITIVE_PROPS_KEY, DEFAULT_ENCRYPTION_PASSWORD);
        final SslContextDefinition sslContextDefinition = parseSslContextDefinition(properties);

        final List<ExtensionClientDefinition> extensionClients = parseExtensionClients(properties);

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
            public File getKrb5File() {
                return krb5File;
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
        };
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
        if (truststoreFile == null || truststoreFile.trim().isEmpty()) {
            return null;
        }

        final SslContextDefinition sslContextDefinition;
        sslContextDefinition = new SslContextDefinition();
        sslContextDefinition.setTruststoreFile(truststoreFile);
        sslContextDefinition.setTruststorePass(properties.getProperty(TRUSTSTORE_PASSWORD));
        sslContextDefinition.setTruststoreType(properties.getProperty(TRUSTSTORE_TYPE));

        final String keystoreFile = properties.getProperty(KEYSTORE_FILE);
        if (keystoreFile != null && !keystoreFile.trim().isEmpty()) {
            sslContextDefinition.setKeystoreFile(keystoreFile);
            sslContextDefinition.setKeystoreType(properties.getProperty(KEYSTORE_TYPE));

            final String keystorePass = properties.getProperty(KEYSTORE_PASSWORD);
            sslContextDefinition.setKeystorePass(keystorePass);

            final String explicitKeyPass = properties.getProperty(KEY_PASSWORD);
            final String keyPass = (explicitKeyPass == null || explicitKeyPass.trim().isEmpty()) ? keystorePass : explicitKeyPass;
            sslContextDefinition.setKeyPass(keyPass);
        }

        return sslContextDefinition;
    }

    private String getRequired(final Properties properties, final String key) throws StatelessConfigurationException {
        final String propertyValue = properties.getProperty(key);
        if (propertyValue == null || propertyValue.trim().isEmpty()) {
            throw new StatelessConfigurationException("Properties file is missing required property " + key);
        }

        return propertyValue.trim();
    }

}
