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
package org.apache.nifi.flow.encryptor.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.nifi.encrypt.PropertyEncryptionMethod;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.flow.encryptor.FlowEncryptor;
import org.apache.nifi.flow.encryptor.JsonFlowEncryptor;

/**
 * Flow Encryptor Command capable of updating Sensitive Properties Key or Algorithm as well as Flow Configuration
 */
class FlowEncryptorCommand implements Runnable {
    protected static final String PROPERTIES_FILE_PATH = "nifi.properties.file.path";

    protected static final String PROPS_KEY = "nifi.sensitive.props.key";

    protected static final String PROPS_ALGORITHM = "nifi.sensitive.props.algorithm";

    protected static final String CONFIGURATION_FILE = "nifi.flow.configuration.file";

    private static final String FLOW_PREFIX = "nifi.flow.";

    private static final String GZ_EXTENSION = ".gz";

    private static final String DEFAULT_PROPERTIES_ALGORITHM = PropertyEncryptionMethod.NIFI_PBKDF2_AES_GCM_256.name();

    private static final String SENSITIVE_PROPERTIES_KEY = String.format("%s=", PROPS_KEY);

    private static final String SENSITIVE_PROPERTIES_ALGORITHM = String.format("%s=", PROPS_ALGORITHM);

    private String requestedPropertiesKey;

    private String requestedPropertiesAlgorithm;

    void setRequestedPropertiesKey(final String requestedPropertiesKey) {
        this.requestedPropertiesKey = Objects.requireNonNull(requestedPropertiesKey, "Key required");
    }

    void setRequestedPropertiesAlgorithm(final String requestedPropertiesAlgorithm) {
        this.requestedPropertiesAlgorithm = Objects.requireNonNull(requestedPropertiesAlgorithm, "Algorithm required");
    }

    /**
     * Run command using nifi.properties location read from System Properties
     */
    @Override
    public void run() {
        final String propertiesFilePath = System.getProperty(PROPERTIES_FILE_PATH);
        if (propertiesFilePath == null) {
            throw new IllegalStateException(String.format("System property not defined [%s]", PROPERTIES_FILE_PATH));
        }
        final File propertiesFile = new File(propertiesFilePath);
        final Properties properties = loadProperties(propertiesFile);

        processFlowConfigurationFiles(properties);

        try {
            storeProperties(propertiesFile);
            System.out.printf("NiFi Properties Processed [%s]%n", propertiesFilePath);
        } catch (final IOException e) {
            final String message = String.format("Failed to Process NiFi Properties [%s]", propertiesFilePath);
            throw new UncheckedIOException(message, e);
        }
    }

    private void processFlowConfigurationFiles(final Properties properties) {
        final String outputAlgorithm = requestedPropertiesAlgorithm == null ? getAlgorithm(properties) : requestedPropertiesAlgorithm;
        final String outputKey;

        if (requestedPropertiesKey == null) {
            final String inputKey = properties.getProperty(PROPS_KEY);
            if (inputKey == null || inputKey.isBlank()) {
                throw new IllegalStateException("Sensitive Properties Key [%s] not specified".formatted(PROPS_KEY));
            } else {
                outputKey = inputKey;
            }
        } else {
            outputKey = requestedPropertiesKey;
        }

        final PropertyEncryptor outputEncryptor = getPropertyEncryptor(outputKey, outputAlgorithm);

        final String configurationFileProperty = properties.getProperty(CONFIGURATION_FILE);
        if (configurationFileProperty == null || configurationFileProperty.isEmpty()) {
            throw new IllegalStateException("Flow Configuration Property not specified [%s]".formatted(configurationFileProperty));
        } else {
            final File configurationFile = new File(configurationFileProperty);
            if (configurationFile.exists()) {
                processFlowConfiguration(configurationFile, properties, outputEncryptor);
            } else {
                throw new IllegalStateException("Flow Configuration File not found [%s]".formatted(configurationFileProperty));
            }
        }
    }

    private void processFlowConfiguration(final File flowConfigurationFile, final Properties properties, final PropertyEncryptor outputEncryptor) {
        try (final InputStream flowInputStream = new GZIPInputStream(new FileInputStream(flowConfigurationFile))) {
            final File flowOutputFile = getFlowOutputFile();
            final Path flowOutputPath = flowOutputFile.toPath();
            try (final OutputStream flowOutputStream = new GZIPOutputStream(new FileOutputStream(flowOutputFile))) {
                final String inputAlgorithm = getAlgorithm(properties);
                final String inputPropertiesKey = getInputPropertiesKey(properties);
                final PropertyEncryptor inputEncryptor = getPropertyEncryptor(inputPropertiesKey, inputAlgorithm);

                final FlowEncryptor flowEncryptor = new JsonFlowEncryptor();
                flowEncryptor.processFlow(flowInputStream, flowOutputStream, inputEncryptor, outputEncryptor);
            }

            final Path flowConfigurationPath = flowConfigurationFile.toPath();
            Files.move(flowOutputPath, flowConfigurationPath, StandardCopyOption.REPLACE_EXISTING);
            System.out.printf("Flow Configuration Processed [%s]%n", flowConfigurationPath);
        } catch (final IOException | RuntimeException e) {
            throw new IllegalStateException("Failed to process Flow Configuration [%s]".formatted(flowConfigurationFile), e);
        }
    }

    private String getAlgorithm(final Properties properties) {
        String algorithm = properties.getProperty(PROPS_ALGORITHM, DEFAULT_PROPERTIES_ALGORITHM);
        if (algorithm.isEmpty()) {
            algorithm = DEFAULT_PROPERTIES_ALGORITHM;
        }
        return algorithm;
    }

    private String getInputPropertiesKey(final Properties properties) {
        final String key = properties.getProperty(PROPS_KEY);
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("Sensitive Properties Key [%s] not found".formatted(PROPS_KEY));
        }
        return key;
    }

    private File getFlowOutputFile() throws IOException {
        final File flowOutputFile = File.createTempFile(FLOW_PREFIX, GZ_EXTENSION);
        flowOutputFile.deleteOnExit();
        return flowOutputFile;
    }

    private Properties loadProperties(final File propertiesFile) {
        final Properties properties = new Properties();
        try (final FileReader reader = new FileReader(propertiesFile)) {
            properties.load(reader);
        } catch (final IOException e) {
            final String message = String.format("Failed to read NiFi Properties [%s]", propertiesFile);
            throw new UncheckedIOException(message, e);
        }
        return properties;
    }

    private void storeProperties(final File propertiesFile) throws IOException {
        final Path propertiesFilePath = propertiesFile.toPath();
        final List<String> lines = Files.readAllLines(propertiesFilePath);
        final List<String> updatedLines = lines.stream().map(line -> {
            if (line.startsWith(SENSITIVE_PROPERTIES_KEY)) {
                return requestedPropertiesKey == null ? line : SENSITIVE_PROPERTIES_KEY + requestedPropertiesKey;
            } else if (line.startsWith(SENSITIVE_PROPERTIES_ALGORITHM)) {
                return requestedPropertiesAlgorithm == null ? line : SENSITIVE_PROPERTIES_ALGORITHM + requestedPropertiesAlgorithm;
            } else {
                return line;
            }
        }).collect(Collectors.toList());
        Files.write(propertiesFilePath, updatedLines);
    }

    private PropertyEncryptor getPropertyEncryptor(final String propertiesKey, final String propertiesAlgorithm) {
        return new PropertyEncryptorBuilder(propertiesKey).setAlgorithm(propertiesAlgorithm).build();
    }
}
