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

import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.flow.encryptor.FlowEncryptor;
import org.apache.nifi.flow.encryptor.StandardFlowEncryptor;
import org.apache.nifi.security.util.EncryptionMethod;

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
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Set Sensitive Properties Key for NiFi Properties and update encrypted Flow Configuration
 */
public class SetSensitivePropertiesKey {
    protected static final String PROPERTIES_FILE_PATH = "nifi.properties.file.path";

    protected static final String PROPS_KEY = "nifi.sensitive.props.key";

    protected static final String PROPS_ALGORITHM = "nifi.sensitive.props.algorithm";

    protected static final String CONFIGURATION_FILE = "nifi.flow.configuration.file";

    private static final int MINIMUM_REQUIRED_LENGTH = 12;

    private static final String FLOW_XML_PREFIX = "flow.xml.";

    private static final String GZ_EXTENSION = ".gz";

    private static final String DEFAULT_PROPERTIES_ALGORITHM = EncryptionMethod.MD5_256AES.getAlgorithm();

    private static final String DEFAULT_PROPERTIES_KEY = "nififtw!";

    private static final String SENSITIVE_PROPERTIES_KEY = String.format("%s=", PROPS_KEY);

    public static void main(final String[] arguments) {
        if (arguments.length == 1) {
            final String outputPropertiesKey = arguments[0];
            if (outputPropertiesKey.length() < MINIMUM_REQUIRED_LENGTH) {
                System.err.printf("Sensitive Properties Key length less than required [%d]%n", MINIMUM_REQUIRED_LENGTH);
            } else {
                run(outputPropertiesKey);
            }
        } else {
            System.err.printf("Unexpected number of arguments [%d]%n", arguments.length);
            System.err.printf("Usage: %s <sensitivePropertiesKey>%n", SetSensitivePropertiesKey.class.getSimpleName());
        }
    }

    private static void run(final String outputPropertiesKey) {
        final String propertiesFilePath = System.getProperty(PROPERTIES_FILE_PATH);
        final File propertiesFile = new File(propertiesFilePath);
        final Properties properties = loadProperties(propertiesFile);

        final File flowConfigurationFile = getFlowConfigurationFile(properties);
        try {
            storeProperties(propertiesFile, outputPropertiesKey);
            System.out.printf("NiFi Properties Processed [%s]%n", propertiesFilePath);
        } catch (final IOException e) {
            final String message = String.format("Failed to Process NiFi Properties [%s]", propertiesFilePath);
            throw new UncheckedIOException(message, e);
        }

        if (flowConfigurationFile.exists()) {
            final String algorithm = getAlgorithm(properties);
            final PropertyEncryptor outputEncryptor = getPropertyEncryptor(outputPropertiesKey, algorithm);
            processFlowConfiguration(properties, outputEncryptor);
        }
    }

    private static void processFlowConfiguration(final Properties properties, final PropertyEncryptor outputEncryptor) {
        final File flowConfigurationFile = getFlowConfigurationFile(properties);
        try (final InputStream flowInputStream = new GZIPInputStream(new FileInputStream(flowConfigurationFile))) {
            final File flowOutputFile = getFlowOutputFile();
            final Path flowOutputPath = flowOutputFile.toPath();
            try (final OutputStream flowOutputStream = new GZIPOutputStream(new FileOutputStream(flowOutputFile))) {
                final String inputAlgorithm = getAlgorithm(properties);
                final String inputPropertiesKey = getKey(properties);
                final PropertyEncryptor inputEncryptor = getPropertyEncryptor(inputPropertiesKey, inputAlgorithm);

                final FlowEncryptor flowEncryptor = new StandardFlowEncryptor();
                flowEncryptor.processFlow(flowInputStream, flowOutputStream, inputEncryptor, outputEncryptor);
            }

            final Path flowConfigurationPath = flowConfigurationFile.toPath();
            Files.move(flowOutputPath, flowConfigurationPath, StandardCopyOption.REPLACE_EXISTING);
            System.out.printf("Flow Configuration Processed [%s]%n", flowConfigurationPath);
        } catch (final IOException|RuntimeException e) {
            System.err.printf("Failed to process Flow Configuration [%s]%n", flowConfigurationFile);
            e.printStackTrace();
        }
    }

    private static String getAlgorithm(final Properties properties) {
        String algorithm = properties.getProperty(PROPS_ALGORITHM, DEFAULT_PROPERTIES_ALGORITHM);
        if (algorithm.length() == 0) {
            algorithm = DEFAULT_PROPERTIES_ALGORITHM;
        }
        return algorithm;
    }

    private static String getKey(final Properties properties) {
        String key = properties.getProperty(PROPS_KEY, DEFAULT_PROPERTIES_KEY);
        if (key.length() == 0) {
            key = DEFAULT_PROPERTIES_KEY;
        }
        return key;
    }

    private static File getFlowOutputFile() throws IOException {
        final File flowOutputFile = File.createTempFile(FLOW_XML_PREFIX, GZ_EXTENSION);
        flowOutputFile.deleteOnExit();
        return flowOutputFile;
    }

    private static Properties loadProperties(final File propertiesFile) {
        final Properties properties = new Properties();
        try (final FileReader reader = new FileReader(propertiesFile)) {
            properties.load(reader);
        } catch (final IOException e) {
            final String message = String.format("Failed to read NiFi Properties [%s]", propertiesFile);
            throw new UncheckedIOException(message, e);
        }
        return properties;
    }

    private static void storeProperties(final File propertiesFile, final String propertiesKey) throws IOException {
        final Path propertiesFilePath = propertiesFile.toPath();
        final List<String> lines = Files.readAllLines(propertiesFilePath);
        final List<String> updatedLines = lines.stream().map(line -> {
            if (line.startsWith(SENSITIVE_PROPERTIES_KEY)) {
                return SENSITIVE_PROPERTIES_KEY + propertiesKey;
            } else {
                return line;
            }
        }).collect(Collectors.toList());
        Files.write(propertiesFilePath, updatedLines);
    }

    private static PropertyEncryptor getPropertyEncryptor(final String propertiesKey, final String propertiesAlgorithm) {
        return new PropertyEncryptorBuilder(propertiesKey).setAlgorithm(propertiesAlgorithm).build();
    }

    private static File getFlowConfigurationFile(final Properties properties) {
        return new File(properties.getProperty(CONFIGURATION_FILE));
    }
}
