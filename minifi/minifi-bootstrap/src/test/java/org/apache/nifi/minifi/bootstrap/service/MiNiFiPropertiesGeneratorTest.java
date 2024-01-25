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

package org.apache.nifi.minifi.bootstrap.service;

import static java.lang.Boolean.TRUE;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.SPACE;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiPropertiesGenerator.DEFAULT_SENSITIVE_PROPERTIES_ENCODING_ALGORITHM;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiPropertiesGenerator.MINIFI_TO_NIFI_PROPERTY_MAPPING;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiPropertiesGenerator.NIFI_PROPERTIES_WITH_DEFAULT_VALUES_AND_COMMENTS;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.MINIFI_APP_LOG_FILE;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.MINIFI_BOOTSTRAP_FILE_PATH;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.MINIFI_BOOTSTRAP_LOG_FILE;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.MINIFI_LOG_DIRECTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.minifi.properties.BootstrapProperties;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MiNiFiPropertiesGeneratorTest {

    @TempDir
    private Path tempDir;

    private Path configDirectory;
    private Path bootstrapPropertiesFile;
    private Path minifiPropertiesFile;

    private MiNiFiPropertiesGenerator testPropertiesGenerator;

    @BeforeEach
    public void setup() throws IOException {
        configDirectory = tempDir.toAbsolutePath().resolve("conf");
        Files.createDirectories(configDirectory);
        bootstrapPropertiesFile = configDirectory.resolve("bootstrap.conf");
        minifiPropertiesFile = configDirectory.resolve("minifi.properties");

        testPropertiesGenerator = new MiNiFiPropertiesGenerator();
    }

    @Test
    public void testGenerateDefaultNiFiProperties() throws ConfigurationChangeException {
        // given
        BootstrapProperties bootstrapProperties = createBootstrapProperties(Map.of());

        // when
        testPropertiesGenerator.generateMinifiProperties(configDirectory.toString(), bootstrapProperties);

        // then
        List<String> expectedMiNiFiProperties = NIFI_PROPERTIES_WITH_DEFAULT_VALUES_AND_COMMENTS.stream()
            .map(triplet -> triplet.getLeft() + "=" + triplet.getMiddle())
            .collect(toList());
        List<String> resultMiNiFiProperties = loadMiNiFiProperties().entrySet()
            .stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(toList());
        assertTrue(resultMiNiFiProperties.containsAll(expectedMiNiFiProperties));
    }

    @Test
    public void testMiNiFiPropertiesMappedToAppropriateNiFiProperties() throws ConfigurationChangeException {
        // given
        BootstrapProperties bootstrapProperties = createBootstrapProperties(Stream.of(
                MiNiFiProperties.NIFI_MINIFI_FLOW_CONFIG.getKey(),
                MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE.getKey(),
                MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE_TYPE.getKey(),
                MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE_PASSWD.getKey(),
                MiNiFiProperties.NIFI_MINIFI_SECURITY_KEY_PASSWD.getKey(),
                MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE.getKey(),
                MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE_TYPE.getKey(),
                MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE_PASSWD.getKey())
            .collect(toMap(Function.identity(), __ -> randomUUID().toString()))
        );

        // when
        testPropertiesGenerator.generateMinifiProperties(configDirectory.toString(), bootstrapProperties);

        // then
        Properties miNiFiProperties = loadMiNiFiProperties();
        assertTrue(
            MINIFI_TO_NIFI_PROPERTY_MAPPING.entrySet().stream()
                .allMatch(entry -> Objects.equals(bootstrapProperties.getProperty(entry.getKey()), miNiFiProperties.getProperty(entry.getValue()))));
    }

    @Test
    public void testSensitivePropertiesAreGeneratedWhenNotProvidedInBootstrap() throws ConfigurationChangeException {
        // given
        BootstrapProperties bootstrapProperties = createBootstrapProperties(Map.of());

        // when
        testPropertiesGenerator.generateMinifiProperties(configDirectory.toString(), bootstrapProperties);

        // then
        Properties miNiFiProperties = loadMiNiFiProperties();
        assertTrue(isNotBlank(miNiFiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY)));
        assertEquals(DEFAULT_SENSITIVE_PROPERTIES_ENCODING_ALGORITHM, miNiFiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM));
    }

    @Test
    public void testSensitivePropertiesAreUsedWhenProvidedInBootstrap() throws ConfigurationChangeException {
        // given
        String sensitivePropertiesKey = "sensitive_properties_key";
        String sensitivePropertiesAlgorithm = "sensitive_properties_algorithm";
        BootstrapProperties bootstrapProperties = createBootstrapProperties(Map.of(
            MiNiFiProperties.NIFI_MINIFI_SENSITIVE_PROPS_KEY.getKey(), sensitivePropertiesKey,
            MiNiFiProperties.NIFI_MINIFI_SENSITIVE_PROPS_ALGORITHM.getKey(), sensitivePropertiesAlgorithm
        ));

        // when
        testPropertiesGenerator.generateMinifiProperties(configDirectory.toString(), bootstrapProperties);

        // then
        Properties miNiFiProperties = loadMiNiFiProperties();
        assertEquals(sensitivePropertiesKey, miNiFiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY));
        assertEquals(sensitivePropertiesAlgorithm, miNiFiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM));
    }

    @Test
    public void testNonBlankC2PropertiesAreCopiedToMiNiFiProperties() throws ConfigurationChangeException {
        // given
        BootstrapProperties bootstrapProperties = createBootstrapProperties(Map.of(
            MiNiFiProperties.C2_ENABLE.getKey(), TRUE.toString(),
            MiNiFiProperties.C2_AGENT_CLASS.getKey(), EMPTY,
            MiNiFiProperties.C2_AGENT_IDENTIFIER.getKey(), SPACE
        ));

        // when
        testPropertiesGenerator.generateMinifiProperties(configDirectory.toString(), bootstrapProperties);

        // then
        Properties miNiFiProperties = loadMiNiFiProperties();
        assertEquals(TRUE.toString(), miNiFiProperties.getProperty(MiNiFiProperties.C2_ENABLE.getKey()));
        assertNull(miNiFiProperties.getProperty(MiNiFiProperties.C2_AGENT_CLASS.getKey()));
        assertNull(miNiFiProperties.getProperty(MiNiFiProperties.C2_AGENT_IDENTIFIER.getKey()));
        assertNull(miNiFiProperties.getProperty(MiNiFiProperties.C2_REST_URL.getKey()));
    }

    @Test
    public void testDefaultNiFiPropertiesAreOverridden() throws ConfigurationChangeException {
        // given
        String archiveDir = "/path/to";
        BootstrapProperties bootstrapProperties = createBootstrapProperties(Map.of(
            NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED, TRUE.toString(),
            NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_DIR, archiveDir
        ));

        // when
        testPropertiesGenerator.generateMinifiProperties(configDirectory.toString(), bootstrapProperties);

        // then
        List<String> expectedMiNiFiProperties = concat(
            NIFI_PROPERTIES_WITH_DEFAULT_VALUES_AND_COMMENTS.stream()
                .filter(triplet ->
                    !triplet.getLeft().equals(NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED)
                        && !triplet.getLeft().equals(NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_DIR))
                .map(triplet -> triplet.getLeft() + "=" + triplet.getMiddle()),
            Stream.of(
                NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED + "=" + TRUE,
                NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_DIR + "=" + archiveDir
            ))
            .collect(toList());
        List<String> resultMiNiFiProperties = loadMiNiFiProperties().entrySet()
            .stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(toList());
        assertTrue(resultMiNiFiProperties.containsAll(expectedMiNiFiProperties));

    }

    @Test
    public void testArbitraryNiFiPropertyCanBePassedViaBootstrapConf() throws ConfigurationChangeException {
        // given
        BootstrapProperties bootstrapProperties = createBootstrapProperties(Map.of(
            "nifi.new.property", "new_property_value",
            "nifi.other.new.property", "other_new_property_value"
        ));

        // when
        testPropertiesGenerator.generateMinifiProperties(configDirectory.toString(), bootstrapProperties);

        // then
        Properties miNiFiProperties = loadMiNiFiProperties();
        assertEquals("new_property_value", miNiFiProperties.getProperty("nifi.new.property"));
        assertEquals("other_new_property_value", miNiFiProperties.getProperty("nifi.other.new.property"));

    }

    @Test
    public void bootstrapFileAndLogPropertiesAreGeneratedIntoMiNiFiProperties() throws ConfigurationChangeException {
        // given
        BootstrapProperties bootstrapProperties = createBootstrapProperties(Map.of());

        // when
        testPropertiesGenerator.generateMinifiProperties(configDirectory.toString(), bootstrapProperties);

        // then
        Properties miNiFiProperties = loadMiNiFiProperties();
        assertTrue(List.of(MINIFI_BOOTSTRAP_FILE_PATH, MINIFI_LOG_DIRECTORY, MINIFI_APP_LOG_FILE, MINIFI_BOOTSTRAP_LOG_FILE)
            .stream()
            .map(miNiFiProperties::getProperty)
            .allMatch(StringUtils::isNotBlank)
        );
    }

    private BootstrapProperties createBootstrapProperties(Map<String, String> keyValues) {
        try (OutputStream outputStream = newOutputStream(bootstrapPropertiesFile)) {
            Properties properties = new Properties();
            BootstrapProperties bootstrapProperties = new BootstrapProperties(keyValues);
            properties.putAll(keyValues);
            properties.store(outputStream, EMPTY);
            return bootstrapProperties;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Properties loadMiNiFiProperties() {
        try (InputStream inputStream = newInputStream(minifiPropertiesFile)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

