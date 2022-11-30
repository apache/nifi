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

package org.apache.nifi.minifi.c2.command;

import static org.apache.nifi.minifi.MiNiFiProperties.C2_ENABLE;
import static org.apache.nifi.minifi.c2.command.UpdatePropertiesPropertyProvider.AVAILABLE_PROPERTIES;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.BOOTSTRAP_UPDATED_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PropertiesPersisterTest {

    private static final String EXTRA_PROPERTY_KEY = "anExtraPropertyWhichShouldNotBeModified";
    private static final String EXTRA_PROPERTY_VALUE = "propertyValue";
    private static final String FALSE = "false";
    private static final String TRUE = "true";
    private static final String BOOLEAN_VALIDATOR = "BOOLEAN_VALIDATOR";
    private static final String UNKNOWN = "unknown";
    private static final String VALID = "VALID";

    @Mock
    private UpdatePropertiesPropertyProvider updatePropertiesPropertyProvider;
    @TempDir
    private File tempDir;

    private PropertiesPersister propertiesPersister;
    private String bootstrapConfigFileLocation;
    private String bootstrapNewConfigFileLocation;

    @BeforeEach
    void setup() {
        bootstrapConfigFileLocation = tempDir.getAbsolutePath() + "/bootstrap.conf";
        bootstrapNewConfigFileLocation = tempDir.getAbsolutePath() + "/" + BOOTSTRAP_UPDATED_FILE_NAME;
        propertiesPersister = new PropertiesPersister(updatePropertiesPropertyProvider, bootstrapConfigFileLocation);
    }

    @Test
    void shouldPersistPropertiesThrowIllegalArgumentExceptionIfParameterContainsUnknownProperty() {
        when(updatePropertiesPropertyProvider.getProperties()).thenReturn(Collections.singletonMap(AVAILABLE_PROPERTIES, Collections.singleton(new UpdatableProperty("propertyName",
            EXTRA_PROPERTY_VALUE, VALID))));
        assertThrows(IllegalArgumentException.class, () -> propertiesPersister.persistProperties(Collections.singletonMap(UNKNOWN, UNKNOWN)));
    }

    @Test
    void shouldPersistPropertiesThrowIllegalArgumentExceptionIfParameterContainsInvalidPropertyValue() {
        when(updatePropertiesPropertyProvider.getProperties()).thenReturn(Collections.singletonMap(AVAILABLE_PROPERTIES, Collections.singleton(new UpdatableProperty(C2_ENABLE.getKey(), null,
            BOOLEAN_VALIDATOR))));
        assertThrows(IllegalArgumentException.class, () ->
            propertiesPersister.persistProperties(Collections.singletonMap(C2_ENABLE.getKey(), UNKNOWN)));
    }

    @Test
    void shouldPersistPropertiesReturnFalseIfThereIsNoNewPropertyValueInParameter() {
        when(updatePropertiesPropertyProvider.getProperties()).thenReturn(Collections.singletonMap(AVAILABLE_PROPERTIES, Collections.singleton(new UpdatableProperty(C2_ENABLE.getKey(),
            TRUE, BOOLEAN_VALIDATOR))));

        assertFalse(propertiesPersister.persistProperties(Collections.singletonMap(C2_ENABLE.getKey(), TRUE)));
    }

    @Test
    void shouldAddNewLinesForPropertiesWhichDidNotExistsBeforeInBootstrapConf() throws IOException {
        new File(bootstrapConfigFileLocation).createNewFile();
        when(updatePropertiesPropertyProvider.getProperties()).thenReturn(Collections.singletonMap(AVAILABLE_PROPERTIES, Collections.singleton(new UpdatableProperty(C2_ENABLE.getKey(),
            TRUE, BOOLEAN_VALIDATOR))));

        propertiesPersister.persistProperties(Collections.singletonMap(C2_ENABLE.getKey(), FALSE));

        Properties properties = readUpdatedProperties();
        assertEquals(1, properties.stringPropertyNames().size());
        assertEquals(FALSE, properties.getProperty(C2_ENABLE.getKey()));
    }

    @Test
    void shouldModifyPropertiesForPreviouslyCommentedLines() {
        writeBootstrapFile("#" + C2_ENABLE.getKey() + "=" + TRUE);
        when(updatePropertiesPropertyProvider.getProperties()).thenReturn(Collections.singletonMap(AVAILABLE_PROPERTIES, Collections.singleton(new UpdatableProperty(C2_ENABLE.getKey(),
            TRUE, BOOLEAN_VALIDATOR))));

        propertiesPersister.persistProperties(Collections.singletonMap(C2_ENABLE.getKey(), FALSE));

        Properties properties = readUpdatedProperties();
        assertEquals(2, properties.stringPropertyNames().size());
        assertEquals(FALSE, properties.getProperty(C2_ENABLE.getKey()));
        assertEquals(EXTRA_PROPERTY_VALUE, properties.getProperty(EXTRA_PROPERTY_KEY));
    }

    @Test
    void shouldModifyProperties() {
        writeBootstrapFile(C2_ENABLE.getKey() + "=" + TRUE);
        when(updatePropertiesPropertyProvider.getProperties()).thenReturn(Collections.singletonMap(AVAILABLE_PROPERTIES, Collections.singleton(new UpdatableProperty(C2_ENABLE.getKey(),
            TRUE, BOOLEAN_VALIDATOR))));

        propertiesPersister.persistProperties(Collections.singletonMap(C2_ENABLE.getKey(), FALSE));

        Properties properties = readUpdatedProperties();
        assertEquals(2, properties.stringPropertyNames().size());
        assertEquals(FALSE, properties.getProperty(C2_ENABLE.getKey()));
        assertEquals(EXTRA_PROPERTY_VALUE, properties.getProperty(EXTRA_PROPERTY_KEY));
    }

    private void writeBootstrapFile(String property) {
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(bootstrapConfigFileLocation))) {
            writer.write(property + System.lineSeparator());
            writer.write(EXTRA_PROPERTY_KEY + "=" + EXTRA_PROPERTY_VALUE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Properties readUpdatedProperties() {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(bootstrapNewConfigFileLocation)) {
            props.load(fis);
        } catch (Exception e) {
            fail("Failed to read bootstrap file");
        }
        return props;
    }
}