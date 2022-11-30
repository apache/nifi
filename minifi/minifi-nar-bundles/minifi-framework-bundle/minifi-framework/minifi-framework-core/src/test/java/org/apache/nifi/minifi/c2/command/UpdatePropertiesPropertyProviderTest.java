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

import static org.apache.nifi.minifi.MiNiFiProperties.GRACEFUL_SHUTDOWN_SECOND;
import static org.apache.nifi.minifi.MiNiFiProperties.JAVA;
import static org.apache.nifi.minifi.MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE_PASSWD;
import static org.apache.nifi.minifi.MiNiFiProperties.PROPERTIES_BY_KEY;
import static org.apache.nifi.minifi.c2.command.UpdatePropertiesPropertyProvider.AVAILABLE_PROPERTIES;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.nifi.minifi.MiNiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UpdatePropertiesPropertyProviderTest {

    @TempDir
    private File tmpDir;

    private UpdatePropertiesPropertyProvider updatePropertiesPropertyProvider;
    private String bootstrapConfigFileLocation;

    @BeforeEach
    void setup() {
        bootstrapConfigFileLocation = tmpDir + "/bootstrap.conf";
        updatePropertiesPropertyProvider = new UpdatePropertiesPropertyProvider(bootstrapConfigFileLocation);
    }

    @Test
    void shouldReturnModifiableNonSensitivePropertiesWithValues() throws IOException {
        Properties props = new Properties();
        props.setProperty(JAVA.getKey(), "java");
        props.setProperty(GRACEFUL_SHUTDOWN_SECOND.getKey(), "20");
        props.setProperty(NIFI_MINIFI_SECURITY_KEYSTORE_PASSWD.getKey(), "truststore");

        try (FileOutputStream fos = new FileOutputStream(bootstrapConfigFileLocation)) {
            props.store(fos, null);
        }

        Map<String, Object> result = updatePropertiesPropertyProvider.getProperties();

        LinkedHashSet<UpdatableProperty> expected = getUpdatableProperties(props);

        assertEquals(Collections.singletonMap(AVAILABLE_PROPERTIES, expected), result);
    }

    @Test
    void shouldGetReturnListWithEmptyValuesInCaseOfFileNotFoundException() {
        Map<String, Object> properties = updatePropertiesPropertyProvider.getProperties();

        assertEquals(Collections.singletonMap(AVAILABLE_PROPERTIES, getUpdatableProperties(new Properties())), properties);
    }

    private static LinkedHashSet<UpdatableProperty> getUpdatableProperties(Properties props) {
        return PROPERTIES_BY_KEY.values()
            .stream()
            .filter(property -> !property.isSensitive())
            .filter(MiNiFiProperties::isModifiable)
            .map(property -> new UpdatableProperty(property.getKey(), (String) props.get(property.getKey()), property.getValidator().name()))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}