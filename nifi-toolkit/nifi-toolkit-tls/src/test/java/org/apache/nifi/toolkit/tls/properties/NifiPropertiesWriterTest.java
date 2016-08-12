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

package org.apache.nifi.toolkit.tls.properties;

import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NifiPropertiesWriterTest {
    private static NiFiPropertiesWriterFactory nifiPropertiesWriterFactory;
    private NiFiPropertiesWriter niFiPropertiesWriter;

    @BeforeClass
    public static void beforeClass() throws IOException {
        nifiPropertiesWriterFactory = new NiFiPropertiesWriterFactory();
    }

    @Before
    public void setup() throws IOException {
        niFiPropertiesWriter = nifiPropertiesWriterFactory.create();
    }

    @Test
    public void testCanUpdateAllKeys() throws IllegalAccessException, IOException {
        String testValue = NifiPropertiesWriterTest.class.getCanonicalName();

        // Get all property keys from NiFiProperties and set them to testValue
        Set<String> propertyKeys = new HashSet<>();
        for (Field field : NiFiProperties.class.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers) && field.getType() == String.class) {
                if (!field.getName().toLowerCase().startsWith("default")) {
                    String fieldName = (String) field.get(null);
                    propertyKeys.add(fieldName);
                    niFiPropertiesWriter.setPropertyValue(fieldName, testValue);
                }
            }
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        niFiPropertiesWriter.writeNiFiProperties(outputStream);

        // Verify operation worked
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(outputStream.toByteArray()));
        for (String propertyKey : propertyKeys) {
            assertEquals(testValue, properties.getProperty(propertyKey));
        }
    }
}
