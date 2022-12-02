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
package org.apache.nifi.serde;

import org.apache.nifi.properties.MutableApplicationProperties;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StandardPropertiesWriterTest {

    @Test
    void testWriteProperties() throws IOException {
        final String AFTER_VALUE = "afterValue";
        final String keyOne = "nifi.sensitive.property.one";
        final String protectedKeyOne = "nifi.sensitive.property.one.protected";
        final String keyTwo = "nifi.sensitive.property.two";
        final String protectedKeyTwo = "nifi.sensitive.property.two.protected";

        final Properties props = new Properties();
        final InputStream input = StandardPropertiesWriterTest.class.getClassLoader().getResourceAsStream("testPropertiesFile");
        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        props.setProperty(keyOne, AFTER_VALUE);
        props.setProperty(protectedKeyOne, AFTER_VALUE);
        props.setProperty(keyTwo, AFTER_VALUE);
        props.setProperty(protectedKeyTwo, AFTER_VALUE);
        MutableApplicationProperties applicationProperties = new MutableApplicationProperties(props);

        new StandardPropertiesWriter().writePropertiesFile(input, output, new MutableApplicationProperties(props));
        Scanner reader = new Scanner(output.toString());
        while (reader.hasNextLine()) {
            String[] pair = reader.nextLine().split("=");
            assertEquals(props.getProperty(pair[0]), pair[1]);
        }
    }
}