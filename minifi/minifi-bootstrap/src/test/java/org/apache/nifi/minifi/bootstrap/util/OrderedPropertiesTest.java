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

package org.apache.nifi.minifi.bootstrap.util;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.apache.nifi.minifi.bootstrap.util.ConfigTransformer.PROPERTIES_FILE_APACHE_2_0_LICENSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OrderedPropertiesTest {
    @Test
    public void testOrderedProperties() throws IOException {
        OrderedProperties orderedProperties = new OrderedProperties();
        orderedProperties.setProperty("prop1", "origVal1");
        orderedProperties.setProperty("prop2", "val2", "#this is property 2");
        orderedProperties.setProperty("prop3", "val3");
        orderedProperties.setProperty("prop4", "val4");
        orderedProperties.setProperty("prop5", "val5", "#this is property 5");
        orderedProperties.setProperty("prop3", "newVal3");
        orderedProperties.setProperty("prop1", "newVal1");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        orderedProperties.store(byteArrayOutputStream, PROPERTIES_FILE_APACHE_2_0_LICENSE);

        try (BufferedReader actualReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())));
             BufferedReader expectedReader = new BufferedReader(new InputStreamReader(OrderedPropertiesTest.class.getClassLoader().getResourceAsStream("orderedPropertiesExpected.properties")))) {
            String expectedLine;
            while((expectedLine = expectedReader.readLine()) != null) {
                String actualLine = actualReader.readLine();
                if (!"#Tue Feb 21 11:03:08 EST 2017".equals(expectedLine)) {
                    assertEquals(expectedLine, actualLine);
                } else {
                    System.out.println("Skipping timestamp comment line");
                }
            }
            assertNull(actualReader.readLine());
        }
    }
}
