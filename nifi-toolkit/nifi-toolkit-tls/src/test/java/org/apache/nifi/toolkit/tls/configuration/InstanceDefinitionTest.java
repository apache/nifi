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

package org.apache.nifi.toolkit.tls.configuration;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InstanceDefinitionTest {

    @Test
    public void testCreateDefinitionKeyPassword() {
        testCreateDefinition("testHostname", 4, "keyStorePassword", "keyPassword", "trustStorePassword");
    }

    @Test
    public void testCreateDefinitionNoKeyPassword() {
        testCreateDefinition("testHostname", 5, "keyStorePassword", null, "trustStorePassword");
    }

    @Test
    public void testCreateDefinitionsSingleHostSingleName() {
        testCreateDefinitions(Arrays.asList("hostname"), Arrays.asList("hostname"), Arrays.asList(1), false);
    }

    @Test
    public void testCreateDefinitionsSingleHostnameOneNumberInParens() {
        testCreateDefinitions(Arrays.asList("hostname(20)"),
                IntStream.range(1, 21).mapToObj(operand -> "hostname").collect(Collectors.toList()),
                integerRange(1, 20).collect(Collectors.toList()), false);
    }

    @Test
    public void testCreateDefinitionsSingleHostnameTwoNumbersInParens() {
        testCreateDefinitions(Arrays.asList("hostname(5-20)"),
                IntStream.range(5, 21).mapToObj(operand -> "hostname").collect(Collectors.toList()),
                integerRange(5, 20).collect(Collectors.toList()), false);
    }

    @Test
    public void testCreateDefinitionsMultipleHostnamesWithMultipleNumbers() {
        testCreateDefinitions(Arrays.asList("host[10]name[02-5](20)"),
                integerRange(1, 10).flatMap(v -> integerRange(2, 5).flatMap(v2 -> integerRange(1, 20).map(v3 -> "host" + v + "name" + String.format("%02d", v2)))).collect(Collectors.toList()),
                integerRange(1, 10).flatMap(val -> integerRange(2, 5).flatMap(val2 -> integerRange(1, 20))).collect(Collectors.toList()), false);
    }

    @Test
    public void testCreateDefinitionsStream() {
        testCreateDefinitions(Arrays.asList("host", "name"), Arrays.asList("host", "name"), Arrays.asList(1, 1), true);
    }

    @Test
    public void testCreateDefinitionsStreamNonNullKeyPasswords() {
        testCreateDefinitions(Arrays.asList("host", "name"), Arrays.asList("host", "name"), Arrays.asList(1, 1), false);
    }

    private Stream<Integer> integerRange(int start, int endInclusive) {
        return IntStream.range(start, endInclusive + 1).mapToObj(value -> value);
    }

    private void testCreateDefinitions(List<String> hostnameExpressions, List<String> expectedHostnames, List<Integer> expectedNumbers, boolean nullForKeyPasswords) {
        List<String> keyStorePasswords = IntStream.range(0, expectedHostnames.size()).mapToObj(value -> "testKeyStorePassword" + value).collect(Collectors.toList());
        List<String> keyPasswords;
        if (nullForKeyPasswords) {
            keyPasswords = null;
        } else {
            keyPasswords = IntStream.range(0, expectedHostnames.size()).mapToObj(value -> "testKeyPassword" + value).collect(Collectors.toList());
        }
        List<String> trustStorePasswords = IntStream.range(0, expectedHostnames.size()).mapToObj(value -> "testTrustStorePassword" + value).collect(Collectors.toList());
        List<InstanceDefinition> instanceDefinitions = InstanceDefinition.createDefinitions(null, hostnameExpressions.stream(),
                mockSupplier(keyStorePasswords.toArray(new String[keyStorePasswords.size()])), keyPasswords == null ? null : mockSupplier(keyPasswords.toArray(new String[keyPasswords.size()])),
                mockSupplier(trustStorePasswords.toArray(new String[trustStorePasswords.size()])));
        testCreateDefinitionsOutput(instanceDefinitions, expectedHostnames, expectedNumbers, keyStorePasswords, keyPasswords, trustStorePasswords);
    }

    private void testCreateDefinitionsOutput(List<InstanceDefinition> instanceDefinitions, List<String> expectedHostnames, List<Integer> expectedNumbers, List<String> keyStorePasswords,
                                             List<String> keyPasswords, List<String> trustStorePasswords) {
        assertEquals(instanceDefinitions.size(), expectedHostnames.size());
        for (int i = 0; i < instanceDefinitions.size(); i++) {
            assertDefinitionEquals(instanceDefinitions.get(i), expectedHostnames.get(i), expectedNumbers.get(i), keyStorePasswords.get(i),
                    keyPasswords == null ? null : keyPasswords.get(i), trustStorePasswords.get(i));
        }
    }

    private void testCreateDefinition(String hostname, int num, String keyStorePassword, String keyPassword, String trustStorePassword) {
        InstanceDefinition definition = InstanceDefinition.createDefinition(new InstanceIdentifier(hostname, num), num, mockSupplier(keyStorePassword),
                keyPassword == null ? null : mockSupplier(keyPassword), mockSupplier(trustStorePassword));
        assertDefinitionEquals(definition, hostname, num, keyStorePassword, keyPassword, trustStorePassword);
    }

    private void assertDefinitionEquals(InstanceDefinition definition, String hostname, int num, String keyStorePassword, String keyPassword, String trustStorePassword) {
        assertEquals(hostname, definition.getHostname());
        assertEquals(num, definition.getNumber());
        assertEquals(keyStorePassword, definition.getKeyStorePassword());
        assertEquals(keyPassword == null ? keyStorePassword : keyPassword, definition.getKeyPassword());
        assertEquals(trustStorePassword, definition.getTrustStorePassword());
    }

    private <T> Supplier<T> mockSupplier(T... values) {
        Supplier<T> supplier = mock(Supplier.class);
        if (values.length == 1) {
            when(supplier.get()).thenReturn(values[0]);
        } else if (values.length > 1) {
            when(supplier.get()).thenReturn(values[0], Arrays.copyOfRange(values, 1, values.length));
        }
        return supplier;
    }
}
