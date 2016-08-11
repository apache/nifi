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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class InstanceIdentifierTest {

    @Test
    public void testExtractHostnamesSingle() {
        testExtractHostnames("test[1-3]", "test1", "test2", "test3");
    }

    @Test
    public void testExtractHostnamesPadding() {
        testExtractHostnames("test[0001-3]", "test0001", "test0002", "test0003");
    }

    @Test
    public void testExtractHostnamesLowGreaterThanHigh() {
        testExtractHostnames("test[3-1]");
    }

    @Test
    public void testExtractHostnamesLowEqualToHigh() {
        testExtractHostnames("test[3-3]", "test3");
    }

    @Test
    public void testExtractHostnamesSingleNumber() {
        testExtractHostnames("test[2]", "test1", "test2");
    }

    @Test
    public void testExtractHostnamesSingleNumberPadding() {
        testExtractHostnames("test[002]", "test001", "test002");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractHostnamesNoNumber() {
        testExtractHostnames("test[]", "test");
    }

    @Test
    public void testExtractHostnamesMultiple() {
        testExtractHostnames("test[1-3]name[1-3]", "test1name1", "test1name2", "test1name3", "test2name1", "test2name2", "test2name3", "test3name1", "test3name2", "test3name3");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractHostnamesUnmatched() {
        testExtractHostnames("test[");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractHostnamesSpace() {
        testExtractHostnames("test[ 1-2]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractHostnamesMultipleHyphens() {
        testExtractHostnames("test[1-2-3]");
    }

    @Test
    public void testCreateDefinitionsSingleHostSingleName() {
        testCreateIdentifiers(Arrays.asList("hostname"), Arrays.asList("hostname"), Arrays.asList(1));
    }

    @Test
    public void testCreateDefinitionsSingleHostnameOneNumberInParens() {
        testCreateIdentifiers(Arrays.asList("hostname(20)"),
                IntStream.range(1, 21).mapToObj(operand -> "hostname").collect(Collectors.toList()),
                integerRange(1, 20).collect(Collectors.toList()));
    }

    @Test
    public void testCreateDefinitionsSingleHostnameTwoNumbersInParens() {
        testCreateIdentifiers(Arrays.asList("hostname(5-20)"),
                IntStream.range(5, 21).mapToObj(operand -> "hostname").collect(Collectors.toList()),
                integerRange(5, 20).collect(Collectors.toList()));
    }

    @Test
    public void testCreateDefinitionsMultipleHostnamesWithMultipleNumbers() {
        testCreateIdentifiers(Arrays.asList("host[10]name[02-5](20)"),
                integerRange(1, 10).flatMap(v -> integerRange(2, 5).flatMap(v2 -> integerRange(1, 20).map(v3 -> "host" + v + "name" + String.format("%02d", v2)))).collect(Collectors.toList()),
                integerRange(1, 10).flatMap(val -> integerRange(2, 5).flatMap(val2 -> integerRange(1, 20))).collect(Collectors.toList()));
    }

    @Test
    public void testCreateDefinitionsStream() {
        testCreateIdentifiers(Arrays.asList("host", "name"), Arrays.asList("host", "name"), Arrays.asList(1, 1));
    }

    @Test
    public void testCreateOrderMap() {
        String abc123 = "abc[1-3]";
        String abc0123 = "abc[01-3]";
        String b = "b";

        Map<InstanceIdentifier, Integer> orderMap = InstanceIdentifier.createOrderMap(Stream.of(abc123, abc0123 + "(2)", b));

        AtomicInteger num = new AtomicInteger(1);
        Consumer<InstanceIdentifier> action = id -> {
            int i = num.getAndIncrement();
            assertEquals(i, orderMap.get(id).intValue());
        };

        InstanceIdentifier.extractHostnames(abc0123).flatMap(s -> Stream.of(new InstanceIdentifier(s, 1), new InstanceIdentifier(s, 2))).forEach(action);
        InstanceIdentifier.extractHostnames(abc123).map(s -> new InstanceIdentifier(s, 1)).forEach(action);
        InstanceIdentifier.extractHostnames(b).map(s -> new InstanceIdentifier(s, 1)).forEach(action);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateIdentifiersCharactersAfterNumber() {
        InstanceIdentifier.createIdentifiers(Stream.of("test(2)a")).count();
    }

    private Stream<Integer> integerRange(int start, int endInclusive) {
        return IntStream.range(start, endInclusive + 1).mapToObj(value -> value);
    }

    private void testExtractHostnames(String hostnameWithRange, String... expectedHostnames) {
        assertEquals(Stream.of(expectedHostnames).collect(Collectors.toList()), InstanceIdentifier.extractHostnames(hostnameWithRange).collect(Collectors.toList()));
    }

    private void testCreateIdentifiers(List<String> hostnameExpressions, List<String> expectedHostnames, List<Integer> expectedNumbers) {
        List<InstanceIdentifier> instanceIdentifiers = InstanceIdentifier.createIdentifiers(hostnameExpressions.stream()).collect(Collectors.toList());
        assertEquals(instanceIdentifiers.size(), expectedHostnames.size());
        for (int i = 0; i < instanceIdentifiers.size(); i++) {
            InstanceIdentifier identifier = instanceIdentifiers.get(i);
            assertEquals(expectedHostnames.get(i), identifier.getHostname());
            assertEquals((int) expectedNumbers.get(i), identifier.getNumber());
        }
    }
}
