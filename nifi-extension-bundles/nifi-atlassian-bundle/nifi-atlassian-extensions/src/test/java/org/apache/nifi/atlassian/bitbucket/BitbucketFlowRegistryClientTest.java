/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.nifi.atlassian.bitbucket;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BitbucketFlowRegistryClientTest {

    @Mock
    private ValidationContext validationContext;

    @ParameterizedTest
    @MethodSource("apiUrlArgs")
    void testApiUrl(String input, boolean valid) {
        when(validationContext.newPropertyValue(input)).thenReturn(new StandardPropertyValue(input, null, null));
        final ValidationResult validationResult = BitbucketFlowRegistryClient.BITBUCKET_API_URL.validate(input, validationContext);

        if (valid) {
            assertTrue(validationResult.isValid(), validationResult.getExplanation());
        } else {
            assertFalse(validationResult.isValid(), validationResult.getExplanation());
        }
    }

    private static Stream<Arguments> apiUrlArgs() {
        return Stream.of(
                Arguments.argumentSet("Valid URL", "https://bitbucket.example.com", true),
                Arguments.argumentSet("Invalid URL", "https:\\bitbucket", false),
                Arguments.argumentSet("Valid URL with port", "http://xxx.xxx.xxx.xxx:7990", true)
        );
    }

    @ParameterizedTest
    @MethodSource("apiHostArgs")
    void testApiHost(String input, boolean valid) {
        final ValidationResult validationResult = BitbucketFlowRegistryClient.BITBUCKET_API_URL.validate(input, validationContext);

        if (valid) {
            assertTrue(validationResult.isValid(), validationResult.getExplanation());
        } else {
            assertFalse(validationResult.isValid(), validationResult.getExplanation());
        }
    }

    private static Stream<Arguments> apiHostArgs() {
        return Stream.of(
                Arguments.argumentSet("Blank", "", false),
                Arguments.argumentSet("Valid Host", "api.bitbucket.org", true),
                Arguments.argumentSet("Invalid Host", "example:com", false)
        );
    }
}
