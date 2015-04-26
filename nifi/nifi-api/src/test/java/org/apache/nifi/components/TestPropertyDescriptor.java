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
package org.apache.nifi.components;

import static org.junit.Assert.assertNotNull;

import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Regression test for issue NIFI-49, to ensure that if a Processor's Property's
 * Default Value is not allowed, the Exception thrown should indicate what the
 * default value is
 */
public class TestPropertyDescriptor {

    private static Builder invalidDescriptorBuilder;
    private static Builder validDescriptorBuilder;
    private static String DEFAULT_VALUE = "Default Value";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        validDescriptorBuilder = new PropertyDescriptor.Builder().name("").allowableValues("Allowable Value", "Another Allowable Value").defaultValue("Allowable Value");
        invalidDescriptorBuilder = new PropertyDescriptor.Builder().name("").allowableValues("Allowable Value", "Another Allowable Value").defaultValue(DEFAULT_VALUE);
    }

    @Test
    public void testExceptionThrownByDescriptorWithInvalidDefaultValue() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("[" + DEFAULT_VALUE + "]");

        invalidDescriptorBuilder.build();
    }

    @Test
    public void testNoExceptionThrownByPropertyDescriptorWithValidDefaultValue() {
        assertNotNull(validDescriptorBuilder.build());
    }
}
