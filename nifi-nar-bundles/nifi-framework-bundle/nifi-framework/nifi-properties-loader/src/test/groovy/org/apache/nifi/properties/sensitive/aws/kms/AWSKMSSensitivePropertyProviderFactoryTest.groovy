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
package org.apache.nifi.properties.sensitive.aws.kms

import org.apache.nifi.properties.sensitive.SensitivePropertyProvider
import org.apache.nifi.properties.sensitive.SensitivePropertyProviderFactory
import org.apache.nifi.properties.sensitive.SensitivePropertyValueDescriptor
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory


@RunWith(JUnit4.class)
class AWSKMSSensitivePropertyProviderFactoryTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(AWSKMSSensitivePropertyProviderFactoryTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @Rule
    public ExpectedException expectException = ExpectedException.none()

    @Test
    void shouldThrowExceptionWithoutKey() throws Exception {
        SensitivePropertyValueDescriptor prop = SensitivePropertyValueDescriptor.fromValueAndScheme("", "")
        SensitivePropertyProviderFactory factory = new AWSKMSSensitivePropertyProviderFactory(prop)

        expectException.expect(org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException.class)
        expectException.expectMessage("The provider factory cannot generate providers without a key")
        factory.getProvider()
    }

    @Test
    void shouldThrowExceptionWithInvalidKey() throws Exception {
        SensitivePropertyValueDescriptor prop = SensitivePropertyValueDescriptor.fromValueAndScheme("", "not a key")
        SensitivePropertyProviderFactory factory = new AWSKMSSensitivePropertyProviderFactory(prop)

        expectException.expect(org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException.class)
        expectException.expectMessage("Invalid AWS KMS key")
        factory.getProvider()
    }
}
