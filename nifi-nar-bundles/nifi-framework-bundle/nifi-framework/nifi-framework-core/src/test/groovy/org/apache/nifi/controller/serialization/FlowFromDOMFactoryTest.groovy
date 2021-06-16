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
package org.apache.nifi.controller.serialization

import org.apache.nifi.encrypt.EncryptionException
import org.apache.nifi.encrypt.PropertyEncryptor
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class FlowFromDOMFactoryTest {
    private static final Logger logger = LoggerFactory.getLogger(FlowFromDOMFactoryTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Test
    void testShouldDecryptSensitiveFlowValue() throws Exception {
        // Arrange
        final String property = "property"
        String wrappedProperty = "enc{${property}}"

        PropertyEncryptor flowEncryptor = createEncryptor()

        // Act
        String recovered = FlowFromDOMFactory.decrypt(wrappedProperty, flowEncryptor)
        logger.info("Recovered: ${recovered}")

        // Assert
        assert property == recovered
    }

    @Test
    void testShouldProvideBetterErrorMessageOnDecryptionFailure() throws Exception {
        // Arrange
        final String property = "property"
        String wrappedProperty = "enc{${property}}"

        PropertyEncryptor flowEncryptor = createExceptionEncryptor()

        // Act
        def msg = shouldFail(EncryptionException) {
            String recovered = FlowFromDOMFactory.decrypt(wrappedProperty, flowEncryptor)
            logger.info("Recovered: ${recovered}")
        }
        logger.expected(msg)

        // Assert
        assert msg.message =~ "Check that the nifi.sensitive.props.key value in nifi.properties matches the value used to encrypt the flow.xml.gz file"
    }

    private PropertyEncryptor createEncryptor() {
        return new PropertyEncryptor() {
            @Override
            String encrypt(String property) {
                return property;
            }

            @Override
            String decrypt(String encryptedProperty) {
                return encryptedProperty;
            }
        };
    }

    private PropertyEncryptor createExceptionEncryptor() {
        return new PropertyEncryptor() {
            @Override
            String encrypt(String property) {
                return property;
            }

            @Override
            String decrypt(String encryptedProperty) {
                throw new EncryptionException("Failed")
            }
        };
    }
}
