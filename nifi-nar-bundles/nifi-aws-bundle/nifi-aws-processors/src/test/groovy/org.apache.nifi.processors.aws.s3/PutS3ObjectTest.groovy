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
package org.apache.nifi.processors.aws.s3

import com.amazonaws.services.s3.model.ObjectMetadata
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class PutS3ObjectTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(PutS3ObjectTest.class);

    private static long mockFlowFileId = 0
    private PutS3Object putS3Object

    @BeforeClass
    static void setUpOnce() {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {
        super.setUp()

        putS3Object = new PutS3Object()

    }

    @After
    void tearDown() {

    }

    @Test
    void testShouldIncludeServerSideEncryptionAlgorithmProperty() {
        // Arrange

        // Act
        def propertyDescriptors = putS3Object.getSupportedPropertyDescriptors()
        def ssePropertyDescriptor = propertyDescriptors.find { it.name =~ "server-side-encryption" }

        // Assert
        assert ssePropertyDescriptor
        assert ssePropertyDescriptor.name == "server-side-encryption"
        assert ssePropertyDescriptor.displayName == "Server Side Encryption"
    }

    @Test
    void testShouldValidateServerSideEncryptionDefaultsToNone() {
        // Arrange

        // Act
        def propertyDescriptors = putS3Object.getSupportedPropertyDescriptors()
        def ssePropertyDescriptor = propertyDescriptors.find { it.name =~ "server-side-encryption" }

        // Assert
        assert ssePropertyDescriptor
        assert ssePropertyDescriptor.defaultValue == putS3Object.NO_SERVER_SIDE_ENCRYPTION
    }

    @Test
    void testShouldValidateServerSideEncryptionAllowableValues() {
        // Arrange

        // Act
        def propertyDescriptors = putS3Object.getSupportedPropertyDescriptors()
        def ssePropertyDescriptor = propertyDescriptors.find { it.name =~ "server-side-encryption" }

        // Assert
        assert ssePropertyDescriptor
        assert ssePropertyDescriptor.allowableValues*.toString() == [putS3Object.NO_SERVER_SIDE_ENCRYPTION, ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION]
    }
}
