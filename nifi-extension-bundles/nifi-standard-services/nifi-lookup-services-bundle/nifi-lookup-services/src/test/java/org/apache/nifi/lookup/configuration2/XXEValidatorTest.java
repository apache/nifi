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
package org.apache.nifi.lookup.configuration2;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class XXEValidatorTest {
    private String simpleXMLFile = "src/test/resources/no_xxe.xml";
    private String remoteXXEFile = "src/test/resources/remote_xxe_file.xml";
    private String localXXEFile = "src/test/resources/local_xxe_file.xml";
    private String multilineXXEFile = "src/test/resources/multiline_xxe_file.xml";
    private String whitespaceXXEFile = "src/test/resources/whitespace_xxe_file.xml";
    private String configurationKey = "Configuration Name";
    private ValidationContext validationContext;

    @BeforeEach
    public void setUp() throws Exception {
        validationContext = mock(ValidationContext.class);
    }

    @Test
    public void testXmlFileWithNoXXEIsValid() {
        // Arrange
        final String parameterKey = configurationKey;
        final String parameterInput = simpleXMLFile;
        final XXEValidator a = new XXEValidator();

        // Act
        final ValidationResult val = a.validate(parameterKey, parameterInput, validationContext);

        //Assert
        assertTrue(val.isValid());
    }

    @Test
    public void testXmlFileWithRemoteXXEIsNotValid() {
        // Arrange
        final String parameterKey = configurationKey;
        final String parameterInput = remoteXXEFile;
        final XXEValidator a = new XXEValidator();

        // Act
        final ValidationResult val = a.validate(parameterKey, parameterInput, validationContext);

        //Assert
        assertFalse(val.isValid());
        assertEquals("XML file " + parameterInput + " contained an external entity. To prevent XXE vulnerabilities, NiFi has external entity processing disabled.", val.getExplanation());
    }

    @Test
    public void testXmlFileWithLocalXXEIsNotValid() {
        // Arrange
        final String parameterKey = configurationKey;
        final String parameterInput = localXXEFile;
        final XXEValidator a = new XXEValidator();

        // Act
        final ValidationResult val = a.validate(parameterKey, parameterInput, validationContext);

        //Assert
        assertFalse(val.isValid());
        assertEquals("XML file " + parameterInput + " contained an external entity. To prevent XXE vulnerabilities, NiFi has external entity processing disabled.", val.getExplanation());
    }

    @Test
    public void testXmlFileWithMultilineXXEIsInvalid() {
        // Arrange
        final String parameterKey = configurationKey;
        final String parameterInput = multilineXXEFile;
        final XXEValidator a = new XXEValidator();

        // Act
        final ValidationResult val = a.validate(parameterKey, parameterInput, validationContext);

        //Assert
        assertFalse(val.isValid());
        assertEquals("XML file " + parameterInput + " contained an external entity. To prevent XXE vulnerabilities, NiFi has external entity processing disabled.", val.getExplanation());
    }

    @Test
    public void testXmlFileWithXXEAndWhitespaceIsInvalid() {
        // Arrange
        final String parameterKey = configurationKey;
        final String parameterInput = whitespaceXXEFile;
        final XXEValidator a = new XXEValidator();

        // Act
        final ValidationResult val = a.validate(parameterKey, parameterInput, validationContext);

        //Assert
        assertFalse(val.isValid());
        assertEquals("XML file " + parameterInput + " contained an external entity. To prevent XXE vulnerabilities, NiFi has external entity processing disabled.", val.getExplanation());
    }

    @Test
    public void testMissingXmlFile() {
        // Arrange
        final String parameterKey = configurationKey;
        final String parameterInput = "missing_file.xml";
        final XXEValidator a = new XXEValidator();

        // Act
        final ValidationResult val = a.validate(parameterKey, parameterInput, validationContext);

        //Assert
        assertFalse(val.isValid());
        assertEquals("File not found: missing_file.xml could not be found.", val.getExplanation());
    }
}
