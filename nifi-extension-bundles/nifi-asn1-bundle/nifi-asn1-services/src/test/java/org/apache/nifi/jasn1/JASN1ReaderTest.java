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
package org.apache.nifi.jasn1;

import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.nifi.jasn1.JASN1Reader.ASN_FILES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JASN1ReaderTest {
    private JASN1Reader testSubject;

    @Mock
    private ControllerServiceInitializationContext context;
    @Mock
    private ComponentLog logger;
    private AutoCloseable mockCloseable;

    @BeforeEach
    public void setUp() {
        mockCloseable = MockitoAnnotations.openMocks(this);
        testSubject = new JASN1Reader();
        when(context.getLogger()).thenReturn(logger);
        testSubject.initialize(context);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (mockCloseable != null) {
            mockCloseable.close();
        }

        assertTrue(testSubject.asnOutDir.toFile().exists());
        testSubject.deleteAsnOutDir();
        assertFalse(testSubject.asnOutDir.toFile().exists());
    }

    @DisabledOnOs({ OS.WINDOWS })
    @Test
    public void testCanLoadClassCompiledFromAsn() throws Exception {
        ConfigurationContext context = mock(ConfigurationContext.class, RETURNS_DEEP_STUBS);
        when(context.getProperty(ASN_FILES).isSet()).thenReturn(true);
        when(context.getProperty(ASN_FILES).evaluateAttributeExpressions().getValue()).thenReturn(Paths.get("src", "test", "resources", "test.asn").toString());

        testSubject.onEnabled(context);

        String actualRootModelName = testSubject.guessRootClassName("ORG-APACHE-NIFI-JASN1-TEST.RootType");
        Class<?> actual = testSubject.customClassLoader.loadClass(actualRootModelName);

        assertEquals("org.apache.nifi.jasn1.test.RootType", actualRootModelName);
        assertNotNull(actual);
    }

    @Test
    public void testAsnFileDoesntExist() {
        ConfigurationContext context = mock(ConfigurationContext.class, RETURNS_DEEP_STUBS);
        when(context.getProperty(ASN_FILES).isSet()).thenReturn(true);
        when(context.getProperty(ASN_FILES).evaluateAttributeExpressions().getValue()).thenReturn(
                new StringJoiner(",")
                        .add(Paths.get("src", "test", "resources", "test.asn").toString())
                        .add(Paths.get("src", "test", "resources", "doesnt_exist.asn").toString())
                        .toString()
        );

        ProcessException processException = assertThrows(
                ProcessException.class,
                () -> testSubject.onEnabled(context)
        );
        Throwable cause = processException.getCause();

        assertEquals(FileNotFoundException.class, cause.getClass());
        assertTrue(cause.getMessage().contains("doesnt_exist.asn"));
    }

    @Test
    /*
     * Checks reported messages of underlying libraries that are explained in additionalDetails.html.
     * In case of changes to this test additionalDetails.html may need to be updated as well.
     */
    public void testCantParseAsn() {
        String asnFile = Paths.get("src", "test", "resources", "cant_parse.asn").toString();

        List<String> expectedErrorMessages = Arrays.asList(
                "line 11:5: unexpected token: field3",
                "line 17:33: unexpected token: ["
        );

        testParseError(asnFile, expectedErrorMessages);
    }

    @DisabledOnOs({ OS.WINDOWS })
    @Test
    /*
     * Checks reported messages of underlying libraries that are explained in additionalDetails.html.
     * In case of changes to this test additionalDetails.html may need to be updated as well.
     */
    public void testCantCompileAsn() {
        String asnFiles = Paths.get("src", "test", "resources", "cant_compile.asn").toString();

        List<String> expectedErrorMessages = Arrays.asList(
                ".*com\\.beanit\\.asn1bean\\.ber\\.types\\.BerInteger.*com\\.beanit\\.asn1bean\\.ber\\.BerLength.*",
                ".*boolean.*java\\.io\\.OutputStream.*",
                ".*-Xdiags:verbose.*"
        );

        testCompileError(asnFiles, expectedErrorMessages);
    }

    @EnabledOnOs({ OS.MAC })
    @Test
    /*
     * Checks reported messages of underlying libraries that are explained in additionalDetails.html.
     * In case of changes to this test additionalDetails.html may need to be updated as well.
     */
    public void testCantCompileAsnOnMac() {
        String asnFiles = Paths.get("src", "test", "resources", "cant_compile_mac_windows.asn").toString();

        List<String> expectedErrorMessages = Collections.singletonList(
                ".*SAMENAMEWithDifferentCase.*SAMENAMEWithDifferentCase.*"
        );

        testCompileError(asnFiles, expectedErrorMessages);
    }

    private void testParseError(String asnFile, List<String> expectedErrorMessages) {
        ConfigurationContext context = mock(ConfigurationContext.class, RETURNS_DEEP_STUBS);
        when(context.getProperty(ASN_FILES).isSet()).thenReturn(true);
        when(context.getProperty(ASN_FILES).evaluateAttributeExpressions().getValue())
                .thenReturn(asnFile);


        assertThrows(
                ProcessException.class,
                () -> testSubject.onEnabled(context)
        );

        ArgumentCaptor<String> errorCaptor = ArgumentCaptor.forClass(String.class);
        verify(testSubject.logger, atLeastOnce()).error(eq("{} - {}"), anyString(), errorCaptor.capture());

        List<String> actualErrorMessages = errorCaptor.getAllValues();

        assertEquals(expectedErrorMessages, actualErrorMessages);
    }

    private void testCompileError(String asnFiles, List<String> expectedErrorMessages) {
        ConfigurationContext context = mock(ConfigurationContext.class, RETURNS_DEEP_STUBS);
        when(context.getProperty(ASN_FILES).isSet()).thenReturn(true);
        when(context.getProperty(ASN_FILES).evaluateAttributeExpressions().getValue())
                .thenReturn(asnFiles);

        assertThrows(
                ProcessException.class,
                () -> testSubject.onEnabled(context)
        );

        ArgumentCaptor<String> errorCaptor = ArgumentCaptor.forClass(String.class);
        verify(testSubject.logger, atLeastOnce()).error(errorCaptor.capture());

        List<String> actualErrorMessages = errorCaptor.getAllValues();
        assertEquals(expectedErrorMessages.size(), actualErrorMessages.size());

        for (int errorMessageIndex = 0; errorMessageIndex < actualErrorMessages.size(); errorMessageIndex++) {
            String expectedErrorMessage = expectedErrorMessages.get(errorMessageIndex);
            String actualErrorMessage = actualErrorMessages.get(errorMessageIndex);
            assertTrue(actualErrorMessage.matches(expectedErrorMessage), "Expected string matching '" + expectedErrorMessage + "', got '" + actualErrorMessage + "'");
        }
    }
}
