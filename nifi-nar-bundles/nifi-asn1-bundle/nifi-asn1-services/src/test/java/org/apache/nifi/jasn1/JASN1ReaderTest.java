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

import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.nifi.jasn1.JASN1Reader.ASN_FILES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JASN1ReaderTest {
    private JASN1Reader testSubject;

    @Mock
    private ControllerServiceInitializationContext context;
    @Mock
    private ComponentLog logger;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        testSubject = new JASN1Reader();

        when(context.getLogger()).thenReturn(logger);

        testSubject.initialize(context);
    }

    @After
    public void tearDown() throws Exception {
        assertTrue(testSubject.asnOutDir.toFile().exists());

        testSubject.deleteAsnOutDir();

        assertTrue(!testSubject.asnOutDir.toFile().exists());
    }

    @Test
    public void testCanLoadClassCompiledFromAsn() throws Exception {
        // GIVEN
        ConfigurationContext context = mock(ConfigurationContext.class, RETURNS_DEEP_STUBS);
        when(context.getProperty(ASN_FILES).isSet()).thenReturn(true);
        when(context.getProperty(ASN_FILES).evaluateAttributeExpressions().getValue()).thenReturn("src/test/resources/test.asn");

        // WHEN
        testSubject.onEnabled(context);

        String actualRootModelName = testSubject.guessRootClassName("ORG-APACHE-NIFI-JASN1-TEST.RootType");
        Class<?> actual = testSubject.customClassLoader.loadClass(actualRootModelName);

        // THEN
        assertEquals("org.apache.nifi.jasn1.test.RootType", actualRootModelName);
        assertNotNull(actual);
    }
}
