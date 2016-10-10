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
package org.apache.nifi.fingerprint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class FingerprintFactoryTest {

    private FingerprintFactory fingerprinter;

    @Before
    public void setup() {
        fingerprinter = new FingerprintFactory(null);
    }

    @Test
    public void testSameFingerprint() throws IOException {
        final String fp1 = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1a.xml"), null);
        final String fp2 = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1b.xml"), null);
        System.out.println(fp1);
        System.out.println(fp2);
        assertEquals(fp1, fp2);
    }

    @Test
    public void testDifferentFingerprint() throws IOException {
        final String fp1 = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1a.xml"), null);
        final String fp2 = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow2.xml"), null);
        assertFalse(fp1.equals(fp2));
    }

    @Test
    public void testResourceValueInFingerprint() throws IOException {
        final String fingerprint = fingerprinter.createFingerprint(getResourceBytes("/nifi/fingerprint/flow1a.xml"), null);
        assertEquals(3, StringUtils.countMatches(fingerprint, "success"));
        assertTrue(fingerprint.contains("In Connection"));
    }

    private byte[] getResourceBytes(final String resource) throws IOException {
        return IOUtils.toByteArray(FingerprintFactoryTest.class.getResourceAsStream(resource));
    }

}
