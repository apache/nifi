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
package org.apache.nifi.properties.sensitive;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Properties;


public class StandardExternalPropertyLookupTest {
    static SecureRandom random = new SecureRandom();

    static String MISSING_KEY;
    static String DEFAULT_VALUE;
    static String KNOWN_FILE_KEY;
    static String KNOWN_FILE_VALUE;
    static String KNOWN_OVERRIDE;

    final static String KNOWN_ENV_KEY = "PATH";
    private static String testFilename;


    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    public static void testCreateTestPropsFile() throws IOException {
        MISSING_KEY = CipherUtils.getRandomHex(CipherUtils.getRandomInt(8, 24));
        DEFAULT_VALUE = CipherUtils.getRandomHex(CipherUtils.getRandomInt(12, 64));
        KNOWN_FILE_KEY = CipherUtils.getRandomHex(8);
        KNOWN_FILE_VALUE = CipherUtils.getRandomHex(32);
        KNOWN_OVERRIDE = CipherUtils.getRandomHex(24);

        File propsFile = testFolder.newFile();
        testFilename = propsFile.getAbsolutePath();

        Properties props = new Properties();
        props.setProperty(KNOWN_FILE_KEY, KNOWN_FILE_VALUE);

        int randomKeyCount = CipherUtils.getRandomInt(12, 32);
        for (int i = 0; i < randomKeyCount; i++) {
            props.setProperty(CipherUtils.getRandomHex(CipherUtils.getRandomInt(4, 8)), CipherUtils.getRandomHex(CipherUtils.getRandomInt(12, 32)));
        }
        props.store(new FileOutputStream(propsFile), "");
    }

    @After
    public void resetProps() {
        System.clearProperty(KNOWN_ENV_KEY);
    }


    @Test
    public void testBasicUsageNoFile() {
        StandardExternalPropertyLookup lookup = new StandardExternalPropertyLookup();

        // this shows that a missing value comes back as null or as the specified default
        Assert.assertNull(lookup.get(MISSING_KEY));
        Assert.assertEquals(lookup.get(MISSING_KEY, DEFAULT_VALUE), DEFAULT_VALUE);

        // this shows that a lookup of known values fails without a backing file:
        Assert.assertNull(lookup.get(KNOWN_FILE_KEY));
        Assert.assertEquals(lookup.get(KNOWN_FILE_KEY, DEFAULT_VALUE), DEFAULT_VALUE);

        // this shows that a well-known property is always found and not any specified default:
        String envPath = lookup.get(KNOWN_ENV_KEY);
        Assert.assertNotNull(envPath);
        Assert.assertNotEquals(lookup.get(KNOWN_ENV_KEY, DEFAULT_VALUE), DEFAULT_VALUE);

        // this shows that if we set a system property with a name also in the environment, the value
        // comes from the system property:
        System.setProperty(KNOWN_ENV_KEY, KNOWN_OVERRIDE);
        Assert.assertNotEquals(envPath, lookup.get(KNOWN_ENV_KEY));
        Assert.assertEquals(KNOWN_OVERRIDE, lookup.get(KNOWN_ENV_KEY));
    }


    @Test
    public void testBasicUsageWithFile() {
        StandardExternalPropertyLookup lookup = new StandardExternalPropertyLookup(testFilename);

        // this shows that a missing value comes back as null or as the specified default
        Assert.assertNull(lookup.get(MISSING_KEY));
        Assert.assertEquals(lookup.get(MISSING_KEY, DEFAULT_VALUE), DEFAULT_VALUE);

        // this shows that a lookup of known values succeeds with a backing file:
        Assert.assertNotNull(lookup.get(KNOWN_FILE_KEY));
        Assert.assertNotEquals(lookup.get(KNOWN_FILE_KEY, DEFAULT_VALUE), DEFAULT_VALUE);
        Assert.assertEquals(lookup.get(KNOWN_FILE_KEY, DEFAULT_VALUE), KNOWN_FILE_VALUE);

        // this shows that a well-known property is always found and not any specified default:
        String envPath = lookup.get(KNOWN_ENV_KEY);
        Assert.assertNotNull(envPath);
        Assert.assertNotEquals(lookup.get(KNOWN_ENV_KEY, DEFAULT_VALUE), DEFAULT_VALUE);

        // this shows that if we set a system property with a name also in the environment, the value
        // comes from the system property:
        System.setProperty(KNOWN_ENV_KEY, KNOWN_OVERRIDE);
        Assert.assertNotEquals(envPath, lookup.get(KNOWN_ENV_KEY));
        Assert.assertEquals(KNOWN_OVERRIDE, lookup.get(KNOWN_ENV_KEY));
    }
}
