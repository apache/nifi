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
package org.apache.nifi.security.pgp;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Random;


public class StandardPGPOperatorTest {
    private static Random random = new Random();
    private static String publicKey;
    private static String secretKey;

    final StandardPGPOperator operator = new StandardPGPOperator();

    @BeforeClass
    public static void setupServiceControllerTestClass() throws IOException {
        publicKey = keyResource("/pgp/public-key.asc");
        secretKey = keyResource("/pgp/secret-key.asc");

        Assert.assertNotNull(publicKey);
        Assert.assertNotNull(secretKey);

        Assert.assertTrue(publicKey.length() > 500);
        Assert.assertTrue(secretKey.length() > 1000);
    }

    @Test
    public void testOperatorCache() {
        // Note that this test exercises the public key cache and not the private key cache because both instances
        // have the same type.
        PGPPublicKeys publicKeys;

        // This shows the cache is empty for a new instance:
        Assert.assertEquals(0, operator.publicKeyCache.hits());
        Assert.assertEquals(0, operator.publicKeyCache.misses());

        // This shows that we still haven't used the cache when first reading a key or keyring:
        publicKeys = operator.readPublicKeys(publicKey);
        Assert.assertNotNull(publicKeys);
        Assert.assertEquals(0, operator.publicKeyCache.hits());
        Assert.assertEquals(1, operator.publicKeyCache.misses());

        // Now that the cache has one item, it's possible to show that the cache is hit each time a key is requested:
        int count = 20 + random.nextInt(100);
        for (int i = 1; i < count; i++) {
            publicKeys = operator.readPublicKeys(publicKey);
            Assert.assertNotNull(publicKeys);
            Assert.assertEquals(i, operator.publicKeyCache.hits());
            Assert.assertEquals(1, operator.publicKeyCache.misses());
        }

        // This shows that flushing the cache resets the counters:
        operator.clearCache();
        Assert.assertEquals(0, operator.publicKeyCache.hits());
        Assert.assertEquals(0, operator.publicKeyCache.misses());

        // This shows that the cache misses after a reset:
        publicKeys = operator.readPublicKeys(publicKey);
        Assert.assertNotNull(publicKeys);
        Assert.assertEquals(0, operator.publicKeyCache.hits());
        Assert.assertEquals(1, operator.publicKeyCache.misses());
    }

    private static String keyResource(String name) throws IOException {
        final InputStream resource = StandardPGPOperatorTest.class.getResourceAsStream(name);
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        int i;

        while ((i = resource.read()) >= 0) {
            output.write(i);
        }
        resource.close();

        return new String(output.toByteArray(), Charset.defaultCharset());
    }

}