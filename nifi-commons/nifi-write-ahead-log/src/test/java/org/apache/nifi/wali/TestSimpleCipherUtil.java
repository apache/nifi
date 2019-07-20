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
package org.apache.nifi.wali;

import org.bouncycastle.crypto.modes.AEADBlockCipher;
import org.junit.Assert;
import org.junit.Test;


public class TestSimpleCipherUtil extends TestAbstractSimpleCipher {
    private AEADBlockCipher cipher = null;

    @Test
    public void testStaticConstants() {
        // This shows the various class values are sane:
        Assert.assertNotNull(SimpleCipherUtil.random);
        Assert.assertTrue(SimpleCipherUtil.IV_BYTE_LEN >= 12);
        Assert.assertTrue(SimpleCipherUtil.AAD_BYTE_LEN >= 32);
        Assert.assertTrue(SimpleCipherUtil.MAC_BIT_LEN >= 128);
        Assert.assertTrue(SimpleCipherUtil.MARKER_BYTE != 0);
    }

    @Test
    public void testCreateCipherWithAllParams() {
        // This shows we can create a cipher for encryption:
        cipher = SimpleCipherUtil.initCipher(cipherKey, true, SimpleCipherUtil.createIV(), SimpleCipherUtil.createAAD());
        Assert.assertNotNull(cipher);

        // This shows we can create a cipher for decryption:
        cipher = SimpleCipherUtil.initCipher(cipherKey, false, SimpleCipherUtil.createIV(), SimpleCipherUtil.createAAD());
        Assert.assertNotNull(cipher);
    }

    @Test
    public void testCreateCipherWithoutKey() {
        // This shows we cannot create a cipher without a key:
        cipher = SimpleCipherUtil.initCipher(null, true, SimpleCipherUtil.createIV(), SimpleCipherUtil.createAAD());
        Assert.assertNull(cipher);

        cipher = SimpleCipherUtil.initCipher(null, false, SimpleCipherUtil.createIV(), SimpleCipherUtil.createAAD());
        Assert.assertNull(cipher);
    }

    @Test
    public void testCreateCipherWithoutIV() {
        // This shows we cannot create a cipher without an iv:
        cipher = SimpleCipherUtil.initCipher(cipherKey, true, null, SimpleCipherUtil.createAAD());
        Assert.assertNull(cipher);

        cipher = SimpleCipherUtil.initCipher(cipherKey, false, null, SimpleCipherUtil.createAAD());
        Assert.assertNull(cipher);
    }

    @Test
    public void testCreateCipherWithoutAAD() {
        // This shows we cannot create a cipher without aad bytes:
        cipher = SimpleCipherUtil.initCipher(cipherKey, true, SimpleCipherUtil.createIV(), null);
        Assert.assertNull(cipher);

        cipher = SimpleCipherUtil.initCipher(cipherKey, false, SimpleCipherUtil.createIV(), null);
        Assert.assertNull(cipher);
    }

    @Test
    public void testCreateIVCommonCase() {
        byte [] iv = SimpleCipherUtil.createIV();

        // This shows the IV is made as expected.
        Assert.assertNotNull(iv);
        Assert.assertEquals(SimpleCipherUtil.IV_BYTE_LEN, iv.length);
    }

    @Test
    public void testCreateAADCommonCase() {
        byte[] aad = SimpleCipherUtil.createAAD();

        // This shows the AAD byte vector is made as expected.
        Assert.assertNotNull(aad);
        Assert.assertEquals(SimpleCipherUtil.AAD_BYTE_LEN, aad.length);
    }

    @Test
    public void testCreateRandomBytes() {
        int[] lengths = new int[]{0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181};

        for (int length : lengths) {
            // This shows we can construct arrays of random (but known) length
            byte[] some = SimpleCipherUtil.randomBytes(length);
            byte[] more = SimpleCipherUtil.randomBytes(length);
            Assert.assertEquals(length, some.length);
            Assert.assertEquals(length, more.length);

            // This shows that two different arrays are in fact different
            Assert.assertNotSame(some, more);
            Assert.assertNotEquals(some, more);

            // This shows the two arrays have (mostly) different content
            if (length > 8) {
                int same = 0;
                for (int i = 0; i < some.length; i++) {
                    same += (some[i] == more[i]) ? 1 : 0;
                }
                Assert.assertTrue(same < length/10);
            }
        }
    }
}