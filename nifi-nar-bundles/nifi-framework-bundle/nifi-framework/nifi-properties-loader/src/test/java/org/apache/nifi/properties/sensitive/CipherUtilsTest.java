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

import org.junit.Assert;
import org.junit.Test;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;


public class CipherUtilsTest {

    // This test shows we get an IV of the correct length when we ask for one.
    @Test
    public void showIvLengthIsCorrect() {
        byte[] iv = CipherUtils.generateIV();
        Assert.assertNotNull(iv);
        Assert.assertEquals(CipherUtils.IV_LENGTH, iv.length);
    }

    // This test shows we get new IVs each time we ask for one.
    @Test
    public void showIvsAreDifferentEachTime() {
        // Create a bunch of IVs
        int count = 4;
        byte [][] ivs = new byte[count][];
        for (int i = 0; i < count; i++) {
            ivs[i] = CipherUtils.generateIV();
        }

        // NB: It's not important for these tests (ad-hoc, non-production), but the code below is possibly susceptible to a timing attack.
        // See https://codahale.com/a-lesson-in-timing-attacks/

        // Compare each IV to every other IV
        for (int i = 0; i < count; i++) {
            byte[] iv = ivs[i];

            for (int j = 0; j < count; j++) {
                if (i == j) continue;
                byte[] other = ivs[j];
                boolean differ = false;
                for (int k = 0; k < other.length; k++) {
                    if (other[k] != iv[k]) {
                        differ = true;
                    }
                }
                Assert.assertTrue(differ);
            }
        }
    }

    // This test shows that we get an IV full of zeros when we ask for one.
    @Test
    public void showZeroIvValuesAreCorrect() {
        byte[] iv = CipherUtils.zeroIV();
        Assert.assertNotNull(iv);
        Assert.assertEquals(CipherUtils.IV_LENGTH, iv.length);

        for (byte b : iv) {
            Assert.assertEquals(b, 0);
        }

        byte[] other = CipherUtils.zeroIV();
        Assert.assertArrayEquals(iv, other);

        other[3] = 3;
        Assert.assertEquals(other[3], 3);
        Assert.assertEquals(iv[3], 0);
    }

    // This test shows that we get a cipher when we ask for one.
    @Test
    public void showBlockCipherCreatesCiphers() throws Exception {
        try {
            Cipher cipher = CipherUtils.blockCipher();
            Assert.assertNotNull(cipher);
        } catch (NoSuchPaddingException | NoSuchAlgorithmException | NoSuchProviderException e) {
            throw new Exception(e);
        }
    }

    // This test shows that we get random hex strings when we ask for them.
    @Test
    public void showRandomHexBehavesAsExpected() {
        int[] goodSizes = new int[]{0, 1, 2, 10};
        for (int goodSize : goodSizes) {
            String hex = CipherUtils.getRandomHex(goodSize);
            Assert.assertEquals(goodSize*2, hex.length());
        }

        int[] badSizes = new int[]{-1, Integer.MAX_VALUE};
        for (int badSize : badSizes) {
            boolean failed = false;
            try {
                String hex = CipherUtils.getRandomHex(badSize);
            } catch (final IllegalArgumentException | OutOfMemoryError e) {
                failed = true;
            }
            Assert.assertTrue(failed);
        }
    }

    // This test shows we can generate random integers as expected.
    @Test
    public void showRandomIntBehavesAsExpected() {
        int[] goodUpperBounds = new int[]{1, 2, 10};
        for (int goodUpperBound : goodUpperBounds) {
            int value = CipherUtils.getRandomInt(0, goodUpperBound);
            Assert.assertTrue(value >= 0);
            Assert.assertTrue(value <= goodUpperBound);
        }

        int[] badUpperBounds = new int[]{0, -1, -2};
        for (int badUpperBound : badUpperBounds) {
            boolean failed = false;
            try {
                int value = CipherUtils.getRandomInt(0, badUpperBound);
            } catch (final IllegalArgumentException ignored) {
                failed = true;
            }
            Assert.assertTrue(failed);
        }
   }
}