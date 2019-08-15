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

import org.apache.nifi.security.util.CipherUtils;
import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractSensitivePropertyProviderTest {

    @Test
    public void showTheAbstractClassWorksLikeYouLike() {
        System.out.println("base class works like you like");
    }

    /**
     * This routine provides tests of behavior common to all SensitivePropertyProviders, specifically that an SPP implementation can
     * cipher and decipher text, and that it throws an exception when trying to cipher an empty string.
     */
    protected void checkProvider(SensitivePropertyProvider sensitivePropertyProvider, int plainSize) {
        String plainText = CipherUtils.getRandomHex(plainSize);
        String cipherText = sensitivePropertyProvider.protect(plainText);

        Assert.assertNotNull(cipherText);
        Assert.assertNotEquals(plainText, cipherText);

        String unwrappedText = sensitivePropertyProvider.unprotect(cipherText);
        Assert.assertNotNull(unwrappedText);
        Assert.assertEquals(unwrappedText, plainText);

        boolean failed = false;
        try  {
            sensitivePropertyProvider.protect("");
        } catch (final IllegalArgumentException ignored) {
            failed = true;
        }
        Assert.assertTrue(failed);
    }


}
