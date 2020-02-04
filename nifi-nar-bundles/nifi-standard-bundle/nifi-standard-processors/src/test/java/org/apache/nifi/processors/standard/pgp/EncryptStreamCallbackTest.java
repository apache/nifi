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
package org.apache.nifi.processors.standard.pgp;


import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;


public class EncryptStreamCallbackTest extends AbstractTestPGP {
    @Test
    public void combinedStreamEncryptAndDecryptReferenceTest() throws IOException, PGPException {
        runEncryptAndDecrypt(PGPKeyMaterialControllerServiceTest.onePublicKey, PGPKeyMaterialControllerServiceTest.onePrivateKey);
    }

    private static void runEncryptAndDecrypt(PGPPublicKey publicKey, PGPPrivateKey privateKey) throws IOException, PGPException {
        byte[] plain = Random.randomBytes(32 + Random.randomInt(4096));
        InputStream plainInput = new ByteArrayInputStream(plain);
        ByteArrayOutputStream cipherOutput = new ByteArrayOutputStream();
        EncryptStreamSession enc = new PublicKeyEncryptKeySession(null, publicKey, PGPEncryptedData.BLOWFISH, true);

        EncryptStreamCallback.encrypt(plainInput, cipherOutput, enc);
        byte[] ciphered = cipherOutput.toByteArray();
        InputStream cipherInput = new ByteArrayInputStream(cipherOutput.toByteArray());
        ByteArrayOutputStream plainOutput = new ByteArrayOutputStream();
        DecryptStreamSession dec = new PrivateKeyDecryptStreamSession(null, privateKey);

        DecryptStreamCallback.decrypt(cipherInput, plainOutput, dec);
        byte[] deciphered = plainOutput.toByteArray();

        Assert.assertNotEquals(plain.length, ciphered.length);
        Assert.assertNotEquals(Hex.encodeHexString(plain), Hex.encodeHexString(ciphered));
        Assert.assertEquals(plain.length, deciphered.length);
        Assert.assertEquals(Hex.encodeHexString(plain), Hex.encodeHexString(deciphered));
    }
}