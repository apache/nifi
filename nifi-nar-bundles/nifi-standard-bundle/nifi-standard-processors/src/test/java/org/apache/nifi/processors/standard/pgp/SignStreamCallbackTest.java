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
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class SignStreamCallbackTest extends AbstractTestPGP {
    @Test
    public void combinedStreamSignAndVerifyReferenceTest() throws IOException, PGPException {
        runSignAndVerify(PGPKeyMaterialControllerServiceTest.onePublicKey, PGPKeyMaterialControllerServiceTest.onePrivateKey);
    }


    private static void runSignAndVerify(PGPPublicKey publicKey, PGPPrivateKey privateKey) throws IOException, PGPException {
        byte[] plain = Random.randomBytes(32 + Random.randomInt(4096));
        InputStream plainInput = new ByteArrayInputStream(plain);
        ByteArrayOutputStream sigOutput = new ByteArrayOutputStream();
        SignStreamSession options = new SignStreamSession(privateKey, PGPUtil.SHA256);
        OutputStream plainOut = new ByteArrayOutputStream();
        SignStreamCallback.sign(plainInput, plainOut, sigOutput, options);
        byte[] signature = sigOutput.toByteArray();
        VerifyStreamSession verifyOptions = new VerifyStreamSession(null, publicKey, new ByteArrayInputStream(signature));

        boolean verified = VerifyStreamCallback.verify(verifyOptions, new ByteArrayInputStream(plain), new ByteArrayOutputStream());
        Assert.assertNotEquals(Hex.encodeHexString(plain), Hex.encodeHexString(signature));
        Assert.assertTrue("Signature unverified: ", verified);
    }
}