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
package org.apache.nifi.pgp.controllerservices;

import org.apache.nifi.controller.ControllerService;
import org.bouncycastle.openpgp.PGPException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * This interface defines the PGP operations we want exposed to the client code (the PGP processors).
 *
 */
public interface PGPService extends ControllerService {
    /**
     * Read from an {@link InputStream} and write an encrypted representation to an {@link OutputStream}.
     *
     * @param input {@link InputStream} of plain data
     * @param output {@link OutputStream} to receive encrypted data
     * @param options {@link EncryptOptions} used to configure encryption operation
     */
    void encrypt(InputStream input, OutputStream output, EncryptOptions options) throws IOException, PGPException;


    /**
     * Read from an encrypted {@link InputStream} and write a decrypted representation to an {@link OutputStream}.
     *
     * @param input {@link InputStream} of encrypted data
     * @param output {@link OutputStream} to receive decrypted data
     * @param options {@link DecryptOptions} used to configure decryption operation
     */
    void decrypt(InputStream input, OutputStream output, DecryptOptions options) throws IOException;


    /**
     * Read from an {@link InputStream} to generate a signature written to an {@link OutputStream}.
     *
     * @param input {@link InputStream} of clear or cipher data
     * @param signature {@link OutputStream} to receive signature
     * @param options {@link SignOptions}  used to configure sign operation
     */
    void sign(InputStream input, OutputStream signature, SignOptions options) throws IOException, PGPException;


    /**
     * Read from an {@link InputStream} to verify its signature.
     *
     * @param input {@link InputStream} of clear or cipher data
     * @param signature {@link InputStream} of signature data
     * @param options {@link VerifyOptions}  used to configure verify operation
     * @return true if the signature matches
     */
    boolean verify(InputStream input, InputStream signature, VerifyOptions options) throws IOException, PGPException;


    /**
     * Generate an {@link EncryptOptions} instance for an encrypt operation.
     *
     * @param algorithm encryption algorithm identifier, see {@link org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags}
     * @param armor if true, encrypted data will be PGP-encoded ascii
     * @return {@link EncryptOptions} instance
     */
    EncryptOptions optionsForEncrypt(int algorithm, boolean armor);


    /**
     * Generate a {@link DecryptOptions} instance for a decrypt operation.
     *
     * @return {@link DecryptOptions} instance
     */
    DecryptOptions optionsForDecrypt();


    /**
     * Generate a {@link SignOptions} instance for a sign operation.
     *
     * @param algorithm signature algorithm identifier, see {@link org.bouncycastle.bcpg.HashAlgorithmTags}
     * @return {@link SignOptions} instance
     */
    SignOptions optionsForSign(int algorithm);


    /**
     * Generate a {@link VerifyOptions} instance for a verify operation.
     *
     * @return {@link VerifyOptions} instance
     */
    VerifyOptions optionsForVerify();
}
