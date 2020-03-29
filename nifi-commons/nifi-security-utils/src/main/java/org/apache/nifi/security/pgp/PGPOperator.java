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

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.bouncycastle.bcpg.HashAlgorithmTags;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The PGPOperator is responsible for defining and providing high-level PGP cryptographic operations.
 *
 */
public interface PGPOperator {
    /**
     * Read from an {@link InputStream} and write an encrypted representation to an {@link OutputStream}.
     *
     * @param input plain data
     * @param output receives encrypted data
     * @param options used to configure encryption operation
     */
    void encrypt(InputStream input, OutputStream output, EncryptOptions options) throws IOException, PGPException;

    /**
     * Read from an encrypted {@link InputStream} and write a decrypted representation to an {@link OutputStream}.
     *
     * @param input encrypted data
     * @param output receives decrypted data
     * @param options used to configure decryption operation
     */
    void decrypt(InputStream input, OutputStream output, DecryptOptions options) throws IOException;

    /**
     * Read from an {@link InputStream} to generate a signature written to an {@link OutputStream}.
     *
     * @param input clear or cipher data
     * @param signature receives signature
     * @param options used to configure sign operation
     */
    void sign(InputStream input, OutputStream signature, SignOptions options) throws IOException, PGPException;

    /**
     * Read from an {@link InputStream} to verify its signature.
     *
     * @param input clear or cipher data
     * @param signature signature data
     * @param options used to configure verify operation
     * @return true if the signature matches
     */
    boolean verify(InputStream input, InputStream signature, VerifyOptions options) throws IOException, PGPException;

    /**
     * Reads a public key from source(s) described in a property context.
     *
     * @param context properties describing public key material
     * @return public key or null if no public key can be read
     */
    PGPPublicKey getPublicKey(PropertyContext context);

    /**
     * Reads a private key from the source(s) described in a property context.
     *
     * @param context properties describing private key material
     * @return private key or null if no private key can be read
     */
    PGPPrivateKey getPrivateKey(PropertyContext context);

    /**
     * Reads a PBE-based passphrase from the sources described in a property context.
     *
     * @param context properties describing the PBE-based passphrase
     * @return passphrase or null if no passphrase is described by the properties
     */
    char[] getPBEPassphrase(PropertyContext context);

    /**
     * Reads a signature from a flow as described by a property context.
     *
     * @param context properties describing the signature
     * @param flow contains signature attribute to read
     * @return signature from flow, if any
     */
    InputStream getSignature(PropertyContext context, FlowFile flow) throws PGPException;

    /**
     * Generate an {@link EncryptOptions} instance for an encrypt operation.
     *
     * @param context context with encryption properties
     * @return options instance
     */
    EncryptOptions optionsForEncrypt(PropertyContext context);

    /**
     * Generate a {@link DecryptOptions} instance for a decrypt operation.
     *
     * @return {@link DecryptOptions} instance
     * @param context properties for deriving options
     */
    DecryptOptions optionsForDecrypt(PropertyContext context);

    /**
     * Generate a {@link SignOptions} instance for a sign operation.
     *
     * @param context signature algorithm identifier, see {@link HashAlgorithmTags}
     * @return options for operation
     */
    SignOptions optionsForSign(PropertyContext context);

    /**
     * Generate a {@link VerifyOptions} instance for a verify operation.
     *
     * @param context properties for deriving options
     * @return options for operation
     */
    VerifyOptions optionsForVerify(PropertyContext context);
}
