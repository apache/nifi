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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureGenerator;
import org.bouncycastle.openpgp.operator.PGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.bc.BcPGPContentSignerBuilder;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class encapsulates a sign operation over a pair of input and output streams.
 *
 */
class SignStreamCallback implements ExtendedStreamCallback {
    private final SignStreamSession options;
    public static final int KEY_ALGORITHM = 1;

    SignStreamCallback(SignStreamSession options) {
        this.options = options;
    }

    /**
     * Use the specified private key and hash algorithm to generate a signature of an input stream.
     *
     * @param input the input stream of data to sign
     * @param output the output stream of data piped from the input stream
     * @param signature the output stream of the signature
     * @throws IOException when input cannot be read, output cannot be written, etc.
     * @throws PGPException when the input cannot be read as PGP data
     */
    static void sign(InputStream input, OutputStream output, OutputStream signature, SignStreamSession options) throws IOException, PGPException {
        if (options == null || options.privateKey == null) {
            throw new IOException("Sign operation invalid without Private Key");
        }

        PGPContentSignerBuilder builder = new BcPGPContentSignerBuilder(KEY_ALGORITHM, options.signHashAlgorithm);
        PGPSignatureGenerator generator = new PGPSignatureGenerator(builder);
        generator.init(PGPSignature.BINARY_DOCUMENT, options.privateKey);
        copyAndUpdate(input, output, generator);
        generator.generate().encode(signature);
    }

    /**
     * Provides a managed output stream for use. The input stream is
     * automatically opened and closed though it is ok to close the stream
     * manually - and quite important if any streams wrapping these streams open
     * resources which should be cleared.
     *
     * @param in  the stream to read bytes from
     * @param out the stream to write bytes to
     * @throws IOException if issues occur reading or writing the underlying streams
     */
    @Override
    public void process(InputStream in, OutputStream out) throws IOException {
        ByteArrayOutputStream sig = new ByteArrayOutputStream();
        try {
            sign(in, out, sig, options);
            options.signature = sig.toByteArray();
        } catch (PGPException | IOException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public void postProcess(ProcessSession session, FlowFile flowFile) {
        session.putAttribute(options.flowFile, options.attribute, Hex.encodeHexString(options.signature));
    }

    // this is similar to the method in VerifyStreamCallback but the types aren't compatible
    private static void copyAndUpdate(InputStream input, OutputStream output, PGPSignatureGenerator signature) throws IOException {
        int i;
        while ((i = input.read()) >= 0) {
            signature.update((byte) i);
            output.write(i);
        }
    }
}
