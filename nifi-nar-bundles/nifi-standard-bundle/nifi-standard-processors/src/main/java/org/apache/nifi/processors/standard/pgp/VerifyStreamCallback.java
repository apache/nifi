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

import org.apache.nifi.processor.exception.ProcessException;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentVerifierBuilderProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class encapsulates a verify operation over a pair of input and output streams.
 *
 */
class VerifyStreamCallback implements ExtendedStreamCallback {
    private static final JcaPGPContentVerifierBuilderProvider contentVerifier = new JcaPGPContentVerifierBuilderProvider().setProvider("BC");
    private final VerifyStreamSession options;

    VerifyStreamCallback(VerifyStreamSession options) {
        this.options = options;
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
        try {
            if (!verify(options, in, out)) {
                throw new ProcessException("Could not verify signature with data.");
            }
        } catch (final PGPException e) {
            throw new ProcessException(e);
        }
    }

    /**
     * Use the specified public key to verify the signature of an input stream.
     *
     * @param input the input stream of data
     * @return true if the signature matches
     * @throws IOException when input cannot be read, output cannot be written, etc.
     * @throws PGPException when the input cannot be read as PGP data
     */
    static boolean verify(VerifyStreamSession session, InputStream input, OutputStream output) throws IOException, PGPException {
        JcaPGPObjectFactory signatureFactory = new JcaPGPObjectFactory(PGPUtil.getDecoderStream(session.signature));
        PGPSignatureList signatures;

        Object head = signatureFactory.nextObject();
        if (head instanceof PGPCompressedData) {
            PGPCompressedData compressed = (PGPCompressedData) head;
            signatureFactory = new JcaPGPObjectFactory(compressed.getDataStream());
            signatures = (PGPSignatureList) signatureFactory.nextObject();
        } else if (head instanceof  PGPSignatureList) {
            signatures = (PGPSignatureList) head;
        } else {
            throw new PGPException("Not a signature stream.");
        }

        PGPSignature innerSignature = signatures.get(0);
        innerSignature.init(contentVerifier, session.publicKey);
        copyAndUpdate(input, output, innerSignature);
        return innerSignature.verify();
    }


    private static void copyAndUpdate(InputStream input, OutputStream output, PGPSignature signature) throws IOException {
        int i;
        while ((i = input.read()) >= 0) {
            signature.update((byte) i);
            output.write(i);
        }
    }
}
