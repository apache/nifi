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
package org.apache.nifi.processors.standard.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.Date;
import java.util.zip.Deflater;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.EncryptContent;
import org.apache.nifi.processors.standard.EncryptContent.Encryptor;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPPBEEncryptedData;
import org.bouncycastle.openpgp.PGPUtil;

public class OpenPGPPasswordBasedEncryptor implements Encryptor {

    private String algorithm;
    private String provider;
    private char[] password;
    private String filename;

    public static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";

    public OpenPGPPasswordBasedEncryptor(final String algorithm, final String provider, final char[] passphrase, final String filename) {
        this.algorithm = algorithm;
        this.provider = provider;
        this.password = passphrase;
        this.filename = filename;
    }

    @Override
    public StreamCallback getEncryptionCallback() throws Exception {
        return new OpenPGPEncryptCallback(algorithm, provider, password, filename);
    }

    @Override
    public StreamCallback getDecryptionCallback() throws Exception {
        return new OpenPGPDecryptCallback(provider, password);
    }

    private class OpenPGPDecryptCallback implements StreamCallback {

        private String provider;
        private char[] password;

        OpenPGPDecryptCallback(final String provider, final char[] password) {
            this.provider = provider;
            this.password = password;
        }

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            InputStream pgpin = PGPUtil.getDecoderStream(in);
            PGPObjectFactory pgpFactory = new PGPObjectFactory(pgpin);

            Object obj = pgpFactory.nextObject();
            if (!(obj instanceof PGPEncryptedDataList)) {
                obj = pgpFactory.nextObject();
                if (!(obj instanceof PGPEncryptedDataList)) {
                    throw new ProcessException("Invalid OpenPGP data");
                }
            }
            PGPEncryptedDataList encList = (PGPEncryptedDataList) obj;

            obj = encList.get(0);
            if (!(obj instanceof PGPPBEEncryptedData)) {
                throw new ProcessException("Invalid OpenPGP data");
            }
            PGPPBEEncryptedData encData = (PGPPBEEncryptedData) obj;

            try {
                InputStream clearData = encData.getDataStream(password, provider);
                PGPObjectFactory clearFactory = new PGPObjectFactory(clearData);

                obj = clearFactory.nextObject();
                if (obj instanceof PGPCompressedData) {
                    PGPCompressedData compData = (PGPCompressedData) obj;
                    clearFactory = new PGPObjectFactory(compData.getDataStream());
                    obj = clearFactory.nextObject();
                }
                PGPLiteralData literal = (PGPLiteralData) obj;

                InputStream lis = literal.getInputStream();
                final byte[] buffer = new byte[4096];
                int len;
                while ((len = lis.read(buffer)) >= 0) {
                    out.write(buffer, 0, len);
                }
            } catch (Exception e) {
                throw new ProcessException(e.getMessage());
            }
        }

    }

    private class OpenPGPEncryptCallback implements StreamCallback {

        private String algorithm;
        private String provider;
        private char[] password;
        private String filename;

        OpenPGPEncryptCallback(final String algorithm, final String provider, final char[] password, final String filename) {
            this.algorithm = algorithm;
            this.provider = provider;
            this.password = password;
            this.filename = filename;
        }

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            try {
                SecureRandom secureRandom = SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM);

                OutputStream output = out;
                if (EncryptContent.isPGPArmoredAlgorithm(algorithm)) {
                    output = new ArmoredOutputStream(out);
                }

                PGPEncryptedDataGenerator encGenerator = new PGPEncryptedDataGenerator(PGPEncryptedData.CAST5, false,
                        secureRandom, provider);
                encGenerator.addMethod(password);
                OutputStream encOut = encGenerator.open(output, new byte[65536]);

                PGPCompressedDataGenerator compData = new PGPCompressedDataGenerator(PGPCompressedData.ZIP, Deflater.BEST_SPEED);
                OutputStream compOut = compData.open(encOut, new byte[65536]);

                PGPLiteralDataGenerator literal = new PGPLiteralDataGenerator();
                OutputStream literalOut = literal.open(compOut, PGPLiteralData.BINARY, filename, new Date(), new byte[65536]);

                final byte[] buffer = new byte[4096];
                int len;
                while ((len = in.read(buffer)) >= 0) {
                    literalOut.write(buffer, 0, len);
                }

                literalOut.close();
                compOut.close();
                encOut.close();
                output.close();
            } catch (Exception e) {
                throw new ProcessException(e.getMessage());
            }

        }

    }
}
