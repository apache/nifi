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
package org.apache.nifi.security.ssl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Standard implementation of PEM Certificate Reader using X.509 Certificate Factory
 */
class StandardPemCertificateReader implements PemCertificateReader {
    static final String CERTIFICATE_HEADER = "-----BEGIN CERTIFICATE-----";

    static final String CERTIFICATE_FOOTER = "-----END CERTIFICATE-----";

    private static final String CERTIFICATE_FACTORY_TYPE = "X.509";

    private static final char LINE_FEED = 10;

    private static final Charset CERTIFICATE_CHARACTER_SET = StandardCharsets.US_ASCII;

    /**
     * Read Certificates from stream of PEM sections
     *
     * @param inputStream Input Stream required
     * @return Parsed certificates or empty when none found
     */
    @Override
    public List<Certificate> readCertificates(final InputStream inputStream) {
        Objects.requireNonNull(inputStream, "Input Stream required");

        final CertificateFactory certificateFactory = getCertificateFactory();
        final List<Certificate> certificates = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, CERTIFICATE_CHARACTER_SET))) {
            String line = reader.readLine();
            while (line != null) {
                if (CERTIFICATE_HEADER.contentEquals(line)) {
                    final Certificate certificate = readCertificate(reader, certificateFactory);
                    certificates.add(certificate);
                }

                line = reader.readLine();
            }
        } catch (final IOException e) {
            throw new ReadEntityException("Read certificates failed", e);
        }

        return certificates;
    }

    private Certificate readCertificate(final BufferedReader reader, final CertificateFactory certificateFactory) throws IOException {
        final StringBuilder builder = new StringBuilder();
        builder.append(CERTIFICATE_HEADER);
        builder.append(LINE_FEED);

        String line = reader.readLine();
        while (line != null) {
            if (CERTIFICATE_FOOTER.contentEquals(line)) {
                builder.append(CERTIFICATE_FOOTER);
                builder.append(LINE_FEED);
                break;
            } else {
                builder.append(line);
                builder.append(LINE_FEED);
            }

            line = reader.readLine();
        }

        final String certificate = builder.toString();
        final byte[] certificateBytes = certificate.getBytes(CERTIFICATE_CHARACTER_SET);
        final InputStream certificateStream = new ByteArrayInputStream(certificateBytes);

        try {
            return certificateFactory.generateCertificate(certificateStream);
        } catch (final CertificateException e) {
            throw new ReadEntityException("Certificate parsing failed", e);
        }
    }

    private CertificateFactory getCertificateFactory() {
        try {
            return CertificateFactory.getInstance(CERTIFICATE_FACTORY_TYPE);
        } catch (final CertificateException e) {
            final String message = String.format("Certificate Factory Type [%s] creation failed", CERTIFICATE_FACTORY_TYPE);
            throw new BuilderConfigurationException(message, e);
        }
    }
}
