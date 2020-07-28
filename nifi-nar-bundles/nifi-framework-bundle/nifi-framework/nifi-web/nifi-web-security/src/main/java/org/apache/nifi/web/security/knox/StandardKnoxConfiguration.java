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
package org.apache.nifi.web.security.knox;

import org.apache.nifi.util.NiFiProperties;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.Set;

public class StandardKnoxConfiguration implements KnoxConfiguration {

    private final NiFiProperties properties;

    public StandardKnoxConfiguration(NiFiProperties properties) {
        this.properties = properties;
    }

    public boolean isKnoxEnabled() {
        return properties.isKnoxSsoEnabled();
    }

    public String getKnoxUrl() {
        return properties.getKnoxUrl();
    }

    @Override
    public Set<String> getAudiences() {
        return properties.getKnoxAudiences();
    }

    public String getKnoxCookieName() {
        return properties.getKnoxCookieName();
    }

    public RSAPublicKey getKnoxPublicKey() {
        // get the path to the public key
        final Path knoxPublicKeyPath = properties.getKnoxPublicKeyPath();

        // ensure the file exists
        if (Files.isRegularFile(knoxPublicKeyPath) && Files.exists(knoxPublicKeyPath)) {
            try (final InputStream publicKeyStream = Files.newInputStream(knoxPublicKeyPath)) {
                final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
                final X509Certificate certificate = (X509Certificate) certificateFactory.generateCertificate(publicKeyStream);
                return (RSAPublicKey) certificate.getPublicKey();
            } catch (final IOException | CertificateException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else {
            throw new RuntimeException(String.format("The specified Knox public key path does not exist '%s'", knoxPublicKeyPath.toString()));
        }
    }
}
