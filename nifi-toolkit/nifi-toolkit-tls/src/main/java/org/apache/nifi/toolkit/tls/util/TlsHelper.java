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

package org.apache.nifi.toolkit.tls.util;

import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.io.pem.PemWriter;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class TlsHelper {
    private TlsHelper() {

    }

    public static void addBouncyCastleProvider() {
        Security.addProvider(new BouncyCastleProvider());
    }

    private static KeyPairGenerator createKeyPairGenerator(String algorithm, int keySize) throws NoSuchAlgorithmException {
        KeyPairGenerator instance = KeyPairGenerator.getInstance(algorithm);
        instance.initialize(keySize);
        return instance;
    }

    public static byte[] calculateHMac(String token, PublicKey publicKey) throws GeneralSecurityException {
        SecretKeySpec keySpec = new SecretKeySpec(token.getBytes(StandardCharsets.UTF_8), "RAW");
        Mac mac = Mac.getInstance("Hmac-SHA256", BouncyCastleProvider.PROVIDER_NAME);
        mac.init(keySpec);
        return mac.doFinal(getKeyIdentifier(publicKey));
    }

    public static byte[] getKeyIdentifier(PublicKey publicKey) throws NoSuchAlgorithmException {
        return new JcaX509ExtensionUtils().createSubjectKeyIdentifier(publicKey).getKeyIdentifier();
    }

    public static String pemEncodeJcaObject(Object object) throws IOException {
        StringWriter writer = new StringWriter();
        try (PemWriter pemWriter = new PemWriter(writer)) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(object));
        }
        return writer.toString();
    }

    public static JcaPKCS10CertificationRequest parseCsr(String pemEncodedCsr) throws IOException {
        try (PEMParser pemParser = new PEMParser(new StringReader(pemEncodedCsr))) {
            Object o = pemParser.readObject();
            if (!PKCS10CertificationRequest.class.isInstance(o)) {
                throw new IOException("Expecting instance of " + PKCS10CertificationRequest.class + " but got " + o);
            }
            return new JcaPKCS10CertificationRequest((PKCS10CertificationRequest) o);
        }
    }

    public static X509Certificate parseCertificate(String pemEncodedCertificate) throws IOException, CertificateException {
        try (PEMParser pemParser = new PEMParser(new StringReader(pemEncodedCertificate))) {
            Object object = pemParser.readObject();
            if (!X509CertificateHolder.class.isInstance(object)) {
                throw new IOException("Expected " + X509CertificateHolder.class);
            }
            return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate((X509CertificateHolder) object);
        }
    }

    public static KeyPair generateKeyPair(String algorithm, int keySize) throws NoSuchAlgorithmException {
        return createKeyPairGenerator(algorithm, keySize).generateKeyPair();
    }

    public static JcaPKCS10CertificationRequest generateCertificationRequest(String requestedDn, KeyPair keyPair, String signingAlgorithm) throws OperatorCreationException {
        JcaPKCS10CertificationRequestBuilder jcaPKCS10CertificationRequestBuilder = new JcaPKCS10CertificationRequestBuilder(new X500Principal(requestedDn), keyPair.getPublic());
        JcaContentSignerBuilder jcaContentSignerBuilder = new JcaContentSignerBuilder(signingAlgorithm);
        return new JcaPKCS10CertificationRequest(jcaPKCS10CertificationRequestBuilder.build(jcaContentSignerBuilder.build(keyPair.getPrivate())));
    }
}
