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

import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.toolkit.tls.commandLine.TlsToolkitCommandLine;
import org.apache.nifi.toolkit.tls.configuration.TlsHelperConfig;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.crmf.CRMFException;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.eac.EACException;
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
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;

public class TlsHelper {
    public static final String PROVIDER = BouncyCastleProvider.PROVIDER_NAME;
    private final KeyPairGenerator keyPairGenerator;
    private final int days;
    private final String signingAlgorithm;
    private final String keyStoreType;

    public TlsHelper(TlsHelperConfig tlsHelperConfig) throws NoSuchAlgorithmException {
        this(tlsHelperConfig.getDays(), tlsHelperConfig.getKeySize(), tlsHelperConfig.getKeyPairAlgorithm(), tlsHelperConfig.getSigningAlgorithm(), tlsHelperConfig.getKeyStoreType());
    }

    public TlsHelper(TlsToolkitCommandLine tlsToolkitCommandLine) throws NoSuchAlgorithmException {
        this(tlsToolkitCommandLine.getTlsHelperConfig());
    }

    public TlsHelper(int days, int keySize, String keyPairAlgorithm, String signingAlgorithm, String keyStoreType) throws NoSuchAlgorithmException {
        this(createKeyPairGenerator(keyPairAlgorithm, keySize), days, signingAlgorithm, keyStoreType);
    }

    protected TlsHelper(KeyPairGenerator keyPairGenerator, int days, String signingAlgorithm, String keyStoreType) {
        this.keyPairGenerator = keyPairGenerator;
        this.days = days;
        this.signingAlgorithm = signingAlgorithm;
        this.keyStoreType = keyStoreType;
    }

    private static KeyPairGenerator createKeyPairGenerator(String algorithm, int keySize) throws NoSuchAlgorithmException {
        KeyPairGenerator instance = KeyPairGenerator.getInstance(algorithm);
        instance.initialize(keySize);
        return instance;
    }

    public KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        return keyPairGenerator.generateKeyPair();
    }

    public void addToKeyStore(KeyStore keyStore, KeyPair keyPair, String alias, char[] passphrase, Certificate... certificates) throws GeneralSecurityException, IOException {
        keyStore.setKeyEntry(alias, keyPair.getPrivate(), passphrase, certificates);
    }

    public KeyStore createKeyStore() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        return createKeyStore(keyStoreType);
    }

    public KeyStore createKeyStore(String keyStoreType) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        keyStore.load(null, null);
        return keyStore;
    }

    public X509Certificate generateSelfSignedX509Certificate(KeyPair keyPair, String dn) throws CertificateException {
        return CertificateUtils.generateSelfSignedX509Certificate(keyPair, dn, signingAlgorithm, days);
    }

    public X509Certificate generateIssuedCertificate(String dn, PublicKey publicKey, X509Certificate issuer, KeyPair issuerKeyPair) throws CertificateException {
        return CertificateUtils.generateIssuedCertificate(dn, publicKey, issuer, issuerKeyPair, signingAlgorithm, days);
    }

    public JcaPKCS10CertificationRequest generateCertificationRequest(String requestedDn, KeyPair keyPair) throws OperatorCreationException {
        JcaPKCS10CertificationRequestBuilder jcaPKCS10CertificationRequestBuilder = new JcaPKCS10CertificationRequestBuilder(new X500Principal(requestedDn), keyPair.getPublic());
        JcaContentSignerBuilder jcaContentSignerBuilder = new JcaContentSignerBuilder(signingAlgorithm);
        return new JcaPKCS10CertificationRequest(jcaPKCS10CertificationRequestBuilder.build(jcaContentSignerBuilder.build(keyPair.getPrivate())));
    }

    public X509Certificate signCsr(JcaPKCS10CertificationRequest certificationRequest, X509Certificate issuer, KeyPair issuerKeyPair) throws InvalidKeySpecException, EACException,
            CertificateException, NoSuchAlgorithmException, IOException, SignatureException, NoSuchProviderException, InvalidKeyException, OperatorCreationException, CRMFException {
        return generateIssuedCertificate(certificationRequest.getSubject().toString(), certificationRequest.getPublicKey(), issuer, issuerKeyPair);
    }

    public boolean checkHMac(byte[] hmac, String token, PublicKey publicKey) throws CRMFException, NoSuchProviderException, NoSuchAlgorithmException, InvalidKeyException {
        return MessageDigest.isEqual(hmac, calculateHMac(token, publicKey));
    }

    public byte[] calculateHMac(String token, PublicKey publicKey) throws NoSuchProviderException, NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(token.getBytes(StandardCharsets.UTF_8), "RAW");
        Mac mac = Mac.getInstance("Hmac-SHA256", PROVIDER);
        mac.init(keySpec);
        return mac.doFinal(getKeyIdentifier(publicKey));
    }

    public byte[] getKeyIdentifier(PublicKey publicKey) throws NoSuchAlgorithmException {
        return new JcaX509ExtensionUtils().createSubjectKeyIdentifier(publicKey).getKeyIdentifier();
    }

    public String pemEncodeJcaObject(Object object) throws IOException {
        StringWriter writer = new StringWriter();
        try (PemWriter pemWriter = new PemWriter(writer)) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(object));
        }
        return writer.toString();
    }

    public X509Certificate parseCertificate(String pemEncodedCertificate) throws IOException, CertificateException {
        try (PEMParser pemParser = new PEMParser(new StringReader(pemEncodedCertificate))) {
            Object object = pemParser.readObject();
            if (!X509CertificateHolder.class.isInstance(object)) {
                throw new IOException("Expected " + X509CertificateHolder.class);
            }
            return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate((X509CertificateHolder) object);
        }
    }

    public JcaPKCS10CertificationRequest parseCsr(String pemEncodedCsr) throws IOException {
        try (PEMParser pemParser = new PEMParser(new StringReader(pemEncodedCsr))) {
            Object o = pemParser.readObject();
            if (!PKCS10CertificationRequest.class.isInstance(o)) {
                throw new IOException("Expecting instance of " + PKCS10CertificationRequest.class + " but got " + o);
            }
            return new JcaPKCS10CertificationRequest((PKCS10CertificationRequest) o);
        }
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }
}
