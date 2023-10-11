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
package org.apache.nifi.security.cert.builder;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x500.style.IETFUtils;
import org.bouncycastle.asn1.x500.style.RFC4519Style;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import javax.security.auth.x500.X500Principal;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Standard X.509 Certificate Builder using Bouncy Castle components
 */
public class StandardCertificateBuilder implements CertificateBuilder {
    private static final String SIGNING_ALGORITHM = "SHA256withRSA";

    private static final String LOCALHOST = "localhost";

    private static final boolean CRITICAL = true;

    private static final boolean NOT_CRITICAL = false;

    private static final int STANDARD_KEY_USAGE = KeyUsage.digitalSignature
            | KeyUsage.keyEncipherment
            | KeyUsage.dataEncipherment
            | KeyUsage.keyAgreement
            | KeyUsage.nonRepudiation;

    private static final int AUTHORITY_KEY_USAGE = STANDARD_KEY_USAGE | KeyUsage.cRLSign | KeyUsage.keyCertSign;

    private final BigInteger serialNumber = BigInteger.valueOf(System.nanoTime());

    private final KeyPair issuerKeyPair;

    private final X500Principal issuer;

    private final Duration validityPeriod;

    private PublicKey subjectPublicKey;

    private X500Principal subject;

    private Set<String> dnsSubjectAlternativeNames = Collections.emptySet();

    /**
     * Standard Certificate Builder with Issuer Key Pair and Issuer Principal defaults for self-signing
     *
     * @param issuerKeyPair Issuer Key Pair also provides the default Subject Public Key
     * @param issuer Issuer also provides the default Subject
     * @param validityPeriod Validity period of not before and not after properties
     */
    public StandardCertificateBuilder(final KeyPair issuerKeyPair, final X500Principal issuer, final Duration validityPeriod) {
        this.issuerKeyPair = Objects.requireNonNull(issuerKeyPair, "Issuer Key Pair required");
        this.issuer = Objects.requireNonNull(issuer, "Issuer required");
        this.validityPeriod = Objects.requireNonNull(validityPeriod, "Validity Period required");
        this.subject = issuer;
        this.subjectPublicKey = issuerKeyPair.getPublic();
    }

    /**
     * Build X.509 Certificate using configured properties
     *
     * @return X.509 Certificate
     */
    @Override
    public X509Certificate build() {
        final X509CertificateHolder certificateHolder = getCertificateHolder();
        final JcaX509CertificateConverter certificateConverter = new JcaX509CertificateConverter();
        try {
            return certificateConverter.getCertificate(certificateHolder);
        } catch (final CertificateException e) {
            throw new IllegalArgumentException("X.509 Certificate conversion failed", e);
        }
    }

    /**
     * Set Subject Principal
     *
     * @param subject Subject Principal
     * @return Builder
     */
    public StandardCertificateBuilder setSubject(final X500Principal subject) {
        this.subject = Objects.requireNonNull(subject, "Subject required");
        return this;
    }

    /**
     * Set Subject Public Key
     *
     * @param subjectPublicKey Subject Public Key
     * @return Builder
     */
    public StandardCertificateBuilder setSubjectPublicKey(final PublicKey subjectPublicKey) {
        this.subjectPublicKey = Objects.requireNonNull(subjectPublicKey, "Subject Public Key required");
        return this;
    }

    /**
     * Set DNS Subject Alternative Names
     *
     * @param dnsSubjectAlternativeNames DNS Subject Alternative Names
     * @return Builder
     */
    public StandardCertificateBuilder setDnsSubjectAlternativeNames(final Collection<String> dnsSubjectAlternativeNames) {
        this.dnsSubjectAlternativeNames = new LinkedHashSet<>(Objects.requireNonNull(dnsSubjectAlternativeNames, "DNS Names required"));
        return this;
    }

    private void setExtensions(final X509v3CertificateBuilder certificateBuilder) {
        final JcaX509ExtensionUtils extensionUtils = getExtensionUtils();

        try {
            final BasicConstraints basicConstraints = getBasicConstraints();
            certificateBuilder.addExtension(Extension.basicConstraints, NOT_CRITICAL, basicConstraints);

            final KeyUsage keyUsage = getKeyUsage(basicConstraints.isCA());
            certificateBuilder.addExtension(Extension.keyUsage, CRITICAL, keyUsage);

            certificateBuilder.addExtension(Extension.subjectKeyIdentifier, NOT_CRITICAL, extensionUtils.createSubjectKeyIdentifier(subjectPublicKey));

            final PublicKey issuerPublicKey = issuerKeyPair.getPublic();
            certificateBuilder.addExtension(Extension.authorityKeyIdentifier, NOT_CRITICAL, extensionUtils.createAuthorityKeyIdentifier(issuerPublicKey));

            final KeyPurposeId[] keyPurposes = {KeyPurposeId.id_kp_clientAuth, KeyPurposeId.id_kp_serverAuth};
            final ExtendedKeyUsage extendedKeyUsage = new ExtendedKeyUsage(keyPurposes);
            certificateBuilder.addExtension(Extension.extendedKeyUsage, NOT_CRITICAL, extendedKeyUsage);

            final GeneralNames subjectAlternativeNames = getSubjectAlternativeNames();
            certificateBuilder.addExtension(Extension.subjectAlternativeName, NOT_CRITICAL, subjectAlternativeNames);
        } catch (final CertIOException e) {
            throw new IllegalArgumentException("Certificate Extension addition failed", e);
        }
    }

    private BasicConstraints getBasicConstraints() {
        final PublicKey issuerPublicKey = issuerKeyPair.getPublic();
        final boolean certificateAuthority = subjectPublicKey.equals(issuerPublicKey);
        return new BasicConstraints(certificateAuthority);
    }

    private KeyUsage getKeyUsage(final boolean certificateAuthority) {
        final int keyUsage = certificateAuthority ? AUTHORITY_KEY_USAGE : STANDARD_KEY_USAGE;
        return new KeyUsage(keyUsage);
    }

    private GeneralNames getSubjectAlternativeNames() {
        final Set<GeneralName> generalNames = new LinkedHashSet<>();

        final String subjectCommonName = getSubjectCommonName();
        final GeneralName subjectGeneralName = new GeneralName(GeneralName.dNSName, subjectCommonName);
        generalNames.add(subjectGeneralName);

        for (final String dnsSubjectAlternativeName : dnsSubjectAlternativeNames) {
            final GeneralName generalName = new GeneralName(GeneralName.dNSName, dnsSubjectAlternativeName);
            generalNames.add(generalName);
        }

        return new GeneralNames(generalNames.toArray(new GeneralName[]{}));
    }

    private String getSubjectCommonName() {
        final X500Name subjectName = getName(subject);
        final RDN[] commonNames = subjectName.getRDNs(BCStyle.CN);

        final String subjectCommonName;
        if (commonNames.length == 0) {
            subjectCommonName = LOCALHOST;
        } else {
            final RDN commonName = commonNames[0];
            final ASN1Encodable commonNameEncoded = commonName.getFirst().getValue();
            subjectCommonName = IETFUtils.valueToString(commonNameEncoded);
        }

        return subjectCommonName;
    }

    private X509CertificateHolder getCertificateHolder() {
        final X509v3CertificateBuilder certificateBuilder = getCertificateBuilder();
        setExtensions(certificateBuilder);

        final ContentSigner contentSigner = getContentSigner();
        return certificateBuilder.build(contentSigner);
    }

    private X509v3CertificateBuilder getCertificateBuilder() {
        final X500Name issuerName = getName(issuer);
        final Date notBefore = new Date();
        final Date notAfter = Date.from(notBefore.toInstant().plus(validityPeriod));
        final X500Name subjectName = getName(subject);
        final SubjectPublicKeyInfo subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(subjectPublicKey.getEncoded());
        return new X509v3CertificateBuilder(issuerName, serialNumber, notBefore, notAfter, subjectName, subjectPublicKeyInfo);
    }

    private ContentSigner getContentSigner() {
        final JcaContentSignerBuilder contentSignerBuilder = new JcaContentSignerBuilder(SIGNING_ALGORITHM);

        final PrivateKey issuerPrivateKey = issuerKeyPair.getPrivate();
        try {
            return contentSignerBuilder.build(issuerPrivateKey);
        } catch (final OperatorCreationException e) {
            throw new IllegalArgumentException("Certificate Signer creation failed", e);
        }
    }

    private JcaX509ExtensionUtils getExtensionUtils() {
        try {
            return new JcaX509ExtensionUtils();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Certificate Extension Utilities creation failed", e);
        }
    }

    private X500Name getName(final X500Principal principal) {
        return new X500Name(RFC4519Style.INSTANCE, principal.getName());
    }
}
