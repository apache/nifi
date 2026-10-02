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

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.net.IDN;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.security.auth.x500.X500Principal;

/**
 * Standard X.509 Certificate Builder
 */
public class StandardCertificateBuilder implements CertificateBuilder {
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final String SIGNATURE_ALGORITHM_OID = "1.2.840.113549.1.1.11";
    private static final String CERTIFICATE_TYPE = "X.509";
    private static final String COMMON_NAME_TYPE = "CN";
    private static final String LOCALHOST = "localhost";
    private static final String BASIC_CONSTRAINTS_OID = "2.5.29.19";
    private static final String KEY_USAGE_OID = "2.5.29.15";
    private static final String SUBJECT_KEY_IDENTIFIER_OID = "2.5.29.14";
    private static final String AUTHORITY_KEY_IDENTIFIER_OID = "2.5.29.35";
    private static final String EXTENDED_KEY_USAGE_OID = "2.5.29.37";
    private static final String SUBJECT_ALTERNATIVE_NAME_OID = "2.5.29.17";
    private static final String CLIENT_AUTHENTICATION_OID = "1.3.6.1.5.5.7.3.2";
    private static final String SERVER_AUTHENTICATION_OID = "1.3.6.1.5.5.7.3.1";
    private static final int NO_UNUSED_BITS = 0;
    private static final int VERSION_TAG = 0;
    private static final int EXTENSIONS_TAG = 3;
    private static final int DNS_NAME_TAG = 2;
    private static final int VERSION_3 = 2;
    private static final int ASCII_LIMIT = 128;
    private static final int LEAF_KEY_USAGE_UNUSED_BITS = 3;
    private static final byte LEAF_KEY_USAGE = (byte) 0xF8;
    private static final int AUTHORITY_KEY_USAGE_UNUSED_BITS = 1;
    private static final byte AUTHORITY_KEY_USAGE = (byte) 0xFE;
    private static final boolean CRITICAL = true;
    private static final boolean NOT_CRITICAL = false;
    private static final byte[] SIGNATURE_ALGORITHM_IDENTIFIER = DerEncoder.sequence(
            DerEncoder.objectIdentifier(SIGNATURE_ALGORITHM_OID),
            DerEncoder.nullValue()
    );

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
        try {
            final byte[] encodedCertificate = getEncodedCertificate();
            final CertificateFactory certificateFactory = CertificateFactory.getInstance(CERTIFICATE_TYPE);
            return (X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(encodedCertificate));
        } catch (final CertificateException e) {
            throw new IllegalArgumentException("X.509 Certificate conversion failed", e);
        } catch (final GeneralSecurityException e) {
            throw new IllegalArgumentException("Certificate Signer creation failed", e);
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
        final Collection<String> requiredDnsNames = Objects.requireNonNull(dnsSubjectAlternativeNames, "DNS Names required");
        this.dnsSubjectAlternativeNames = new LinkedHashSet<>(requiredDnsNames);
        return this;
    }

    private byte[] getEncodedCertificate() throws GeneralSecurityException {
        final byte[] tbsCertificate = getTbsCertificate();
        final Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
        final PrivateKey issuerPrivateKey = issuerKeyPair.getPrivate();
        signature.initSign(issuerPrivateKey);
        signature.update(tbsCertificate);
        final byte[] signatureBytes = signature.sign();
        final byte[] signatureValue = DerEncoder.bitString(NO_UNUSED_BITS, signatureBytes);
        return DerEncoder.sequence(tbsCertificate, SIGNATURE_ALGORITHM_IDENTIFIER, signatureValue);
    }

    private byte[] getTbsCertificate() {
        final Date notBefore = new Date();
        final Instant notBeforeInstant = notBefore.toInstant();
        final Instant notAfterInstant = notBeforeInstant.plus(validityPeriod);
        final Date notAfter = Date.from(notAfterInstant);
        final byte[] notBeforeEncoded = DerEncoder.time(notBefore);
        final byte[] notAfterEncoded = DerEncoder.time(notAfter);
        final byte[] validity = DerEncoder.sequence(notBeforeEncoded, notAfterEncoded);
        final BigInteger versionNumber = BigInteger.valueOf(VERSION_3);
        final byte[] versionEncoded = DerEncoder.integer(versionNumber);
        final byte[] version = DerEncoder.explicit(VERSION_TAG, versionEncoded);
        final byte[] serialNumberEncoded = DerEncoder.integer(serialNumber);
        final byte[] issuerEncoded = issuer.getEncoded();
        final byte[] subjectEncoded = subject.getEncoded();
        final byte[] subjectPublicKeyEncoded = subjectPublicKey.getEncoded();
        final byte[] extensionsEncoded = getExtensions();
        final byte[] extensions = DerEncoder.explicit(EXTENSIONS_TAG, extensionsEncoded);
        return DerEncoder.sequence(
                version,
                serialNumberEncoded,
                SIGNATURE_ALGORITHM_IDENTIFIER,
                issuerEncoded,
                validity,
                subjectEncoded,
                subjectPublicKeyEncoded,
                extensions
        );
    }

    private byte[] getExtensions() {
        final PublicKey issuerPublicKey = issuerKeyPair.getPublic();
        final boolean certificateAuthority = subjectPublicKey.equals(issuerPublicKey);
        final byte[] basicConstraints;
        if (certificateAuthority) {
            basicConstraints = DerEncoder.sequence(DerEncoder.booleanTrue());
        } else {
            basicConstraints = DerEncoder.sequence();
        }

        final int keyUsageUnusedBits = certificateAuthority ? AUTHORITY_KEY_USAGE_UNUSED_BITS : LEAF_KEY_USAGE_UNUSED_BITS;
        final byte keyUsageByte = certificateAuthority ? AUTHORITY_KEY_USAGE : LEAF_KEY_USAGE;
        final byte[] keyUsageBits = new byte[] {keyUsageByte};
        final byte[] keyUsage = DerEncoder.bitString(keyUsageUnusedBits, keyUsageBits);
        final byte[] clientAuthentication = DerEncoder.objectIdentifier(CLIENT_AUTHENTICATION_OID);
        final byte[] serverAuthentication = DerEncoder.objectIdentifier(SERVER_AUTHENTICATION_OID);
        final byte[] extendedKeyUsage = DerEncoder.sequence(clientAuthentication, serverAuthentication);
        final byte[] subjectPublicKeyEncoded = subjectPublicKey.getEncoded();
        final byte[] subjectKeyIdentifier = DerEncoder.subjectKeyIdentifier(subjectPublicKeyEncoded);
        final byte[] issuerPublicKeyEncoded = issuerPublicKey.getEncoded();
        final byte[] authorityKeyIdentifier = DerEncoder.authorityKeyIdentifier(issuerPublicKeyEncoded);
        final byte[] subjectAlternativeNames = getSubjectAlternativeNames();
        return DerEncoder.sequence(
                extension(BASIC_CONSTRAINTS_OID, NOT_CRITICAL, basicConstraints),
                extension(KEY_USAGE_OID, CRITICAL, keyUsage),
                extension(SUBJECT_KEY_IDENTIFIER_OID, NOT_CRITICAL, subjectKeyIdentifier),
                extension(AUTHORITY_KEY_IDENTIFIER_OID, NOT_CRITICAL, authorityKeyIdentifier),
                extension(EXTENDED_KEY_USAGE_OID, NOT_CRITICAL, extendedKeyUsage),
                extension(SUBJECT_ALTERNATIVE_NAME_OID, NOT_CRITICAL, subjectAlternativeNames)
        );
    }

    private byte[] extension(final String extensionOid, final boolean critical, final byte[] extensionValue) {
        final byte[] encodedObjectIdentifier = DerEncoder.objectIdentifier(extensionOid);
        final byte[] encodedValue = DerEncoder.octetString(extensionValue);
        if (critical) {
            return DerEncoder.sequence(encodedObjectIdentifier, DerEncoder.booleanTrue(), encodedValue);
        }

        return DerEncoder.sequence(encodedObjectIdentifier, encodedValue);
    }

    private byte[] getSubjectAlternativeNames() {
        final List<byte[]> encodedDnsNames = new ArrayList<>();
        final String subjectCommonName = getSubjectCommonName();
        final byte[] subjectCommonNameEncoded = getDnsNameEncoded(subjectCommonName);
        encodedDnsNames.add(subjectCommonNameEncoded);
        for (final String dnsSubjectAlternativeName : dnsSubjectAlternativeNames) {
            final byte[] dnsNameEncoded = getDnsNameEncoded(dnsSubjectAlternativeName);
            boolean encodedDnsNameFound = false;
            for (final byte[] encodedDnsName : encodedDnsNames) {
                if (Arrays.equals(encodedDnsName, dnsNameEncoded)) {
                    encodedDnsNameFound = true;
                    break;
                }
            }

            if (encodedDnsNameFound) {
                continue;
            }

            encodedDnsNames.add(dnsNameEncoded);
        }

        final byte[][] generalNames = new byte[encodedDnsNames.size()][];
        int index = 0;
        for (final byte[] dnsNameEncoded : encodedDnsNames) {
            final byte[] generalName = DerEncoder.implicit(DNS_NAME_TAG, dnsNameEncoded);
            generalNames[index++] = generalName;
        }

        return DerEncoder.sequence(generalNames);
    }

    private byte[] getDnsNameEncoded(final String dnsName) {
        for (int i = 0; i < dnsName.length(); i++) {
            if (dnsName.charAt(i) >= ASCII_LIMIT) {
                final String asciiDnsName = IDN.toASCII(dnsName);
                return asciiDnsName.getBytes(StandardCharsets.US_ASCII);
            }
        }

        return dnsName.getBytes(StandardCharsets.US_ASCII);
    }

    private String getSubjectCommonName() {
        try {
            final String subjectDistinguishedName = subject.getName();
            final LdapName subjectName = new LdapName(subjectDistinguishedName);
            for (final Rdn relativeDistinguishedName : subjectName.getRdns()) {
                final String attributeType = relativeDistinguishedName.getType();
                if (COMMON_NAME_TYPE.equalsIgnoreCase(attributeType)) {
                    final Object commonName = relativeDistinguishedName.getValue();
                    return commonName.toString();
                }
            }
        } catch (final InvalidNameException e) {
            throw new IllegalArgumentException("Subject common name parsing failed", e);
        }

        return LOCALHOST;
    }
}
