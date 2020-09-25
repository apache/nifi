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
package org.apache.nifi.processors.standard.crypto.cms;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.pki.CertificateService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.crypto.algorithm.CryptographicAlgorithm;
import org.apache.nifi.processors.standard.crypto.attributes.CryptographicAttribute;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.cms.CMSEnvelopedDataStreamGenerator;
import org.bouncycastle.cms.CMSException;
import org.bouncycastle.cms.RecipientInfoGenerator;
import org.bouncycastle.cms.bc.BcCMSContentEncryptorBuilder;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipientInfoGenerator;
import org.bouncycastle.operator.OutputEncryptor;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Encrypt CMS Processor writes and encrypts content using provided certificates and algorithm specified
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Cryptography", "CMS", "PKCS7", "RFC 5652", "AES"})
@CapabilityDescription("Encrypt content using Cryptographic Message Syntax")
@SideEffectFree
@SeeAlso({DecryptCMS.class})
@WritesAttributes({
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM, description = "Cryptographic Algorithm"),
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM_BLOCK_CIPHER_MODE, description = "Cryptographic Algorithm Block Cipher Mode"),
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM_CIPHER, description = "Cryptographic Algorithm Cipher"),
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM_KEY_SIZE, description = "Cryptographic Algorithm Key Size"),
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM_OBJECT_IDENTIFIER, description = "Cryptographic Algorithm Object Identifier"),
        @WritesAttribute(attribute = CryptographicAttribute.METHOD, description = "Cryptographic Method is CMS"),
        @WritesAttribute(attribute = CryptographicAttribute.PROCESSING_COMPLETED, description = "Cryptographic Processing Completed"),
        @WritesAttribute(attribute = "mime.type", description = "MIME Type of encrypted contents set to application/pkcs7-mime")
})
public class EncryptCMS extends AbstractCMSCryptographicProcessor {
    public static final PropertyDescriptor CRYPTOGRAPHIC_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Cryptographic Algorithm")
            .displayName("Cryptographic Algorithm")
            .description("Cryptographic Algorithm supports various ciphers and key sizes compatible with CMS")
            .defaultValue(CryptographicAlgorithm.AES_256_GCM.toString())
            .allowableValues(
                    CryptographicAlgorithm.AES_128_CBC.toString(),
                    CryptographicAlgorithm.AES_128_CCM.toString(),
                    CryptographicAlgorithm.AES_128_GCM.toString(),
                    CryptographicAlgorithm.AES_192_CBC.toString(),
                    CryptographicAlgorithm.AES_192_CCM.toString(),
                    CryptographicAlgorithm.AES_192_GCM.toString(),
                    CryptographicAlgorithm.AES_256_CBC.toString(),
                    CryptographicAlgorithm.AES_256_CCM.toString(),
                    CryptographicAlgorithm.AES_256_GCM.toString(),
                    CryptographicAlgorithm.TDEA_168_CBC.toString(),
                    CryptographicAlgorithm.RC2_40_CBC.toString()
            )
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor CERTIFICATE_SERVICE = new PropertyDescriptor.Builder()
            .name("Certificate Service")
            .displayName("Certificate Service")
            .description("Certificate Service provides X.509 Certificates for Recipients")
            .required(true)
            .identifiesControllerService(CertificateService.class)
            .build();

    public static final PropertyDescriptor CERTIFICATE_SEARCH = new PropertyDescriptor.Builder()
            .name("Certificate Search")
            .displayName("Certificate Search")
            .description("Certificate Search pattern defined according to configured Certificate Service")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Encryption Succeeded")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Encryption Failed")
            .build();

    public static final String MIME_TYPE = "application/pkcs7-mime; smime-type=enveloped-data";

    private static final String CONTENT_ENCRYPTOR_BUILDER_FAILED = "Building Output Encryptor Failed for Algorithm Identifier [%s]";

    private static final String ENCRYPTED_OUTPUT_STREAM_FAILED = "Opening Encrypted Output Stream Failed";

    private static final String SECURE_RANDOM_FAILED = "Building SecureRandom Failed";

    private static final String PROCESSING_FAILED = "CMS Processing Failed {}: {}";

    private static final String CERTIFICATE_FAILED = "Recipient Information Certificate Encoding Failed [%s]: %s";

    private SecureRandom secureRandom;

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    /**
     * Get Relationships
     *
     * @return Relations configured during initialization
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Get Supported Property Descriptors
     *
     * @return Supported Property Descriptors configured during initialization
     */
    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * On Trigger encrypts Flow File contents using configured algorithm and Certificate Service properties
     *
     * @param context Process Context properties configured properties
     * @param session Process Session for handling Flow Files
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            final String method = context.getProperty(CRYPTOGRAPHIC_ALGORITHM).getValue();
            final CryptographicAlgorithm cryptographicAlgorithm = getCryptographicAlgorithm(method);

            final CertificateService certificateService = context.getProperty(CERTIFICATE_SERVICE).asControllerService(CertificateService.class);
            final String search = context.getProperty(CERTIFICATE_SEARCH).evaluateAttributeExpressions(flowFile).getValue();

            flowFile = session.write(flowFile, (inputStream, outputStream) -> {
                final ASN1ObjectIdentifier algorithmIdentifier = new ASN1ObjectIdentifier(cryptographicAlgorithm.getObjectIdentifier());
                final OutputEncryptor outputEncryptor = getOutputEncryptor(algorithmIdentifier);
                final List<X509Certificate> certificates = certificateService.findCertificates(search);
                final List<RecipientInfoGenerator> recipients = getRecipients(certificates);

                final OutputStream encryptedOutputStream = getCmsOutputStream(outputStream, outputEncryptor, recipients);
                IOUtils.copy(inputStream, encryptedOutputStream);
                encryptedOutputStream.close();
            });

            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), MIME_TYPE);
            final Map<String, String> cryptographicAttributes = getCryptographicAttributes(cryptographicAlgorithm);
            flowFile = session.putAllAttributes(flowFile, cryptographicAttributes);
            session.transfer(flowFile, SUCCESS);
        } catch (final ProcessException e) {
            getLogger().error(PROCESSING_FAILED, new Object[]{flowFile, e.getMessage()}, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    /**
     * Initialize Processor Properties and Relationships
     *
     * @param context Processor Initialization Context is not used
     */
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CRYPTOGRAPHIC_ALGORITHM);
        descriptors.add(CERTIFICATE_SERVICE);
        descriptors.add(CERTIFICATE_SEARCH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        try {
            secureRandom = SecureRandom.getInstanceStrong();
        } catch (final NoSuchAlgorithmException e) {
            throw new ProcessException(SECURE_RANDOM_FAILED, e);
        }
    }

    /**
     * Get Output Encryptor
     *
     * @param objectIdentifier Algorithm Object Identifier
     * @return Output Encryptor
     */
    private OutputEncryptor getOutputEncryptor(final ASN1ObjectIdentifier objectIdentifier) {
        final BcCMSContentEncryptorBuilder builder = new BcCMSContentEncryptorBuilder(objectIdentifier);
        builder.setSecureRandom(secureRandom);
        try {
            return builder.build();
        } catch (final CMSException e) {
            final String message = String.format(CONTENT_ENCRYPTOR_BUILDER_FAILED, objectIdentifier);
            throw new ProcessException(message, e);
        }
    }

    /**
     * Get Recipient Information Generators for provided X.509 Certificates
     *
     * @param certificates X.509 Certificates
     * @return Recipient Information Generators
     */
    private List<RecipientInfoGenerator> getRecipients(final List<X509Certificate> certificates) {
        return certificates.stream().map(certificate -> {
            try {
                return new JceKeyTransRecipientInfoGenerator(certificate);
            } catch (final CertificateEncodingException e) {
                final X500Principal subjectPrincipal = certificate.getSubjectX500Principal();
                final String message = String.format(CERTIFICATE_FAILED, subjectPrincipal, e.getMessage());
                throw new ProcessException(message, e);
            }
        }).collect(Collectors.toList());
    }

    /**
     * Get CMS Encrypted Output Stream wrapping provided Output Stream
     *
     * @param outputStream    Output Stream
     * @param outputEncryptor Output Encryptor
     * @param recipients      Recipient Information Generator
     * @return Encrypted Output Stream
     * @throws IOException Thrown on CMSEnvelopedDataStreamGenerator.open()
     */
    private OutputStream getCmsOutputStream(final OutputStream outputStream, final OutputEncryptor outputEncryptor, final List<RecipientInfoGenerator> recipients) throws IOException {
        final CMSEnvelopedDataStreamGenerator dataStreamGenerator = new CMSEnvelopedDataStreamGenerator();
        recipients.forEach(dataStreamGenerator::addRecipientInfoGenerator);
        try {
            return dataStreamGenerator.open(outputStream, outputEncryptor);
        } catch (final CMSException e) {
            throw new ProcessException(ENCRYPTED_OUTPUT_STREAM_FAILED, e);
        }
    }

    /**
     * Get Cryptographic Algorithm from method property matching CryptographicAlgorithm.toString()
     *
     * @param algorithm Algorithm Property
     * @return Cryptographic Algorithm
     */
    private CryptographicAlgorithm getCryptographicAlgorithm(final String algorithm) {
        final Stream<CryptographicAlgorithm> algorithms = Arrays.stream(CryptographicAlgorithm.values());
        final Optional<CryptographicAlgorithm> foundAlgorithm = algorithms.filter(cryptographicAlgorithm ->
                cryptographicAlgorithm.toString().equals(algorithm)
        ).findFirst();
        if (foundAlgorithm.isPresent()) {
            return foundAlgorithm.get();
        } else {
            throw new IllegalArgumentException(algorithm);
        }
    }
}
