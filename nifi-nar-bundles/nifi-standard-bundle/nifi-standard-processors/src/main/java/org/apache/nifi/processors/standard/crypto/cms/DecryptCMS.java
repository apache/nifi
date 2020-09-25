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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.pki.PrivateKeyService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import org.apache.nifi.processors.standard.crypto.algorithm.CryptographicAlgorithm;
import org.apache.nifi.processors.standard.crypto.algorithm.CryptographicAlgorithmResolver;
import org.apache.nifi.processors.standard.crypto.algorithm.DefaultCryptographicAlgorithmResolver;
import org.apache.nifi.processors.standard.crypto.attributes.CryptographicAttribute;
import org.apache.nifi.processors.standard.crypto.attributes.CryptographicAttributeKey;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cms.CMSEnvelopedDataParser;
import org.bouncycastle.cms.CMSException;
import org.bouncycastle.cms.CMSTypedStream;
import org.bouncycastle.cms.KeyTransRecipient;
import org.bouncycastle.cms.KeyTransRecipientId;
import org.bouncycastle.cms.RecipientId;
import org.bouncycastle.cms.RecipientInformation;
import org.bouncycastle.cms.RecipientInformationStore;
import org.bouncycastle.cms.jcajce.JceKeyTransEnvelopedRecipient;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Decrypt CMS Processor reads and decrypts contents using private keys matching CMS Recipients
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Cryptography", "CMS", "PKCS7", "RFC 5652", "AES"})
@CapabilityDescription("Decrypt content using Cryptographic Message Syntax")
@SideEffectFree
@SeeAlso({EncryptCMS.class})
@WritesAttributes({
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM, description = "Cryptographic Algorithm"),
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM_BLOCK_CIPHER_MODE, description = "Cryptographic Algorithm Block Cipher Mode"),
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM_CIPHER, description = "Cryptographic Algorithm Cipher"),
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM_KEY_SIZE, description = "Cryptographic Algorithm Key Size"),
        @WritesAttribute(attribute = CryptographicAttribute.ALGORITHM_OBJECT_IDENTIFIER, description = "Cryptographic Algorithm Object Identifier"),
        @WritesAttribute(attribute = CryptographicAttribute.METHOD, description = "Cryptographic Method is CMS"),
        @WritesAttribute(attribute = CryptographicAttribute.PROCESSING_COMPLETED, description = "Cryptographic Processing Completed")
})
public class DecryptCMS extends AbstractCMSCryptographicProcessor {
    public static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("Private Key Service")
            .displayName("Private Key Service")
            .description("Private Key Service provides Private Keys for Recipients")
            .required(true)
            .identifiesControllerService(PrivateKeyService.class)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Decryption Succeeded")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Decryption Failed")
            .build();

    private static final String CMS_PARSING_FAILED = "CMS Parsing Failed: %s";

    private static final String PROCESSING_FAILED = "CMS Processing Failed {}";

    private static final String RECIPIENT_NOT_FOUND = "Recipient Private Key not found for Serial Numbers and Issuers";

    private static final String RECIPIENT_PARSED = "Parsed Recipient Serial Number [{}] Issuer [{}]";

    private static final String KEY_FOUND = "Recipient Private Key Found Serial Number [{}] Issuer [{}]";

    private static final String ALGORITHM_NOT_RESOLVED = "Cryptographic Algorithm not resolved [{}]";

    private static final CryptographicAlgorithmResolver RESOLVER = new DefaultCryptographicAlgorithmResolver();

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
     * Initialize Processor Properties and Relationships
     *
     * @param context Processor Initialization Context is not used
     */
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PRIVATE_KEY_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
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

        final PrivateKeyService privateKeyService = context.getProperty(PRIVATE_KEY_SERVICE).asControllerService(PrivateKeyService.class);
        try {
            flowFile = processFlowFile(flowFile, session, privateKeyService);
            session.transfer(flowFile, SUCCESS);
        } catch (final ProcessException e) {
            getLogger().error(PROCESSING_FAILED, new Object[]{flowFile}, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    /**
     * Process Flow File and write decrypted contents when matching Private Key found
     *
     * @param flowFile          Flow File to be processed
     * @param session           Process Session
     * @param privateKeyService Private Key Service for finding matching recipient private keys
     * @return Updated Flow File
     */
    private FlowFile processFlowFile(final FlowFile flowFile, final ProcessSession session, final PrivateKeyService privateKeyService) {
        final Map<String, String> flowFileAttributes = new HashMap<>();

        final FlowFile decryptedFlowFile = session.write(flowFile, (inputStream, outputStream) -> {
            final AtomicReference<KeyTransRecipientId> recipientIdFound = new AtomicReference<>();
            try {
                final CMSEnvelopedDataParser parser = new CMSEnvelopedDataParser(inputStream);
                final RecipientInformationStore recipientStore = parser.getRecipientInfos();
                final Collection<RecipientInformation> recipients = recipientStore.getRecipients();
                for (final RecipientInformation recipientInformation : recipients) {
                    final RecipientId recipientId = recipientInformation.getRID();
                    if (recipientId instanceof KeyTransRecipientId) {
                        final KeyTransRecipientId keyTransRecipientId = (KeyTransRecipientId) recipientId;
                        final BigInteger serialNumber = keyTransRecipientId.getSerialNumber();
                        final X500Name issuerName = keyTransRecipientId.getIssuer();
                        getLogger().debug(RECIPIENT_PARSED, new Object[]{serialNumber, issuerName});

                        final X500Principal issuer = new X500Principal(issuerName.toString());
                        final Optional<PrivateKey> privateKey = privateKeyService.findPrivateKey(serialNumber, issuer);
                        if (privateKey.isPresent()) {
                            getLogger().info(KEY_FOUND, new Object[]{serialNumber, issuer});
                            final InputStream contentStream = getContentStream(privateKey.get(), recipientInformation);
                            IOUtils.copy(contentStream, outputStream);
                            recipientIdFound.set(keyTransRecipientId);
                            break;
                        }
                    }
                }

                final String objectIdentifier = parser.getEncryptionAlgOID();
                final Optional<CryptographicAlgorithm> optionalAlgorithm = RESOLVER.findCryptographicAlgorithm(objectIdentifier);
                if (optionalAlgorithm.isPresent()) {
                    final CryptographicAlgorithm cryptographicAlgorithm = optionalAlgorithm.get();
                    final Map<String, String> cryptographicAttributes = getCryptographicAttributes(cryptographicAlgorithm);
                    flowFileAttributes.putAll(cryptographicAttributes);
                } else {
                    flowFileAttributes.put(CryptographicAttributeKey.ALGORITHM_OBJECT_IDENTIFIER.key(), objectIdentifier);
                    getLogger().warn(ALGORITHM_NOT_RESOLVED, new Object[]{objectIdentifier});
                }
            } catch (final CMSException e) {
                final String message = String.format(CMS_PARSING_FAILED, e.getMessage());
                throw new IOException(message, e);
            }

            if (recipientIdFound.get() == null) {
                throw new IOException(RECIPIENT_NOT_FOUND);
            }
        });

        return session.putAllAttributes(decryptedFlowFile, flowFileAttributes);
    }

    /**
     * Get Decrypted Content Stream
     *
     * @param key                  Private Key matched from Private Key Service
     * @param recipientInformation Recipient Information matching Private Key
     * @return Decrypted Input Stream
     * @throws CMSException Thrown on RecipientInformation.getContentStream()
     * @throws IOException  Thrown on RecipientInformation.getContentStream()
     */
    private InputStream getContentStream(final PrivateKey key, final RecipientInformation recipientInformation) throws CMSException, IOException {
        final KeyTransRecipient keyTransRecipient = new JceKeyTransEnvelopedRecipient(key);
        final CMSTypedStream cmsTypedStream = recipientInformation.getContentStream(keyTransRecipient);
        return cmsTypedStream.getContentStream();
    }
}
