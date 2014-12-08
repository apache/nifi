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
package org.apache.nifi.processors.standard;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.io.StreamUtils;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.EventDriven;
import org.apache.nifi.processor.annotation.SideEffectFree;
import org.apache.nifi.processor.annotation.SupportsBatching;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.util.StopWatch;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.*;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.security.Security;
import java.text.Normalizer;
import java.util.*;
import java.util.concurrent.TimeUnit;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"encryption", "decryption", "password", "JCE"})
@CapabilityDescription("Encrypts or Decrypts a FlowFile using a randomly generated salt")
public class EncryptContent extends AbstractProcessor {

    public static final String ENCRYPT_MODE = "Encrypt";
    public static final String DECRYPT_MODE = "Decrypt";
    public static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
    public static final int DEFAULT_SALT_SIZE = 8;

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Specifies whether the content should be encrypted or decrypted")
            .required(true)
            .allowableValues(ENCRYPT_MODE, DECRYPT_MODE)
            .defaultValue(ENCRYPT_MODE)
            .build();
    public static final PropertyDescriptor ENCRYPTION_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Encryption Algorithm")
            .description("The Encryption Algorithm to use")
            .required(true)
            .allowableValues(EncryptionMethod.values())
            .defaultValue(EncryptionMethod.MD5_256AES.name())
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The Password to use for encrypting or decrypting the data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Any FlowFile that is successfully encrypted or decrypted will be routed to success").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Any FlowFile that cannot be encrypted or decrypted will be routed to failure").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    static {
        // add BouncyCastle encryption providers
        Security.addProvider(new BouncyCastleProvider());
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        properties.add(ENCRYPTION_ALGORITHM);
        properties.add(PASSWORD);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final String method = context.getProperty(ENCRYPTION_ALGORITHM).getValue();
        final EncryptionMethod encryptionMethod = EncryptionMethod.valueOf(method);
        final String providerName = encryptionMethod.getProvider();
        final String algorithm = encryptionMethod.getAlgorithm();

        final String password = context.getProperty(PASSWORD).getValue();
        final char[] normalizedPassword = Normalizer.normalize(password, Normalizer.Form.NFC).toCharArray();
        final PBEKeySpec pbeKeySpec = new PBEKeySpec(normalizedPassword);

        final SecureRandom secureRandom;
        final SecretKeyFactory factory;
        final SecretKey secretKey;
        final Cipher cipher;
        try {
            secureRandom = SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM);
            secureRandom.setSeed(System.currentTimeMillis());
            factory = SecretKeyFactory.getInstance(algorithm, providerName);
            secretKey = factory.generateSecret(pbeKeySpec);
            cipher = Cipher.getInstance(algorithm, providerName);
        } catch (final Exception e) {
            logger.error("failed to initialize Encryption/Decryption algorithm due to {}", new Object[]{e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final int algorithmBlockSize = cipher.getBlockSize();
        final int saltSize = (algorithmBlockSize > 0) ? algorithmBlockSize : DEFAULT_SALT_SIZE;

        final StopWatch stopWatch = new StopWatch(true);
        if (context.getProperty(MODE).getValue().equalsIgnoreCase(ENCRYPT_MODE)) {
            final byte[] salt = new byte[saltSize];
            secureRandom.nextBytes(salt);

            final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, 1000);
            try {
                cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
            } catch (final InvalidKeyException | InvalidAlgorithmParameterException e) {
                logger.error("unable to encrypt {} due to {}", new Object[]{flowFile, e});
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            flowFile = session.write(flowFile, new EncryptCallback(cipher, salt));
            logger.info("Successfully encrypted {}", new Object[]{flowFile});
        } else {
            if (flowFile.getSize() <= saltSize) {
                logger.error("Cannot decrypt {} because its file size is not greater than the salt size", new Object[]{flowFile});
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            flowFile = session.write(flowFile, new DecryptCallback(cipher, secretKey, saltSize));
            logger.info("successfully decrypted {}", new Object[]{flowFile});
        }

        session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        session.transfer(flowFile, REL_SUCCESS);
    }

    private static class DecryptCallback implements StreamCallback {

        private final Cipher cipher;
        private final SecretKey secretKey;
        private final int saltSize;

        public DecryptCallback(final Cipher cipher, final SecretKey secretKey, final int saltSize) {
            this.cipher = cipher;
            this.secretKey = secretKey;
            this.saltSize = saltSize;
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            final byte[] salt = new byte[saltSize];
            StreamUtils.fillBuffer(in, salt);

            final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, 1000);
            try {
                cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);
            } catch (final Exception e) {
                throw new ProcessException(e);
            }

            final byte[] buffer = new byte[65536];
            int len;
            while ((len = in.read(buffer)) > 0) {
                final byte[] decryptedBytes = cipher.update(buffer, 0, len);
                if (decryptedBytes != null) {
                    out.write(decryptedBytes);
                }
            }

            try {
                out.write(cipher.doFinal());
            } catch (final Exception e) {
                throw new ProcessException(e);
            }
        }
    }

    private static class EncryptCallback implements StreamCallback {

        private final Cipher cipher;
        private final byte[] salt;

        public EncryptCallback(final Cipher cipher, final byte[] salt) {
            this.cipher = cipher;
            this.salt = salt;
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            out.write(salt);

            final byte[] buffer = new byte[65536];
            int len;
            while ((len = in.read(buffer)) > 0) {
                final byte[] encryptedBytes = cipher.update(buffer, 0, len);
                if (encryptedBytes != null) {
                    out.write(encryptedBytes);
                }
            }

            try {
                out.write(cipher.doFinal());
            } catch (final IllegalBlockSizeException | BadPaddingException e) {
                throw new ProcessException(e);
            }
        }
    }
}
