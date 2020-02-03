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
package org.apache.nifi.processors.standard.pgp;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPPublicKey;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The EncryptPGP processor attempts to encrypt flow file contents when triggered.  The processor uses a
 * {@link PGPKeyMaterialControllerService} to provide encryption keys.
 */

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"encryption", "decryption", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Encrypts a FlowFile using a PGP key.")
@SystemResourceConsideration(resource = SystemResource.CPU)

public class EncryptPGP extends AbstractProcessorPGP {
    public static final PropertyDescriptor PGP_KEY_SERVICE =
        AbstractProcessorPGP.buildKeyServiceProperty("PGP Key Material Controller Service that provides the public key for encryption.");

    public static final PropertyDescriptor ENCRYPT_ALGORITHM = new PropertyDescriptor.Builder()
            .name("encrypt-algorithm")
            .displayName("Encryption Cipher Algorithm")
            .description("The cipher algorithm used when encrypting data.")
            .allowableValues(getCipherAllowableValues())
            .defaultValue(getCipherDefaultValue())
            .build();

    public static final PropertyDescriptor ENCRYPT_ENCODING = new PropertyDescriptor.Builder()
            .name("encrypt-encoding")
            .displayName("Encryption Data Encoding")
            .description("The data encoding method used when writing encrypting data.")
            .allowableValues(
                    new AllowableValue("0", "Raw (bytes with no encoding)"),
                    new AllowableValue("1", "PGP Armor (encoded text)"))
            .defaultValue("0")
            .build();

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final ExtendedStreamCallback callback = new EncryptStreamCallback(buildEncryptSession(context));

        try {
            final StopWatch stopWatch = new StopWatch(true);
            final FlowFile finalFlow = session.write(flowFile, callback);
            callback.postProcess(session, finalFlow);
            logger.debug("Called to encrypt flow {}", new Object[]{flowFile});
            session.getProvenanceReporter().modifyContent(finalFlow, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(finalFlow, REL_SUCCESS);
        } catch (final ProcessException e) {
            logger.error("Exception in encrypt flow {} ", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PGP_KEY_SERVICE);
        properties.add(ENCRYPT_ALGORITHM);
        properties.add(ENCRYPT_ENCODING);
        return properties;
    }

    private EncryptStreamSession buildEncryptSession(ProcessContext context) {
        final PGPKeyMaterialService service = context.getProperty(PGP_KEY_SERVICE).asControllerService(PGPKeyMaterialService.class);
        int algo = getEncryptAlgorithm(context);
        boolean armor = 1 == getEncryptionEncoding(context);

        char[] passphrase = service.getPBEPassPhrase();
        if (passphrase != null && passphrase.length != 0) {
            return new PBEEncryptStreamSession(getLogger(), passphrase, algo, armor);
        }

        final PGPPublicKey publicKey = service.getPublicKey();
        return new PublicKeyEncryptKeySession(getLogger(), publicKey, algo, armor);
    }

    private int getEncryptAlgorithm(ProcessContext context) {
        return context.getProperty(ENCRYPT_ALGORITHM).asInteger();
    }

    private int getEncryptionEncoding(ProcessContext context) {
        return context.getProperty(ENCRYPT_ENCODING).asInteger();
    }

    private static AllowableValue[] getCipherAllowableValues() {
        return new AllowableValue[]{
                // Values match integer values in org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags
                // 0 - NULL not supported
                new AllowableValue("1", "IDEA"),
                new AllowableValue("2", "TRIPLE DES"),
                new AllowableValue("3", "CAST5"),
                new AllowableValue("4", "BLOWFISH"),
                new AllowableValue("6", "DES"),
                // 6 - SAFER not supported
                new AllowableValue("7", "AES 128"),
                new AllowableValue("8", "AES 192"),
                new AllowableValue("9", "AES 256"),
                new AllowableValue("10", "TWOFISH"),
                new AllowableValue("11", "CAMELLIA 128"),
                new AllowableValue("12", "CAMELLIA 192"),
                new AllowableValue("13", "CAMELLIA 256")};
    }

    static String getCipherDefaultValue() {
        return String.valueOf(PGPEncryptedData.AES_128);
    }
}
