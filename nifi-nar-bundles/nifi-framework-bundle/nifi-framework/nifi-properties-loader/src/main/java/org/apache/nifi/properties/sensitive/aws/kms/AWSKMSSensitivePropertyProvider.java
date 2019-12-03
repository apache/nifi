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
package org.apache.nifi.properties.sensitive.aws.kms;

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.DecryptResult;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.EncryptResult;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.bouncycastle.util.encoders.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This provider uses the AWS SDK to interact with the AWS KMS.  Values are encoded/decoded base64, using the
 * standard encoders from bouncycastle.
 */
public class AWSKMSSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(AWSKMSSensitivePropertyProvider.class);

    private static final String IMPLEMENTATION_NAME = "AWS KMS Sensitive Property Provider";
    private static final String IMPLEMENTATION_KEY = "aws/kms/";

    private AWSKMS client;
    private final String keyId;

    public AWSKMSSensitivePropertyProvider(String keyId) {
        this.keyId = normalizeKey(keyId);
        this.client = AWSKMSClientBuilder.standard().build();
    }

    /**
     * Ensures the key is usable, and ensures the key id is just the key id, no prefix.
     *
     * @param keyId AWS KMS key identifier, possibly prefixed.
     * @return AWS KMS key identifier, bare.
     */
    private String normalizeKey(String keyId) {
        if (StringUtils.isBlank(keyId)) {
            throw new SensitivePropertyConfigurationException("The key cannot be empty");
        }
        if (keyId.startsWith(IMPLEMENTATION_KEY)) {
            keyId = keyId.substring(IMPLEMENTATION_KEY.length());
        }
        return keyId;
    }

    /**
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider
     */
    @Override
    public String getName() {
        return IMPLEMENTATION_NAME;
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return IMPLEMENTATION_KEY + keyId;
    }

    /**
     * Returns the encrypted cipher text.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     * @throws SensitivePropertyProtectionException if there is an exception encrypting the value
     */
    @Override
    public String protect(String unprotectedValue) throws SensitivePropertyProtectionException {
        if (unprotectedValue == null || StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt an empty value");
        }

        EncryptRequest request = new EncryptRequest()
            .withKeyId(keyId)
            .withPlaintext(ByteBuffer.wrap(unprotectedValue.getBytes()));

        EncryptResult response = client.encrypt(request);
        return Base64.toBase64String(response.getCiphertextBlob().array()); // BC calls String(bytes)
    }

    /**
     * Returns the decrypted plaintext.
     *
     * @param protectedValue the cipher text read from the {@code nifi.properties} file
     * @return the raw value to be used by the application
     * @throws SensitivePropertyProtectionException if there is an error decrypting the cipher text
     */
    @Override
    public String unprotect(String protectedValue) throws SensitivePropertyProtectionException {
        DecryptRequest request;
        try {
            request = new DecryptRequest().withCiphertextBlob(ByteBuffer.wrap(Base64.decode(protectedValue)));
        } catch (final org.bouncycastle.util.encoders.DecoderException e) {
            throw new SensitivePropertyProtectionException(e);
        }

        DecryptResult response;
        try {
            response = client.decrypt(request);
        } catch (final com.amazonaws.services.kms.model.InvalidCiphertextException e) {
            throw new SensitivePropertyProtectionException(e);
        }
        return new String(response.getPlaintext().array(), Charset.defaultCharset());
    }

    /**
     * True when the client specifies a key like 'aws/kms/...'.
     *
     * @param material name of encryption or protection scheme
     * @return true if this class can provide protected values
     */
    public static boolean isProviderFor(String material) {
        return StringUtils.isNotBlank(material) && material.startsWith(IMPLEMENTATION_KEY);
    }

    /**
     * Returns a printable representation of a key.
     *
     * @param key key material or key id
     * @return printable string
     */
    public static String toPrintableString(String key) {
        return key;
    }
}
