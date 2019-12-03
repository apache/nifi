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
package org.apache.nifi.properties.sensitive.gcp.kms;

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This provider uses the GCP SDK to interact with the GCP KMS.  Values are encoded/decoded base64, using the
 * standard encoders from bouncycastle.
 */
public class GCPKMSSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(GCPKMSSensitivePropertyProvider.class);
    private static final String IMPLEMENTATION_NAME = "GCP KMS Sensitive Property Provider";

    private static final String MATERIAL_PREFIX = "gcp";
    private static final String MATERIAL_KEY_TYPE = "kms";
    private static final String MATERIAL_SEPARATOR = "/";
    private static final String IMPLEMENTATION_PREFIX = MATERIAL_PREFIX + MATERIAL_SEPARATOR + MATERIAL_KEY_TYPE + MATERIAL_SEPARATOR;

    private static final Pattern simplePattern = Pattern.compile("([^/]+)/([^/]+)/([^/]+)/([^/]+)");
    private static final Pattern verbosePattern = Pattern.compile("projects/([^/]+)/locations/([^/]+)/keyRings/([^/]+)/cryptoKeys/([^/]+)");

    private String keyId;
    private String projectId;
    private String locationId;
    private String keyRingId;
    private String cryptoKeyId;

    private final String resource;
    private final KeyManagementServiceClient client;


    public GCPKMSSensitivePropertyProvider(String keyId) {
        if (StringUtils.isBlank(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"))) {
            throw new SensitivePropertyConfigurationException("Unable to find Google Application Credentials");
        }
        setKeyParts(keyId);
        this.resource = CryptoKeyName.format(projectId, locationId, keyRingId, cryptoKeyId);
        try {
            this.client = KeyManagementServiceClient.create();
        } catch (IOException e) {
            throw new SensitivePropertyConfigurationException("Unable to create service client", e);
        }
    }

    /**
     * Extracts various values from the key identifier, namely project, location, key ring id, and key id.
     * Supports both simple form (value/value/etc) and long form (projects/value/locations/value/etc).
     *
     * @param keyId key material in the form "gcp/kms/{values}"
     */
    private void setKeyParts(String keyId) {
        if (StringUtils.isBlank(keyId)) throw new SensitivePropertyConfigurationException("The key cannot be empty");

        this.keyId = keyId;

        String[] parts = this.keyId.split(MATERIAL_SEPARATOR, 3);
        if (parts.length != 3 || !parts[0].equals(MATERIAL_PREFIX) || !parts[1].equals(MATERIAL_KEY_TYPE))
            throw new SensitivePropertyConfigurationException("Invalid GCP key");

        String path = parts[2];

        Matcher match = null;
        if (verbosePattern.asPredicate().test(path)) {
            match = verbosePattern.matcher(path);
        } else if (simplePattern.asPredicate().test(path)) {
            match = simplePattern.matcher(path);
        }

        if (match == null || match.groupCount() != 4 || !match.find())
            throw new SensitivePropertyConfigurationException("Invalid GCP key pattern");

        projectId = match.group(1);
        locationId = match.group(2);
        keyRingId = match.group(3);
        cryptoKeyId = match.group(4);

        if (StringUtils.isAnyBlank(projectId, locationId, keyRingId, cryptoKeyId))
            throw new SensitivePropertyConfigurationException("Invalid GCP key identifier");
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
        return keyId;
    }

    /**
     * Returns the "protected" form of this value. This is a form which can safely be persisted in the {@code nifi.properties} file without compromising the value.
     * An encryption-based provider would return a cipher text, while a remote-lookup provider could return a unique ID to retrieve the secured value.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     */
    @Override
    public String protect(String unprotectedValue) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt an empty value");
        }

        EncryptResponse response;
        try {
            response = client.encrypt(resource, ByteString.copyFrom(unprotectedValue, StandardCharsets.UTF_8));
        } catch (final NotFoundException e) {
            throw new SensitivePropertyProtectionException(e);
        }
        return Base64.toBase64String(response.getCiphertext().toByteArray());
    }

    /**
     * Returns the "unprotected" form of this value. This is the raw sensitive value which is used by the application logic.
     * An encryption-based provider would decrypt a cipher text and return the plaintext, while a remote-lookup provider could retrieve the secured value.
     *
     * @param protectedValue the protected value read from the {@code nifi.properties} file
     * @return the raw value to be used by the application
     */
    @Override
    public String unprotect(String protectedValue) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(protectedValue)) {
            throw new IllegalArgumentException("Cannot decrypt empty or blank cipher text.");
        }
        DecryptResponse response;
        try {
            response = client.decrypt(resource, ByteString.copyFrom(Base64.decode(protectedValue)));
        } catch (final DecoderException | InvalidArgumentException | NotFoundException e) {
            throw new SensitivePropertyProtectionException(e);
        }
        return response.getPlaintext().toStringUtf8();
    }

    /**
     * True when the client specifies a key like 'gcp/kms/...'.
     *
     * @param material name of encryption or protection scheme
     * @return true if this class can provide protected values
     */
    public static boolean isProviderFor(String material) {
        return StringUtils.isNotBlank(material) && material.startsWith(IMPLEMENTATION_PREFIX);
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
