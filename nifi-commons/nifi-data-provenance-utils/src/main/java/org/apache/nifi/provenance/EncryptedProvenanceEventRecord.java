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
package org.apache.nifi.provenance;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedProvenanceEventRecord extends StandardProvenanceEventRecord {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedProvenanceEventRecord.class);

    // Notable fields NOT encryptable -- storage & content offsets & claims and eventId
    private static final List<String> ENCRYPTABLE_STRING_PROPERTIES =
            Collections.unmodifiableList(Arrays.asList("componentId", "componentType", "transitUri", "sourceSystemFlowFileIdentifier", "uuid",
                    "alternateIdentifierUri", "details", "relationship", "sourceQueueIdentifier"));
    // TODO: Include above list
    private static final Map<String, Class> ENCRYPTABLE_PROPERTIES = generateNonStringPropertiesMap();
    private String keyId;
    private String algorithm;
    private String version;
    private byte[] iv;

    private boolean isEncrypted;
    private static final String LIST_DELIMITER = ",";

    EncryptedProvenanceEventRecord(final Builder builder) {
        super(builder);
        isEncrypted = false;
    }

    /**
     * This method performs a standard copy {@link StandardProvenanceEventRecord#copy(StandardProvenanceEventRecord)}
     * and then if the source was an {@link EncryptedProvenanceEventRecord},
     * will also copy the encryption-specific values.
     *
     * @param other the record to copy
     * @return an {@link EncryptedProvenanceEventRecord} instance (does not perform any encryption)
     */
    public static EncryptedProvenanceEventRecord copy(ProvenanceEventRecord other) {
        EncryptedProvenanceEventRecord copy = (EncryptedProvenanceEventRecord) StandardProvenanceEventRecord.copy((StandardProvenanceEventRecord) other);
        if (other instanceof EncryptedProvenanceEventRecord) {
            EncryptedProvenanceEventRecord encryptedOther = (EncryptedProvenanceEventRecord) other;
            copy.keyId = encryptedOther.getKeyId();
            copy.version = encryptedOther.getVersion();
            copy.algorithm = encryptedOther.getAlgorithm();
            copy.iv = encryptedOther.getIv();
            copy.isEncrypted = encryptedOther.isEncrypted();
        }
        return copy;
    }

    private static Map<String, Class> generateNonStringPropertiesMap() {
        Map<String, Class> props = new HashMap<>();
        props.put("eventTime", Long.class);
        props.put("entryDate", Long.class);
        props.put("eventType", ProvenanceEventType.class);
        props.put("lineageStartDate", Long.class);
        props.put("parentUuids", List.class);
        props.put("childrenUuids", List.class);
        props.put("eventDuration", Long.class);
        props.put("previousAttributes", Map.class);
        props.put("updatedAttributes", Map.class);

        return Collections.unmodifiableMap(props);
    }

    public static Map<String, Class> getEncryptableNonStringProperties() {
        return ENCRYPTABLE_PROPERTIES;
    }

    public static List<String> getEncryptableStringProperties() {
        return ENCRYPTABLE_STRING_PROPERTIES;
    }

    public void encrypt(Cipher cipher, String keyId, String version) throws EncryptionException {
        try {
            for (String prop : getEncryptableStringProperties()) {
                String existingValue = getProperty(prop);
                byte[] cipherBytes = cipher.doFinal(existingValue.getBytes(StandardCharsets.UTF_8));
                String encryptedValue = Base64.toBase64String(cipherBytes);
                setProperty(prop, encryptedValue);
                logger.debug("Encrypted " + prop + " for " + getBestEventIdentifier());
            }
            // TODO: Handle non-String fields (serialize, join, etc.)
            this.version = version;
            this.keyId = keyId;
            this.algorithm = cipher.getAlgorithm();
            this.iv = cipher.getIV();
            this.isEncrypted = true;
        } catch (BadPaddingException | IllegalBlockSizeException | IllegalAccessException e) {
            logger.error("Encountered an error encrypting the provenance record: ", e.getLocalizedMessage());
            throw new EncryptionException(e);
        }
    }

    @Override
    public String toString() {
        return "EncryptedProvenanceEventRecord ["
                + "eventId=" + getEventId()
                + ", eventType=" + getEventType()
                + ", eventTime=" + new Date(getEventTime())
                + ", uuid=" + getFlowFileUuid()
                + ", fileSize=" + getFileSize()
                + ", componentId=" + getComponentId()
                + ", transitUri=" + getTransitUri()
                + ", sourceSystemFlowFileIdentifier=" + getSourceSystemFlowFileIdentifier()
                + ", parentUuids=" + getParentUuids()
                + ", alternateIdentifierUri=" + getAlternateIdentifierUri()
                + ", keyId=" + getKeyId()
                + ", algorithm=" + getAlgorithm()
                + ", version=" + getVersion()
                + ", iv=" + Hex.toHexString(getIv())
                + ", isEncrypted=" + isEncrypted()
                + "]";
    }

    public String getKeyId() {
        return keyId;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public String getVersion() {
        return version;
    }

    public byte[] getIv() {
        return iv;
    }

    public boolean isEncrypted() {
        return isEncrypted;
    }

    public String getProperty(String property) throws IllegalAccessException {
        if (CryptoUtils.isEmpty(property)) {
            throw new IllegalArgumentException("Cannot access empty property");
        }
        if (!getAllEncryptablePropertyNames().contains(property)) {
            throw new IllegalAccessException(property + " is not an encryptable property");
        }

        try {
            Class<?> c = Class.forName(this.getClass().getName());
            Field f = c.getDeclaredField(property);
            Class<?> propertyClass = f.getType();
            // TODO: Extend to full list of types, but currently only String, long, and List are used
            // TODO: Serialize eventType to String
            // TODO: Serialize attributes (Map) to String
            String value;
            if (propertyClass.equals(String.class)) {
                value = (String) f.get(this);
            } else if (propertyClass.equals(long.class)) {
                long l = f.getLong(this);
                value = Long.toString(l);
            } else if (propertyClass.equals(List.class)) {
                // So far always a List<String>
                List<String> list = (List<String>) f.get(this);
                // Serialize with ',' delimiter
                value = CryptoUtils.serializeList(list, LIST_DELIMITER);
            } else {
                throw new IllegalArgumentException(property + " of type " + propertyClass.getName() + " is not a supported type");
            }

            logger.debug("Retrieved field " + property + " of type " + propertyClass.getName());
            logger.debug("Returning String representation");
            return value;
        } catch (ClassNotFoundException | NoSuchFieldException e) {
            logger.error("Encountered an error: ", e);
            throw new IllegalArgumentException("Cannot access " + property, e);
        }
    }

    /**
     * Returns a complete list of all property names which are encryptable (includes String and non-String fields). This order is consistent (all String properties, then all non-String
     * properties, sorted alphabetically).
     *
     * @return a consistently-ordered list
     */
    static List<String> getAllEncryptablePropertyNames() {
        List<String> names = new ArrayList<>(ENCRYPTABLE_STRING_PROPERTIES);
        names.addAll(ENCRYPTABLE_PROPERTIES.keySet().stream().sorted().collect(Collectors.toList()));
        return names;
    }

    public void setProperty(String property, String value) throws IllegalAccessException {
        if (CryptoUtils.isEmpty(property)) {
            throw new IllegalArgumentException("Cannot access empty property");
        }
        if (!getAllEncryptablePropertyNames().contains(property)) {
            throw new IllegalAccessException(property + " is not an encryptable property");
        }

        try {
            Class<?> c = Class.forName(this.getClass().getName());
            Field f = c.getDeclaredField(property);
            Class<?> propertyClass = f.getType();
            // TODO: Extend to full list of types, but currently only String, long, and List are used
            // TODO: Serialize eventType to String
            // TODO: Serialize attributes (Map) to String
            if (propertyClass.equals(String.class)) {
                f.set(this, value);
            } else if (propertyClass.equals(long.class)) {
                long l = Long.valueOf(value);
                f.setLong(this, l);
                logger.debug("Parsed long from String");
            } else if (propertyClass.equals(List.class)) {
                // So far always a List<String>
                List<String> list = CryptoUtils.deserializeList(value, LIST_DELIMITER);
                f.set(this, list);
                logger.debug("Parsed List<String> from String");
            } else {
                throw new IllegalArgumentException(property + " of type " + propertyClass.getName() + " is not a supported type");
            }

            logger.debug("Set field " + property + " of type " + propertyClass.getName());
        } catch (ClassNotFoundException | NoSuchFieldException e) {
            logger.error("Encountered an error: ", e);
            throw new IllegalArgumentException("Cannot access " + property, e);
        }
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && encryptionParamsEqual(o);
    }

    private boolean encryptionParamsEqual(Object o) {
        EncryptedProvenanceEventRecord other = (EncryptedProvenanceEventRecord) o;
        return getKeyId().equals(other.getKeyId())
                && getAlgorithm().equals(other.getAlgorithm())
                && getVersion().equals(other.getVersion())
                && Arrays.equals(getIv(), other.getIv());
    }
}
