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
package org.apache.nifi.controller.serialization;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.encrypt.EncryptionException;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FlowSynchronizationUtils {

    private static final Logger logger = LoggerFactory.getLogger(FlowSynchronizationUtils.class);

    private FlowSynchronizationUtils() {
    }

    static BundleCoordinate createBundleCoordinate(final ExtensionManager extensionManager, final Bundle bundle, final String componentType) {
        BundleCoordinate coordinate;
        try {
            final BundleDTO bundleDto = new BundleDTO(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            coordinate = BundleUtils.getCompatibleBundle(extensionManager, componentType, bundleDto);
        } catch (final IllegalStateException e) {
            coordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
        }

        return coordinate;
    }

    static Set<String> getSensitiveDynamicPropertyNames(final ComponentNode componentNode, final VersionedConfigurableExtension extension) {
        final Set<String> versionedSensitivePropertyNames = new LinkedHashSet<>();

        // Get Sensitive Property Names based on encrypted values including both supported and dynamic properties
        extension.getProperties()
                .entrySet()
                .stream()
                .filter(entry -> isValueSensitive(entry.getValue()))
                .map(Map.Entry::getKey)
                .forEach(versionedSensitivePropertyNames::add);

        // Get Sensitive Property Names based on supported and dynamic property descriptors
        extension.getPropertyDescriptors()
                .values()
                .stream()
                .filter(VersionedPropertyDescriptor::isSensitive)
                .map(VersionedPropertyDescriptor::getName)
                .forEach(versionedSensitivePropertyNames::add);

        // Filter combined Sensitive Property Names based on Component Property Descriptor status
        return versionedSensitivePropertyNames.stream()
                .map(componentNode::getPropertyDescriptor)
                .filter(PropertyDescriptor::isDynamic)
                .map(PropertyDescriptor::getName)
                .collect(Collectors.toSet());
    }

    static boolean isValueSensitive(final String value) {
        return value != null && value.startsWith(FlowSerializer.ENC_PREFIX) && value.endsWith(FlowSerializer.ENC_SUFFIX);
    }

    static Map<String, String> decryptProperties(final Map<String, String> encrypted, final PropertyEncryptor encryptor) {
        final Map<String, String> decrypted = new HashMap<>(encrypted.size());
        encrypted.forEach((key, value) -> decrypted.put(key, decrypt(value, encryptor)));
        return decrypted;
    }

    static String decrypt(final String value, final PropertyEncryptor encryptor) {
        if (isValueSensitive(value)) {
            try {
                return encryptor.decrypt(value.substring(FlowSerializer.ENC_PREFIX.length(), value.length() - FlowSerializer.ENC_SUFFIX.length()));
            } catch (EncryptionException e) {
                final String moreDescriptiveMessage = "There was a problem decrypting a sensitive flow configuration value. " +
                        "Check that the nifi.sensitive.props.key value in nifi.properties matches the value used to encrypt the flow.json.gz file";
                logger.error(moreDescriptiveMessage, e);
                throw new EncryptionException(moreDescriptiveMessage, e);
            }
        } else {
            return value;
        }
    }


}
