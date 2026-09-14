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
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedExtensionComponent;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.security.encryption.PropertyEncryptionEncoder;
import org.apache.nifi.security.encryption.PropertyEncryptionException;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.security.encryption.SensitivePropertyCodec;
import org.apache.nifi.security.encryption.SensitivePropertyContext;
import org.apache.nifi.security.encryption.SensitivePropertyContextFactory;
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
                .filter(entry -> PropertyEncryptionEncoder.isEncrypted(entry.getValue()))
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

    /**
     * Decrypt the properties of a versioned component. The context supplied for each value is built from the instance identifier
     * and type of the component, matching the context supplied when the flow was serialized.
     *
     * @param component Versioned component that owns the properties
     * @param encrypted Properties of the component, which may contain encrypted values
     * @param propertyEncryptionProvider Provider used to decrypt sensitive values
     * @return Properties with sensitive values decrypted
     */
    static Map<String, String> decryptProperties(final VersionedComponent component, final Map<String, String> encrypted,
                                                 final PropertyEncryptionProvider propertyEncryptionProvider) {
        final Map<String, String> decrypted = new HashMap<>(encrypted.size());
        encrypted.forEach((key, value) -> decrypted.put(key, decrypt(value, getSensitivePropertyContext(component, key), propertyEncryptionProvider)));
        return decrypted;
    }

    /**
     * Get the context describing a sensitive property of a versioned component. The instance identifier is used rather than the
     * identifier, because the identifier of a mapped component is a generated versioned identifier while the context supplied when
     * the value was encrypted described the component instance.
     */
    static SensitivePropertyContext getSensitivePropertyContext(final VersionedComponent component, final String propertyName) {
        final String componentType = component instanceof final VersionedExtensionComponent extension ? extension.getType() : null;
        return SensitivePropertyContextFactory.forComponent(component.getInstanceIdentifier(), componentType, propertyName);
    }

    static String decrypt(final String value, final SensitivePropertyContext context, final PropertyEncryptionProvider propertyEncryptionProvider) {
        if (PropertyEncryptionEncoder.isEncrypted(value)) {
            final String encryptedValue = PropertyEncryptionEncoder.getDecoded(value);
            try {
                return SensitivePropertyCodec.decrypt(propertyEncryptionProvider, encryptedValue, context);
            } catch (final PropertyEncryptionException e) {
                final String moreDescriptiveMessage = "There was a problem decrypting a sensitive flow configuration value. " +
                        "Check that the Property Encryption Provider configuration matches the configuration used to encrypt the flow.json.gz file";
                logger.error(moreDescriptiveMessage, e);
                throw new PropertyEncryptionException(moreDescriptiveMessage, e);
            }
        } else {
            return value;
        }
    }

}
