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

package org.apache.nifi.c2.client.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.nifi.c2.protocol.api.SupportedOperation;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.DefinedType;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ManifestHashProvider {
    private String currentBundles = null;
    private Set<SupportedOperation> currentSupportedOperations = Collections.emptySet();
    private int currentHashCode;
    private String currentManifestHash;

    public String calculateManifestHash(List<Bundle> loadedBundles, Set<SupportedOperation> supportedOperations) {
        String bundleString = loadedBundles.stream()
                .map(this::getComponentCoordinates)
                .flatMap(Collection::stream)
                .sorted()
                .collect(Collectors.joining(","));
        int hashCode = Objects.hash(bundleString, supportedOperations);
        if (hashCode != currentHashCode
                || !(Objects.equals(bundleString, currentBundles) && Objects.equals(supportedOperations, currentSupportedOperations))) {
            byte[] bytes;
            try {
                bytes = MessageDigest.getInstance("SHA-512").digest(getBytes(supportedOperations, bundleString));
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Unable to set up manifest hash calculation due to not having support for the chosen digest algorithm", e);
            }
            currentHashCode = hashCode;
            currentManifestHash = bytesToHex(bytes);
            currentBundles = bundleString;
            currentSupportedOperations = supportedOperations;
        }
        return currentManifestHash;
    }

    private byte[] getBytes(Set<SupportedOperation> supportedOperations, String bundleString) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.write(bundleString.getBytes(StandardCharsets.UTF_8));
            oos.writeObject(supportedOperations);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to transform supportedOperations and bundles to byte array", e);
        }
    }

    private String bytesToHex(byte[] in) {
        final StringBuilder builder = new StringBuilder();
        for (byte b : in) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }

    private List<String> getComponentCoordinates(Bundle bundle) {
        ComponentManifest componentManifest = bundle.getComponentManifest();

        List<String> coordinates = componentManifest == null
                ? emptyList()
                : Stream.of(componentManifest.getProcessors(),
                        componentManifest.getApis(),
                        componentManifest.getControllerServices(),
                        componentManifest.getReportingTasks())
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .map(DefinedType::getType)
                .map(type -> bundle.getGroup() + bundle.getArtifact() + bundle.getVersion() + type).toList();

        return coordinates.isEmpty()
                ? singletonList(bundle.getGroup() + bundle.getArtifact() + bundle.getVersion())
                : coordinates;
    }
}
