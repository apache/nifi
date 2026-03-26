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

package org.apache.nifi.components.connector;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.nar.ExtensionManager;

import java.util.List;
import java.util.Optional;

public class StandardComponentBundleLookup implements ComponentBundleLookup {
    private final ExtensionManager extensionManager;

    public StandardComponentBundleLookup(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    public List<Bundle> getAvailableBundles(final String componentType) {
        final List<org.apache.nifi.bundle.Bundle> bundles = extensionManager.getBundles(componentType);
        return bundles.stream()
            .map(this::convertBundle)
            .toList();
    }

    @Override
    public Optional<Bundle> getLatestBundle(final String componentType) {
        final List<Bundle> availableBundles = getAvailableBundles(componentType);
        if (availableBundles.isEmpty()) {
            return Optional.empty();
        }

        Bundle newest = null;
        for (final Bundle bundle : availableBundles) {
            if (newest == null || compareVersion(bundle.getVersion(), newest.getVersion()) > 0) {
                newest = bundle;
            }
        }

        return Optional.ofNullable(newest);
    }

    private int compareVersion(final String v1, final String v2) {
        final String baseVersion1 = getBaseVersion(v1);
        final String baseVersion2 = getBaseVersion(v2);

        final String[] parts1 = baseVersion1.split("\\.");
        final String[] parts2 = baseVersion2.split("\\.");

        final int length = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            final String part1Str = i < parts1.length ? parts1[i] : "0";
            final String part2Str = i < parts2.length ? parts2[i] : "0";

            final int comparison = compareVersionPart(part1Str, part2Str);
            if (comparison != 0) {
                return comparison;
            }
        }

        // Base versions are equal; compare qualifiers
        final String qualifier1 = getQualifier(v1);
        final String qualifier2 = getQualifier(v2);
        return compareQualifiers(qualifier1, qualifier2);
    }

    private int compareQualifiers(final String qualifier1, final String qualifier2) {
        final int rank1 = getQualifierRank(qualifier1);
        final int rank2 = getQualifierRank(qualifier2);

        if (rank1 != rank2) {
            return Integer.compare(rank1, rank2);
        }

        // Same qualifier type; compare numeric suffixes (e.g., RC2 > RC1, M4 > M3)
        final int num1 = getQualifierNumber(qualifier1);
        final int num2 = getQualifierNumber(qualifier2);
        return Integer.compare(num1, num2);
    }

    private int getQualifierRank(final String qualifier) {
        if (qualifier == null || qualifier.isEmpty()) {
            return 4;
        } else if (qualifier.startsWith("RC")) {
            return 3;
        } else if (qualifier.startsWith("M")) {
            return 2;
        } else if (qualifier.equals("SNAPSHOT")) {
            return 0;
        } else {
            return 1;
        }
    }

    private int getQualifierNumber(final String qualifier) {
        if (qualifier == null || qualifier.isEmpty()) {
            return 0;
        }

        final StringBuilder digits = new StringBuilder();
        for (int i = 0; i < qualifier.length(); i++) {
            final char c = qualifier.charAt(i);
            if (Character.isDigit(c)) {
                digits.append(c);
            }
        }

        if (digits.isEmpty()) {
            return 0;
        }

        try {
            return Integer.parseInt(digits.toString());
        } catch (final NumberFormatException e) {
            return 0;
        }
    }

    private String getQualifier(final String version) {
        final int qualifierIndex = version.indexOf('-');
        return qualifierIndex > 0 ? version.substring(qualifierIndex + 1) : null;
    }

    private int compareVersionPart(final String part1, final String part2) {
        final Integer num1 = parseVersionPart(part1);
        final Integer num2 = parseVersionPart(part2);

        if (num1 != null && num2 != null) {
            return Integer.compare(num1, num2);
        } else if (num1 != null) {
            return 1;
        } else if (num2 != null) {
            return -1;
        } else {
            return part1.compareTo(part2);
        }
    }

    private Integer parseVersionPart(final String part) {
        try {
            return Integer.parseInt(part);
        } catch (final NumberFormatException e) {
            return null;
        }
    }

    private String getBaseVersion(final String version) {
        final int qualifierIndex = version.indexOf('-');
        return qualifierIndex > 0 ? version.substring(0, qualifierIndex) : version;
    }

    private Bundle convertBundle(final org.apache.nifi.bundle.Bundle bundle) {
        final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();
        return new Bundle(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion());
    }
}
