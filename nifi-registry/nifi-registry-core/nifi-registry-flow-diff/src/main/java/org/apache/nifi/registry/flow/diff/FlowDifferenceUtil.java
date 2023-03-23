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
package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.VersionedFlowCoordinates;

public final class FlowDifferenceUtil {

    private FlowDifferenceUtil() {
        // Not to be instantiated
    }

    public static boolean areRegistryStrictlyEqual(final VersionedFlowCoordinates coordinatesA, final VersionedFlowCoordinates coordinatesB) {
        final String registryUrlA = coordinatesA.getRegistryUrl();
        final String registryUrlB = coordinatesB.getRegistryUrl();
        return registryUrlA != null && registryUrlB != null && registryUrlA.equals(registryUrlB);
    }

    public static boolean areRegistryUrlsEqual(final VersionedFlowCoordinates coordinatesA, final VersionedFlowCoordinates coordinatesB) {
        final String registryUrlA = coordinatesA.getRegistryUrl();
        final String registryUrlB = coordinatesB.getRegistryUrl();

        if (registryUrlA != null && registryUrlB != null) {
            if (registryUrlA.equals(registryUrlB)) {
                return true;
            }

            final String normalizedRegistryUrlA = registryUrlA.endsWith("/") ? registryUrlA.substring(0, registryUrlA.length() - 1) : registryUrlA;
            final String normalizedRegistryUrlB = registryUrlB.endsWith("/") ? registryUrlB.substring(0, registryUrlB.length() - 1) : registryUrlB;

            return normalizedRegistryUrlA.equals(normalizedRegistryUrlB);
        }

        return false;
    }
}
