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
package org.apache.nifi.util;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.web.api.dto.BundleDTO;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for Bundles.
 */
public final class BundleUtils {

    private static BundleCoordinate findBundleForType(final String type, final BundleCoordinate desiredCoordinate) {
        final List<Bundle> bundles = ExtensionManager.getBundles(type);
        if (bundles.isEmpty()) {
            throw new IllegalStateException(String.format("%s is not known to this NiFi instance.", type));
        } else if (bundles.size() > 1) {
            if (desiredCoordinate == null) {
                throw new IllegalStateException(String.format("Multiple versions of %s exist.", type));
            } else {
                throw new IllegalStateException(String.format("Multiple versions of %s exist. No exact match for %s.", type, desiredCoordinate));
            }
        } else {
            return bundles.get(0).getBundleDetails().getCoordinate();
        }
    }

    private static BundleCoordinate findCompatibleBundle(final String type, final BundleDTO bundleDTO, final boolean allowCompatibleBundle) {
        final BundleCoordinate coordinate = new BundleCoordinate(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
        final Bundle bundle = ExtensionManager.getBundle(coordinate);

        if (bundle == null) {
            if (allowCompatibleBundle) {
                return findBundleForType(type, coordinate);
            } else {
                throw new IllegalStateException(String.format("%s from %s is not known to this NiFi instance.", type, coordinate));
            }
        } else {
            final List<BundleCoordinate> bundlesForType = ExtensionManager.getBundles(type).stream().map(b -> b.getBundleDetails().getCoordinate()).collect(Collectors.toList());
            if (bundlesForType.contains(coordinate)) {
                return coordinate;
            } else {
                throw new IllegalStateException(String.format("Found bundle %s but does not support %s", coordinate, type));
            }
        }
    }

    /**
     * Gets a bundle that supports the specified type. If the bundle is specified, an
     * exact match must be available.
     *
     *  <ul>
     *      <li>If bundleDTO is specified</li>
     *      <ul>
     *          <li>Matching bundle found</li>
     *          <ul>
     *              <li>If bundle supports type, use it</li>
     *              <li>If bundle doesn't support type, throw IllegalStateException</li>
     *          </ul>
     *          <li>No matching bundle found, IllegalStateException</li>
     *      </ul>
     *      <li>If bundleDTO is not specified</li>
     *      <ul>
     *          <li>One bundle that supports the specified type, use it</li>
     *          <li>No bundle that supports the specified type, IllegalStateException</li>
     *          <li>Multiple bundle that supports the specified type, IllegalStateException</li>
     *      </ul>
     *  </ul>
     *
     * @param type the component type
     * @param bundleDTO bundle to find the component
     * @return the bundle coordinate
     * @throws IllegalStateException bundle not found
     */
    public static BundleCoordinate getBundle(final String type, final BundleDTO bundleDTO) {
        if (bundleDTO == null) {
            return findBundleForType(type, null);
        } else {
            return findCompatibleBundle(type, bundleDTO, false);
        }
    }

    /**
     * Gets a compatible bundle that supports the specified type. If the bundle is
     * specified but is not available, a compatible bundle may be returned if there
     * is only one.
     *
     *  <ul>
     *      <li>If bundleDTO is specified</li>
     *      <ul>
     *          <li>Matching bundle found</li>
     *          <ul>
     *              <li>If bundle supports type, use it</li>
     *              <li>If bundle doesn't support type, throw IllegalStateException</li>
     *          </ul>
     *          <li>No matching bundle found</li>
     *          <ul>
     *              <li>One bundle that supports the specified type, use it</li>
     *              <li>No bundle that supports the specified type, IllegalStateException</li>
     *              <li>Multiple bundle that supports the specified type, IllegalStateException</li>
     *          </ul>
     *      </ul>
     *      <li>If bundleDTO is not specified</li>
     *      <ul>
     *          <li>One bundle that supports the specified type, use it</li>
     *          <li>No bundle that supports the specified type, IllegalStateException</li>
     *          <li>Multiple bundle that supports the specified type, IllegalStateException</li>
     *      </ul>
     *  </ul>
     *
     * @param type the component type
     * @param bundleDTO bundle to find the component
     * @return the bundle coordinate
     * @throws IllegalStateException no compatible bundle found
     */
    public static BundleCoordinate getCompatibleBundle(final String type, final BundleDTO bundleDTO) {
        if (bundleDTO == null) {
            return findBundleForType(type, null);
        } else {
            return findCompatibleBundle(type, bundleDTO, true);
        }
    }

}
