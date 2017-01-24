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

/**
 * Utility class for Bundles.
 */
public final class BundleUtils {

    public static BundleCoordinate getBundle(final String type, final BundleDTO bundleDTO) {
        final Bundle bundle;
        if (bundleDTO == null) {
            final List<Bundle> bundles = ExtensionManager.getBundles(type);
            if (bundles.isEmpty()) {
                throw new IllegalStateException(String.format("%s is not known to this NiFi instance.", type));
            } else if (bundles.size() > 1) {
                throw new IllegalStateException(String.format("Multiple versions of %s exist. Please specify the desired bundle.", type));
            } else {
                bundle = bundles.get(0);
            }
        } else {
            final BundleCoordinate coordinate = new BundleCoordinate(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
            bundle = ExtensionManager.getBundle(coordinate);
            if (bundle == null) {
                throw new IllegalStateException(String.format("%s is not known to this NiFi instance.", type));
            }
        }

        return bundle.getBundleDetails().getCoordinate();
    }

}
