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

    private Bundle convertBundle(final org.apache.nifi.bundle.Bundle bundle) {
        final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();
        return new Bundle(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion());
    }
}
