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
package org.apache.nifi.controller.service;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.BundleUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class StandardControllerServiceApiLookup implements ControllerServiceApiLookup {

    private final ExtensionManager extensionManager;

    public StandardControllerServiceApiLookup(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    public Map<String, ControllerServiceAPI> getRequiredServiceApis(final String type, final Bundle bundle) {
        final Optional<BundleCoordinate> compatibleBundle = BundleUtils.getOptionalCompatibleBundle(extensionManager, type, BundleUtils.createBundleDto(bundle));
        if (!compatibleBundle.isPresent()) {
            return Collections.emptyMap();
        }

        final Map<String, ControllerServiceAPI> serviceApis = new HashMap<>();
        final ConfigurableComponent tempComponent = extensionManager.getTempComponent(type, compatibleBundle.get());

        for (final PropertyDescriptor descriptor : tempComponent.getPropertyDescriptors()) {
            final Class<? extends ControllerService> requiredServiceApiClass = descriptor.getControllerServiceDefinition();
            if (requiredServiceApiClass == null) {
                continue;
            }

            final ClassLoader serviceApiClassLoader = requiredServiceApiClass.getClassLoader();
            final org.apache.nifi.bundle.Bundle serviceApiBundle = extensionManager.getBundle(serviceApiClassLoader);
            final BundleDetails serviceApiBundleDetails = serviceApiBundle.getBundleDetails();
            final BundleCoordinate serviceApiBundleCoordinate = serviceApiBundleDetails.getCoordinate();

            final ControllerServiceAPI serviceApi = new ControllerServiceAPI();
            serviceApi.setType(requiredServiceApiClass.getCanonicalName());
            serviceApi.setBundle(new Bundle(serviceApiBundleCoordinate.getGroup(), serviceApiBundleCoordinate.getId(), serviceApiBundleCoordinate.getVersion()));
            serviceApis.put(descriptor.getName(), serviceApi);
        }

        return serviceApis;
    }
}
