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
package org.apache.nifi.registry.provider;

import org.apache.nifi.registry.extension.BundleCoordinate;
import org.apache.nifi.registry.extension.BundlePersistenceContext;
import org.apache.nifi.registry.extension.BundlePersistenceException;
import org.apache.nifi.registry.extension.BundlePersistenceProvider;
import org.apache.nifi.registry.extension.BundleVersionCoordinate;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class MockBundlePersistenceProvider implements BundlePersistenceProvider {

    private Map<String, String> properties;

    @Override
    public void createBundleVersion(final BundlePersistenceContext context, final InputStream contentStream) throws BundlePersistenceException {

    }

    @Override
    public void updateBundleVersion(final BundlePersistenceContext context, final InputStream contentStream) throws BundlePersistenceException {

    }

    @Override
    public void getBundleVersionContent(final BundleVersionCoordinate versionCoordinate, final OutputStream outputStream) throws BundlePersistenceException {

    }

    @Override
    public void deleteBundleVersion(final BundleVersionCoordinate versionCoordinate) throws BundlePersistenceException {

    }

    @Override
    public void deleteAllBundleVersions(final BundleCoordinate bundleCoordinate) throws BundlePersistenceException {

    }

    @Override
    public void onConfigured(final ProviderConfigurationContext configurationContext)
            throws ProviderCreationException {
        properties = configurationContext.getProperties();
    }

    public Map<String, String> getProperties() {
        return properties;
    }

}
