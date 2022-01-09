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
package org.apache.nifi.registry.provider.hook;

import org.apache.nifi.registry.extension.ExtensionClassLoader;
import org.apache.nifi.registry.extension.ExtensionManager;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.provider.ProviderCreationException;
import org.apache.nifi.registry.provider.ProviderFactory;
import org.apache.nifi.registry.provider.StandardProviderFactory;
import org.junit.Test;
import org.mockito.Mockito;

import javax.sql.DataSource;

import java.net.URL;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TestScriptEventHookProvider {

    @Test(expected = ProviderCreationException.class)
    public void testBadScriptProvider() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiRegistryProperties.PROVIDERS_CONFIGURATION_FILE, "src/test/resources/provider/hook/bad-script-provider.xml");
        final NiFiRegistryProperties props = new NiFiRegistryProperties(properties);

        final ExtensionManager extensionManager = Mockito.mock(ExtensionManager.class);
        when(extensionManager.getExtensionClassLoader(any(String.class)))
                .thenReturn(new ExtensionClassLoader("/tmp", new URL[0],this.getClass().getClassLoader()));

        final DataSource dataSource = Mockito.mock(DataSource.class);

        final ProviderFactory providerFactory = new StandardProviderFactory(props, extensionManager, dataSource);
        providerFactory.initialize();
        providerFactory.getEventHookProviders();
    }

}
