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
package org.apache.nifi.migration;

import org.apache.nifi.components.PropertyDescriptor;

import java.net.Proxy;
import java.util.HashMap;
import java.util.Map;

public final class ProxyServiceMigration {

    static final String PROXY_SERVICE_CLASSNAME = "org.apache.nifi.proxy.StandardProxyConfigurationService";

    static final String PROXY_SERVICE_TYPE = "proxy-type";
    static final String PROXY_SERVICE_HOST = "proxy-server-host";
    static final String PROXY_SERVICE_PORT = "proxy-server-port";
    static final String PROXY_SERVICE_USERNAME = "proxy-user-name";
    static final String PROXY_SERVICE_PASSWORD = "proxy-user-password";

    private ProxyServiceMigration() { }

    /**
     * Migrates component level proxy properties to ProxyConfigurationService with HTTP proxy type.
     *
     * @param config the component's property config to be migrated
     * @param proxyServiceProperty the component's property descriptor referencing ProxyConfigurationService
     * @param proxyHostProperty the name of the component level Proxy Host property
     * @param proxyPortProperty the name of the component level Proxy Port property
     * @param proxyUsernameProperty the name of the component level Proxy Username property
     * @param proxyPasswordProperty the name of the component level Proxy Password property
     */
    public static void migrateProxyProperties(final PropertyConfiguration config, final PropertyDescriptor proxyServiceProperty,
                                              final String proxyHostProperty, final String proxyPortProperty,
                                              final String proxyUsernameProperty, final String proxyPasswordProperty) {
        migrateProxyProperties(config,
                proxyServiceProperty,
                Proxy.Type.HTTP,
                proxyHostProperty,
                proxyPortProperty,
                proxyUsernameProperty,
                proxyPasswordProperty);
    }

    /**
     * Migrates component level proxy properties to ProxyConfigurationService with the specified proxy type.
     *
     * @param config the component's property config to be migrated
     * @param proxyServiceProperty the component's property descriptor referencing ProxyConfigurationService
     * @param proxyTypeProperty the name of the component level Proxy Type property
     * @param proxyHostProperty the name of the component level Proxy Host property
     * @param proxyPortProperty the name of the component level Proxy Port property
     * @param proxyUsernameProperty the name of the component level Proxy Username property
     * @param proxyPasswordProperty the name of the component level Proxy Password property
     */
    public static void migrateProxyProperties(final PropertyConfiguration config, final PropertyDescriptor proxyServiceProperty,
                                              final String proxyTypeProperty,
                                              final String proxyHostProperty, final String proxyPortProperty,
                                              final String proxyUsernameProperty, final String proxyPasswordProperty) {
        final Proxy.Type proxyType = Proxy.Type.valueOf(config.getRawPropertyValue(proxyTypeProperty).orElse(Proxy.Type.DIRECT.name()));

        migrateProxyProperties(config,
                proxyServiceProperty,
                proxyType,
                proxyHostProperty,
                proxyPortProperty,
                proxyUsernameProperty,
                proxyPasswordProperty);

        config.removeProperty(proxyTypeProperty);
    }

    private static void migrateProxyProperties(final PropertyConfiguration config, final PropertyDescriptor proxyServiceProperty,
                                              final Proxy.Type proxyType,
                                              final String proxyHostProperty, final String proxyPortProperty,
                                              final String proxyUsernameProperty, final String proxyPasswordProperty) {
        if (config.isPropertySet(proxyHostProperty)) {
            final Map<String, String> proxyProperties = new HashMap<>();
            proxyProperties.put(PROXY_SERVICE_TYPE, proxyType.name());
            proxyProperties.put(PROXY_SERVICE_HOST, config.getRawPropertyValue(proxyHostProperty).get());

            // Map any optional proxy configs
            config.getRawPropertyValue(proxyPortProperty).ifPresent(value -> proxyProperties.put(PROXY_SERVICE_PORT, value));
            config.getRawPropertyValue(proxyUsernameProperty).ifPresent(value -> proxyProperties.put(PROXY_SERVICE_USERNAME, value));
            config.getRawPropertyValue(proxyPasswordProperty).ifPresent(value -> proxyProperties.put(PROXY_SERVICE_PASSWORD, value));

            final String serviceId = config.createControllerService(PROXY_SERVICE_CLASSNAME, proxyProperties);
            config.setProperty(proxyServiceProperty, serviceId);
        }

        config.removeProperty(proxyHostProperty);
        config.removeProperty(proxyPortProperty);
        config.removeProperty(proxyUsernameProperty);
        config.removeProperty(proxyPasswordProperty);
    }
}
