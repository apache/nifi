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

package org.apache.nifi.controller.state.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.state.ConfigParseException;
import org.apache.nifi.util.DomUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class StateManagerConfiguration {
    private final Map<String, StateProviderConfiguration> providers;

    private StateManagerConfiguration(final Map<String, StateProviderConfiguration> providerConfigs) {
        this.providers = providerConfigs;
    }

    public Map<String, StateProviderConfiguration> getStateProviderConfigurations() {
        return Collections.unmodifiableMap(providers);
    }

    public StateProviderConfiguration getStateProviderConfiguration(final String providerId) {
        return providers.get(providerId);
    }

    public List<StateProviderConfiguration> getStateProviderConfigurations(final Scope scope) {
        final List<StateProviderConfiguration> configs = new ArrayList<>();
        for (final StateProviderConfiguration config : providers.values()) {
            if (config.getScope() == scope) {
                configs.add(config);
            }
        }

        return configs;
    }

    public static StateManagerConfiguration parse(final File configFile) throws IOException, ConfigParseException {
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);

        final Document document;
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
            document = builder.parse(configFile);
        } catch (ParserConfigurationException | SAXException e) {
            throw new ConfigParseException("Unable to parse file " + configFile + ", as it does not appear to be a valid XML File", e);
        }

        final Element rootElement = document.getDocumentElement();
        final List<Element> localProviderElements = DomUtils.getChildElementsByTagName(rootElement, "local-provider");
        if (localProviderElements.isEmpty()) {
            throw new ConfigParseException("State Management config file " + configFile + " is not a valid configuration file, "
                + "as it does not contain a 'local-provider' element, or the local-provider element is not the child of the root element");
        }

        final Map<String, StateProviderConfiguration> configs = new HashMap<>();
        for (final Element localProviderElement : localProviderElements) {
            final StateProviderConfiguration providerConfig = parseProviderConfiguration(localProviderElement, Scope.LOCAL, configFile);
            if (configs.containsKey(providerConfig.getId())) {
                throw new ConfigParseException("State Management config file " + configFile + " is not a valid configuration file, "
                    + "as it contains multiple providers with the \"id\" of \"" + providerConfig.getId() + "\"");
            }

            configs.put(providerConfig.getId(), providerConfig);
        }

        final List<Element> clusterProviderElements = DomUtils.getChildElementsByTagName(rootElement, "cluster-provider");
        for (final Element clusterProviderElement : clusterProviderElements) {
            final StateProviderConfiguration providerConfig = parseProviderConfiguration(clusterProviderElement, Scope.CLUSTER, configFile);
            if (configs.containsKey(providerConfig.getId())) {
                throw new ConfigParseException("State Management config file " + configFile + " is not a valid configuration file, "
                    + "as it contains multiple providers with the \"id\" of \"" + providerConfig.getId() + "\"");
            }

            configs.put(providerConfig.getId(), providerConfig);
        }

        return new StateManagerConfiguration(configs);
    }

    private static StateProviderConfiguration parseProviderConfiguration(final Element providerElement, final Scope scope, final File configFile) throws ConfigParseException {
        final String elementName = providerElement.getNodeName();

        final String id = DomUtils.getChildText(providerElement, "id");
        if (id == null) {
            throw new ConfigParseException("State Management config file " + configFile + " is not a valid configuration file, "
                + "as a " + elementName + " element does not contain an \"id\" element");
        }
        if (id.trim().isEmpty()) {
            throw new ConfigParseException("State Management config file " + configFile + " is not a valid configuration file, "
                + "as a " + elementName + "'s \"id\" element is empty");
        }

        final String className = DomUtils.getChildText(providerElement, "class");
        if (className == null) {
            throw new ConfigParseException("State Management config file " + configFile + " is not a valid configuration file, "
                + "as a " + elementName + " element does not contain an \"class\" element");
        }
        if (className.trim().isEmpty()) {
            throw new ConfigParseException("State Management config file " + configFile + " is not a valid configuration file, "
                + "as a " + elementName + "'s \"class\" element is empty");
        }

        final List<Element> propertyElements = DomUtils.getChildElementsByTagName(providerElement, "property");
        final Map<String, String> propertyMap = new HashMap<>();
        for (final Element propertyElement : propertyElements) {
            final String propertyName = propertyElement.getAttribute("name");
            final String propertyValue = propertyElement.getTextContent();
            propertyMap.put(propertyName, propertyValue);
        }

        return new StateProviderConfiguration(id, className, scope, propertyMap);
    }
}
