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
package org.apache.nifi.registry.service.alias;

import org.apache.nifi.registry.flow.VersionedFlowCoordinates;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.provider.ProviderFactoryException;
import org.apache.nifi.registry.provider.StandardProviderFactory;
import org.apache.nifi.registry.security.util.XmlUtils;
import org.apache.nifi.registry.url.aliaser.generated.Alias;
import org.apache.nifi.registry.url.aliaser.generated.Aliases;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Allows aliasing of registry url(s) without modifying the flows on disk.
 */
@Service
public class RegistryUrlAliasService {
    private static final String ALIASES_XSD = "/aliases.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.registry.url.aliaser.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, RegistryUrlAliasService.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.", e);
        }
    }

    // Will be LinkedHashMap to preserve insertion order.
    private final Map<String, String> aliases;

    @Autowired
    public RegistryUrlAliasService(NiFiRegistryProperties niFiRegistryProperties) {
        this(createAliases(niFiRegistryProperties));
    }

    private static List<Alias> createAliases(NiFiRegistryProperties niFiRegistryProperties) {
        File configurationFile = niFiRegistryProperties.getRegistryAliasConfigurationFile();
        if (configurationFile.exists()) {
            try {
                // find the schema
                final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                final Schema schema = schemaFactory.newSchema(StandardProviderFactory.class.getResource(ALIASES_XSD));

                // attempt to unmarshal
                final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);

                final JAXBElement<Aliases> element = unmarshaller.unmarshal(XmlUtils.createSafeReader(new StreamSource(configurationFile)), Aliases.class);
                return element.getValue().getAlias();
            } catch (SAXException | JAXBException | XMLStreamException e) {
                throw new ProviderFactoryException("Unable to load the registry alias configuration file at: " + configurationFile.getAbsolutePath(), e);
            }
        } else {
            return Collections.emptyList();
        }
    }

    protected RegistryUrlAliasService(List<Alias> aliases) {
        Pattern urlStart = Pattern.compile("^https?://");

        this.aliases = new LinkedHashMap<>();

        for (Alias alias : aliases) {
            String internal = alias.getInternal();
            String external = alias.getExternal();

            if (!urlStart.matcher(external).find()) {
                throw new IllegalArgumentException("Expected " + external + " to start with http:// or https://");
            }

            if (this.aliases.put(internal, external) != null) {
                throw new IllegalArgumentException("Duplicate internal token " + internal);
            }
        }
    }

    /**
     * Recursively replaces the aliases with the external url for a process group and children.
     */
    public void setExternal(VersionedProcessGroup processGroup) {
        processGroup.getProcessGroups().forEach(this::setExternal);

        VersionedFlowCoordinates coordinates = processGroup.getVersionedFlowCoordinates();
        if (coordinates != null) {
            coordinates.setRegistryUrl(getExternal(coordinates.getRegistryUrl()));
        }
    }

    /**
     * Recursively replaces the external url with the aliases for a process group and children.
     */
    public void setInternal(VersionedProcessGroup processGroup) {
        processGroup.getProcessGroups().forEach(this::setInternal);

        VersionedFlowCoordinates coordinates = processGroup.getVersionedFlowCoordinates();
        if (coordinates != null) {
            coordinates.setRegistryUrl(getInternal(coordinates.getRegistryUrl()));
        }
    }

    protected String getExternal(String url) {
        for (Map.Entry<String, String> alias : aliases.entrySet()) {
            String internal = alias.getKey();
            String external = alias.getValue();

            if (url.startsWith(internal)) {
                int internalLength = internal.length();
                if (url.length() == internalLength) {
                    return external;
                }
                return external + url.substring(internalLength);
            }
        }
        return url;
    }

    protected String getInternal(String url) {
        for (Map.Entry<String, String> alias : aliases.entrySet()) {
            String internal = alias.getKey();
            String external = alias.getValue();

            if (url.startsWith(external)) {
                int externalLength = external.length();
                if (url.length() == externalLength) {
                    return internal;
                }
                return internal + url.substring(externalLength);
            }
        }
        return url;
    }
}
