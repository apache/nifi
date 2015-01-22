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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.util.DomUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 *
 */
public class ControllerServiceLoader {

    private static final Log logger = LogFactory.getLog(ControllerServiceLoader.class);

    private final Path serviceConfigXmlPath;

    public ControllerServiceLoader(final Path serviceConfigXmlPath) throws IOException {
        final File serviceConfigXmlFile = serviceConfigXmlPath.toFile();
        if (!serviceConfigXmlFile.exists() || !serviceConfigXmlFile.canRead()) {
            throw new IOException(serviceConfigXmlPath + " does not appear to exist or cannot be read. Cannot load configuration.");
        }

        this.serviceConfigXmlPath = serviceConfigXmlPath;
    }

    public List<ControllerServiceNode> loadControllerServices(final ControllerServiceProvider provider) throws IOException {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        InputStream fis = null;
        BufferedInputStream bis = null;
        documentBuilderFactory.setNamespaceAware(true);

        final List<ControllerServiceNode> services = new ArrayList<>();

        try {
            final URL configurationResource = this.getClass().getResource("/ControllerServiceConfiguration.xsd");
            if (configurationResource == null) {
                throw new NullPointerException("Unable to load XML Schema for ControllerServiceConfiguration");
            }
            final Schema schema = schemaFactory.newSchema(configurationResource);
            documentBuilderFactory.setSchema(schema);
            final DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();

            builder.setErrorHandler(new org.xml.sax.ErrorHandler() {

                @Override
                public void fatalError(final SAXParseException err) throws SAXException {
                    logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.error("Error Stack Dump", err);
                    }
                    throw err;
                }

                @Override
                public void error(final SAXParseException err) throws SAXParseException {
                    logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.error("Error Stack Dump", err);
                    }
                    throw err;
                }

                @Override
                public void warning(final SAXParseException err) throws SAXParseException {
                    logger.warn(" Config file line " + err.getLineNumber() + ", uri " + err.getSystemId() + " : message : " + err.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.warn("Warning stack dump", err);
                    }
                    throw err;
                }
            });

            //if controllerService.xml does not exist, create an empty file...
            fis = Files.newInputStream(this.serviceConfigXmlPath, StandardOpenOption.READ);
            bis = new BufferedInputStream(fis);
            if (Files.size(this.serviceConfigXmlPath) > 0) {
                final Document document = builder.parse(bis);
                final NodeList servicesNodes = document.getElementsByTagName("services");
                final Element servicesElement = (Element) servicesNodes.item(0);

                final List<Element> serviceNodes = DomUtils.getChildElementsByTagName(servicesElement, "service");
                for (final Element serviceElement : serviceNodes) {
                    //get properties for the specific controller task - id, name, class,
                    //and schedulingPeriod must be set
                    final String serviceId = DomUtils.getChild(serviceElement, "identifier").getTextContent().trim();
                    final String serviceClass = DomUtils.getChild(serviceElement, "class").getTextContent().trim();

                    //set the class to be used for the configured controller task
                    final ControllerServiceNode serviceNode = provider.createControllerService(serviceClass, serviceId, false);

                    //optional task-specific properties
                    for (final Element optionalProperty : DomUtils.getChildElementsByTagName(serviceElement, "property")) {
                        final String name = optionalProperty.getAttribute("name").trim();
                        final String value = optionalProperty.getTextContent().trim();
                        serviceNode.setProperty(name, value);
                    }

                    services.add(serviceNode);
                    serviceNode.setDisabled(false);
                }
            }
        } catch (SAXException | ParserConfigurationException sxe) {
            throw new IOException(sxe);
        } finally {
            FileUtils.closeQuietly(fis);
            FileUtils.closeQuietly(bis);
        }

        return services;
    }
}
