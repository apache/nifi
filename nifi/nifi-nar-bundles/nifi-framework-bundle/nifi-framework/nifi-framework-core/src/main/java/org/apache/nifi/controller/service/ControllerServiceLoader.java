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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.nifi.controller.FlowFromDOMFactory;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class ControllerServiceLoader {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceLoader.class);

    public static List<ControllerServiceNode> loadControllerServices(
            final ControllerServiceProvider provider,
            final InputStream serializedStream,
            final StringEncryptor encryptor,
            final BulletinRepository bulletinRepo,
            final boolean autoResumeState) throws IOException {
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);

        try (final InputStream in = new BufferedInputStream(serializedStream)) {
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

            final Document document = builder.parse(in);
            final Element controllerServices = document.getDocumentElement();
            final List<Element> serviceElements = DomUtils.getChildElementsByTagName(controllerServices, "controllerService");
            return new ArrayList<>(loadControllerServices(serviceElements, provider, encryptor, bulletinRepo, autoResumeState));
        } catch (SAXException | ParserConfigurationException sxe) {
            throw new IOException(sxe);
        }
    }

    public static Collection<ControllerServiceNode> loadControllerServices(
            final List<Element> serviceElements,
            final ControllerServiceProvider provider,
            final StringEncryptor encryptor,
            final BulletinRepository bulletinRepo,
            final boolean autoResumeState) {
        final Map<ControllerServiceNode, Element> nodeMap = new HashMap<>();
        for (final Element serviceElement : serviceElements) {
            final ControllerServiceNode serviceNode = createControllerService(provider, serviceElement, encryptor);
            // We need to clone the node because it will be used in a separate thread below, and
            // Element is not thread-safe.
            nodeMap.put(serviceNode, (Element) serviceElement.cloneNode(true));
        }
        for (final Map.Entry<ControllerServiceNode, Element> entry : nodeMap.entrySet()) {
            configureControllerService(entry.getKey(), entry.getValue(), encryptor);
        }

        // Start services
        if (autoResumeState) {
            final Set<ControllerServiceNode> nodesToEnable = new HashSet<>();

            for (final ControllerServiceNode node : nodeMap.keySet()) {
                final Element controllerServiceElement = nodeMap.get(node);

                final ControllerServiceDTO dto;
                synchronized (controllerServiceElement.getOwnerDocument()) {
                    dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor);
                }

                final ControllerServiceState state = ControllerServiceState.valueOf(dto.getState());
                if (state == ControllerServiceState.ENABLED) {
                    nodesToEnable.add(node);
                }
            }

            provider.enableControllerServices(nodesToEnable);
        }

        return nodeMap.keySet();
    }

    private static ControllerServiceNode createControllerService(final ControllerServiceProvider provider, final Element controllerServiceElement, final StringEncryptor encryptor) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor);

        final ControllerServiceNode node = provider.createControllerService(dto.getType(), dto.getId(), false);
        node.setName(dto.getName());
        node.setComments(dto.getComments());
        return node;
    }

    private static void configureControllerService(final ControllerServiceNode node, final Element controllerServiceElement, final StringEncryptor encryptor) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor);
        node.setAnnotationData(dto.getAnnotationData());

        for (final Map.Entry<String, String> entry : dto.getProperties().entrySet()) {
            if (entry.getValue() == null) {
                node.removeProperty(entry.getKey());
            } else {
                node.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }
}
