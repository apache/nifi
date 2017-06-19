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
package org.apache.nifi.bootstrap;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.bootstrap.notification.NotificationContext;
import org.apache.nifi.bootstrap.notification.NotificationInitializationContext;
import org.apache.nifi.bootstrap.notification.NotificationService;
import org.apache.nifi.bootstrap.notification.NotificationType;
import org.apache.nifi.bootstrap.notification.NotificationValidationContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.registry.VariableRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class NotificationServiceManager {
    private static final Logger logger = LoggerFactory.getLogger(NotificationServiceManager.class);
    private final Map<String, ConfiguredNotificationService> servicesById = new HashMap<>();
    private final Map<NotificationType, List<ConfiguredNotificationService>> servicesByNotificationType = new HashMap<>();

    private final ScheduledExecutorService notificationExecutor;
    private int maxAttempts = 5;
    private final VariableRegistry variableRegistry;


    public NotificationServiceManager() {
        this(VariableRegistry.ENVIRONMENT_SYSTEM_REGISTRY);
    }

    NotificationServiceManager(final VariableRegistry variableRegistry){
        this.variableRegistry = variableRegistry;
        notificationExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setName("Notification Service Dispatcher");
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void setMaxNotificationAttempts(final int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    /**
     * Loads the Notification Services from the given XML configuration file.
     *
     * File is expected to have the following format:
     *
     * <pre>
     * &lt;services&gt;
     *   &lt;service&gt;
     *     &lt;id&gt;service-identifier&lt;/id&gt;
     *     &lt;class&gt;org.apache.nifi.MyNotificationService&lt;/class&gt;
     *     &lt;property name="My First Property"&gt;Property Value&lt;/property&gt;
     *   &lt;/service&gt;
     *   &lt;service&gt;
     *     &lt;id&gt;other-service&lt;/id&gt;
     *     &lt;class&gt;org.apache.nifi.MyOtherNotificationService&lt;/class&gt;
     *     &lt;property name="Another Property"&gt;Property Value 2&lt;/property&gt;
     *   &lt;/service&gt;
     *   ...
     *   &lt;service&gt;
     *     &lt;id&gt;service-identifier-2&lt;/id&gt;
     *     &lt;class&gt;org.apache.nifi.FinalNotificationService&lt;/class&gt;
     *     &lt;property name="Yet Another Property"&gt;3rd Prop Value&lt;/property&gt;
     *   &lt;/service&gt;
     * &lt;/services&gt;
     * </pre>
     *
     * Note that as long as the file can be interpreted properly, a misconfigured service will result in a warning
     * or error being logged and the service will be unavailable but will not prevent the rest of the services from loading.
     *
     * @param servicesFile the XML file to load services from.
     * @throws IOException if unable to read from the given file
     * @throws ParserConfigurationException if unable to parse the given file as XML properly
     * @throws SAXException if unable to parse the given file properly
     */
    public void loadNotificationServices(final File servicesFile) throws IOException, ParserConfigurationException, SAXException {
        final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setNamespaceAware(false);
        final DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();

        final Map<String, ConfiguredNotificationService> serviceMap = new HashMap<>();
        try (final InputStream fis = new FileInputStream(servicesFile);
            final InputStream in = new BufferedInputStream(fis)) {

            final Document doc = docBuilder.parse(new InputSource(in));
            final List<Element> serviceElements = getChildElementsByTagName(doc.getDocumentElement(), "service");
            logger.debug("Found {} service elements", serviceElements.size());

            for (final Element serviceElement : serviceElements) {
                final ConfiguredNotificationService config = createService(serviceElement);
                final NotificationService service = config.getService();

                if (service == null) {
                    continue;  // reason will have already been logged, so just move on.
                }

                final String id = service.getIdentifier();
                if (serviceMap.containsKey(id)) {
                    logger.error("Found two different Notification Services configured with the same ID: '{}'. Loaded the first service.", id);
                    continue;
                }

                // Check if the service is valid; if not, warn now so that users know this before they fail to receive notifications
                final ValidationContext validationContext = new NotificationValidationContext(buildNotificationContext(config), variableRegistry);
                final Collection<ValidationResult> validationResults = service.validate(validationContext);
                final List<String> invalidReasons = new ArrayList<>();

                for (final ValidationResult result : validationResults) {
                    if (!result.isValid()) {
                        invalidReasons.add(result.toString());
                    }
                }

                if (!invalidReasons.isEmpty()) {
                    logger.warn("Configured Notification Service {} is not valid for the following reasons: {}", service, invalidReasons);
                }

                serviceMap.put(id, config);
            }
        }

        logger.info("Successfully loaded the following {} services: {}", serviceMap.size(), serviceMap.keySet());

        servicesById.clear();
        servicesById.putAll(serviceMap);
    }

    public void notify(final NotificationType type, final String subject, final String message) {
        final List<ConfiguredNotificationService> configs = servicesByNotificationType.get(type);
        if (configs == null || configs.isEmpty()) {
            return;
        }

        for (final ConfiguredNotificationService config : configs) {
            final NotificationService service = config.getService();
            final AtomicInteger attemptCount = new AtomicInteger(0);

            notificationExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    // Check if the service is valid; if not, warn now so that users know this before they fail to receive notifications
                    final ValidationContext validationContext = new NotificationValidationContext(buildNotificationContext(config), variableRegistry);
                    final Collection<ValidationResult> validationResults = service.validate(validationContext);
                    final List<String> invalidReasons = new ArrayList<>();

                    for (final ValidationResult result : validationResults) {
                        if (!result.isValid()) {
                            invalidReasons.add(result.toString());
                        }
                    }

                    // If the service is valid, attempt to send the notification
                    boolean failure = false;
                    if (invalidReasons.isEmpty()) {
                        final NotificationContext context = buildNotificationContext(config);
                        try {
                            service.notify(context, type, subject, message);
                            logger.info("Successfully sent notification of type {} to {}", type, service);
                        } catch (final Throwable t) {   // keep running even if a Throwable is caught because we need to ensure that we are able to restart NiFi
                            logger.error("Failed to send notification of type {} to {} with Subject {} due to {}. Will ",
                                type, service == null ? "Unknown Notification Service" : service.toString(), subject, t.toString());
                            logger.error("", t);
                            failure = true;
                        }
                    } else {
                        logger.warn("Notification Service {} is not valid for the following reasons: {}", service, invalidReasons);
                        failure = true;
                    }

                    final int attempts = attemptCount.incrementAndGet();
                    if (failure) {
                        if (attempts < maxAttempts) {
                            logger.info("After failing to send notification to {} {} times, will attempt again in 1 minute", service, attempts);
                            notificationExecutor.schedule(this, 1, TimeUnit.MINUTES);
                        } else {
                            logger.info("After failing to send notification of type {} to {} {} times, will no longer attempt to send notification", type, service, attempts);
                        }
                    }
                }
            });

            if (NotificationType.NIFI_STOPPED.equals(type)) {
                // If we are stopping NiFi, we want to block until we've tried to send the notification at least once before
                // we return. We do this because the executor used to run the task marks the threads as daemon, and on shutdown
                // we don't want to return before the notifier has had a chance to perform its task.
                while (attemptCount.get() == 0) {
                    try {
                        Thread.sleep(1000L);
                    } catch (final InterruptedException ie) {
                    }
                }
            }
        }
    }

    private NotificationContext buildNotificationContext(final ConfiguredNotificationService config) {
        return new NotificationContext() {
            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                final PropertyDescriptor fullPropDescriptor = config.getService().getPropertyDescriptor(descriptor.getName());
                if (fullPropDescriptor == null) {
                    return null;
                }

                String configuredValue = config.getProperties().get(fullPropDescriptor.getName());
                if (configuredValue == null) {
                    configuredValue = fullPropDescriptor.getDefaultValue();
                }

                return new StandardPropertyValue(configuredValue, null, variableRegistry);
            }

            @Override
            public Map<PropertyDescriptor, String> getProperties() {
                final Map<PropertyDescriptor, String> props = new HashMap<>();
                final Map<String, String> configuredProps = config.getProperties();

                final NotificationService service = config.getService();
                final List<PropertyDescriptor> configuredPropertyDescriptors = new ArrayList<>(service.getPropertyDescriptors());

                // This is needed to capture all dynamic properties
                configuredProps.forEach((key, value) -> {
                    PropertyDescriptor propertyDescriptor = config.service.getPropertyDescriptor(key);
                    props.put(config.service.getPropertyDescriptor(key), value);
                    configuredPropertyDescriptors.remove(propertyDescriptor);
                });

                for (final PropertyDescriptor descriptor : configuredPropertyDescriptors) {
                    props.put(descriptor, descriptor.getDefaultValue());
                }

                return props;
            }
        };
    }

    /**
     * Registers the service that has the given identifier to respond to notifications of the given type
     *
     * @param type the type of notification to register the service for
     * @param serviceId the identifier of the service
     */
    public void registerNotificationService(final NotificationType type, final String serviceId) {
        final ConfiguredNotificationService service = servicesById.get(serviceId);
        if (service == null) {
            throw new IllegalArgumentException("No Notification Service exists with ID " + serviceId);
        }

        List<ConfiguredNotificationService> services = servicesByNotificationType.get(type);
        if (services == null) {
            services = new ArrayList<>();
            servicesByNotificationType.put(type, services);
        }

        services.add(service);
    }

    /**
     * Creates a Notification Service and initializes it. Then returns the service and its configured properties
     *
     * @param serviceElement the XML element from which to build the Notification Service
     * @return a Tuple with the NotificationService as the key and the configured properties as the value, or <code>null</code> if
     *         unable to create the service
     */
    private ConfiguredNotificationService createService(final Element serviceElement) {
        final Element idElement = getChild(serviceElement, "id");
        if (idElement == null) {
            logger.error("Found configuration for Notification Service with no 'id' element; this service cannot be referenced so it will not be loaded");
            return null;
        }

        final String serviceId = idElement.getTextContent().trim();
        logger.debug("Loading Notification Service with ID {}", serviceId);

        final Element classElement = getChild(serviceElement, "class");
        if (classElement == null) {
            logger.error("Found configuration for Notification Service with no 'class' element; Service ID is '{}'. This service annot be loaded", serviceId);
            return null;
        }

        final String className = classElement.getTextContent().trim();
        final Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (final Exception e) {
            logger.error("Found configuration for Notification Service with ID '{}' and Class '{}' but could not load class.", serviceId, className);
            logger.error("", e);
            return null;
        }

        if (!NotificationService.class.isAssignableFrom(clazz)) {
            logger.error("Found configuration for Notification Service with ID '{}' and Class '{}' but class is not a Notification Service.", serviceId, className);
            return null;
        }

        final Object serviceObject;
        try {
            serviceObject = clazz.newInstance();
        } catch (final Exception e) {
            logger.error("Found configuration for Notification Service with ID '{}' and Class '{}' but could not instantiate Notification Service.", serviceId, className);
            logger.error("", e);
            return null;
        }

        final Map<String, String> propertyValues = new HashMap<>();
        final List<Element> propertyElements = getChildElementsByTagName(serviceElement, "property");
        for (final Element propertyElement : propertyElements) {
            final String propName = propertyElement.getAttribute("name");
            if (propName == null || propName.trim().isEmpty()) {
                logger.warn("Found configuration for Notification Service with ID '{}' that has property value configured but no name for the property.", serviceId);
                continue;
            }

            final String propValue = propertyElement.getTextContent().trim();
            propertyValues.put(propName, propValue);
        }

        final NotificationService service = (NotificationService) serviceObject;

        try {
            service.initialize(new NotificationInitializationContext() {
                @Override
                public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                    final String propName = descriptor.getName();
                    String value = propertyValues.get(propName);
                    if (value == null) {
                        value = descriptor.getDefaultValue();
                    }

                    return new StandardPropertyValue(value, null, variableRegistry);
                }

                @Override
                public Map<String,String> getAllProperties() {
                    return Collections.unmodifiableMap(propertyValues);
                }

                @Override
                public String getIdentifier() {
                    return serviceId;
                }
            });
        } catch (final Exception e) {
            logger.error("Failed to load Notification Service with ID '{}'", serviceId);
            logger.error("", e);
        }

        return new ConfiguredNotificationService(service, propertyValues);
    }

    public static Element getChild(final Element element, final String tagName) {
        final List<Element> children = getChildElementsByTagName(element, tagName);
        if (children.isEmpty()) {
            return null;
        }

        if (children.size() > 1) {
            return null;
        }

        return children.get(0);
    }

    public static List<Element> getChildElementsByTagName(final Element element, final String tagName) {
        final List<Element> matches = new ArrayList<>();
        final NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            final Node node = nodeList.item(i);
            if (!(node instanceof Element)) {
                continue;
            }

            final Element child = (Element) nodeList.item(i);
            if (child.getNodeName().equals(tagName)) {
                matches.add(child);
            }
        }

        return matches;
    }

    private static class ConfiguredNotificationService {
        private final NotificationService service;
        private final Map<String, String> properties;

        public ConfiguredNotificationService(final NotificationService service, final Map<String, String> properties) {
            this.service = service;
            this.properties = properties;
        }

        public NotificationService getService() {
            return service;
        }

        public Map<String, String> getProperties() {
            return properties;
        }
    }
}
