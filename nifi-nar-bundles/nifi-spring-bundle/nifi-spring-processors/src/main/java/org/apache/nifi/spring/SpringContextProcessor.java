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
package org.apache.nifi.spring;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.spring.SpringDataExchanger.SpringResponse;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;

/**
 * Implementation of {@link Processor} capable of sending and receiving data
 * from application defined in Spring Application context. It does so via
 * predefined in/out {@link MessageChannel}s (see spring-messaging module of
 * Spring). Once such channels are defined user is free to implement the rest of
 * the application any way they wish (e.g., custom code and/or using frameworks
 * such as Spring Integration or Camel).
 * <p>
 * The requirement and expectations for channel types are:
 * <ul>
 * <li>Input channel must be of type {@link MessageChannel} and named "fromNiFi"
 * (see {@link SpringNiFiConstants#FROM_NIFI})</li>
 * <li>Output channel must be of type {@link PollableChannel} and named "toNiFi"
 * (see {@link SpringNiFiConstants#TO_NIFI})</li>
 * </ul>
 * </p>
 * Below is the example of sample configuration:
 *
 * <pre>
 * &lt;?xml version="1.0" encoding="UTF-8"?&gt;
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *   xmlns:int="http://www.springframework.org/schema/integration"
 *  xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *      http://www.springframework.org/schema/integration http://www.springframework.org/schema/integration/spring-integration-4.2.xsd"&gt;
 *
 *  &lt;int:channel id="fromNiFi"/&gt;
 *
 *  . . . . .
 *
 *  &lt;int:channel id="toNiFi"&gt;
 *      &lt;int:queue/&gt;
 *  &lt;/int:channel&gt;
 *
 * &lt;/beans&gt;
 * </pre>
 * <p>
 * Defining {@link MessageChannel} is optional. That's why this processor
 * supports 3 modes of interaction with Spring Application Context:
 * <ul>
 * <li>Headless – no channels are defined therefore nothing is sent to or
 * received from such Application Contexts (i.e., some monitoring app).</li>
 * <li>One way (NiFi -&gt; Spring or Spring -&gt; NiFi) - depends on existence
 * of one of "fromNiFi" or "toNiFi" channel in the Spring Application Context.
 * </li>
 * <li>Bi-directional (NiFi -&gt; Spring -&gt; Nifi or Spring -&gt; NiFi -&gt;
 * Spring) - depends on existence of both "fromNiFi" and "toNiFi" channels in
 * the Spring Application Context</li>
 * </ul>
 *
 * </p>
 * <p>
 * To create an instance of the ApplicationConetxt this processor requires user
 * to provide configuration file path and the path to the resources that needs
 * to be added to the classpath of ApplicationContext. This essentially allows
 * user to package their Spring Application any way they want as long as
 * everything it requires is available on the classpath.
 * </p>
 * <p>
 * Data exchange between Spring and NiFi relies on simple mechanism which is
 * exposed via {@link SpringDataExchanger}; {@link FlowFile}s's content is
 * converted to primitive representation that can be easily wrapped in Spring
 * {@link Message}. The requirement imposed by this Processor is to send/receive
 * {@link Message} with <i>payload</i> of type <i>byte[]</i> and headers of type
 * <i>Map&lt;String, Object&gt;</i>. This is primarily for simplicity and type
 * safety. Converters and Transformers could be used by either side to change
 * representation of the content that is being exchanged between NiFi and
 * Spring.
 */
@TriggerWhenEmpty
@Tags({ "Spring", "Message", "Get", "Put", "Integration" })
@CapabilityDescription("A Processor that supports sending and receiving data from application defined in "
        + "Spring Application Context via predefined in/out MessageChannels.")
public class SpringContextProcessor extends AbstractProcessor {
    private final Logger logger = LoggerFactory.getLogger(SpringContextProcessor.class);

    public static final PropertyDescriptor CTX_CONFIG_PATH = new PropertyDescriptor.Builder()
            .name("Application Context config path")
            .description("The path to the Spring Application Context configuration file relative to the classpath")
            .required(true)
            .addValidator(new SpringContextConfigValidator())
            .build();
    public static final PropertyDescriptor CTX_LIB_PATH = new PropertyDescriptor.Builder()
            .name("Application Context class path")
            .description("Path to the directory with resources (i.e., JARs, configuration files etc.) required to be on "
                            + "the classpath of the ApplicationContext.")
            .addValidator(new SpringContextConfigValidator())
            .required(true)
            .build();
    public static final PropertyDescriptor SEND_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Send Timeout")
            .description("Timeout for sending data to Spring Application Context. Defaults to 0.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor RECEIVE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Receive Timeout")
            .description("Timeout for receiving date from Spring context. Defaults to 0.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    // ====

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description(
                    "All FlowFiles that are successfully received from Spring Application Context are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description(
                    "All FlowFiles that cannot be sent to Spring Application Context are routed to this relationship")
            .build();

    private final static Set<Relationship> relationships;

    private final static List<PropertyDescriptor> propertyDescriptors;

    // =======

    private volatile String applicationContextConfigFileName;

    private volatile String applicationContextLibPath;

    private volatile long sendTimeout;

    private volatile long receiveTimeout;

    private volatile SpringDataExchanger exchanger;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(CTX_CONFIG_PATH);
        _propertyDescriptors.add(CTX_LIB_PATH);
        _propertyDescriptors.add(SEND_TIMEOUT);
        _propertyDescriptors.add(RECEIVE_TIMEOUT);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     *
     */
    @OnScheduled
    public void initializeSpringContext(ProcessContext processContext) {
        this.applicationContextConfigFileName = processContext.getProperty(CTX_CONFIG_PATH).getValue();
        this.applicationContextLibPath = processContext.getProperty(CTX_LIB_PATH).getValue();

        String stStr = processContext.getProperty(SEND_TIMEOUT).getValue();
        this.sendTimeout = stStr == null ? 0 : FormatUtils.getTimeDuration(stStr, TimeUnit.MILLISECONDS);

        String rtStr = processContext.getProperty(RECEIVE_TIMEOUT).getValue();
        this.receiveTimeout = rtStr == null ? 0 : FormatUtils.getTimeDuration(rtStr, TimeUnit.MILLISECONDS);

        try {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Initializing Spring Application Context defined in " + this.applicationContextConfigFileName);
            }
            this.exchanger = SpringContextFactory.createSpringContextDelegate(this.applicationContextLibPath,
                    this.applicationContextConfigFileName);
        } catch (Exception e) {
            throw new IllegalStateException("Failed while initializing Spring Application Context", e);
        }
        if (logger.isInfoEnabled()) {
            logger.info("Successfully initialized Spring Application Context defined in "
                    + this.applicationContextConfigFileName);
        }
    }

    /**
     * Will close the 'exchanger' which in turn will close both Spring
     * Application Context and the ClassLoader that loaded it allowing new
     * instance of Spring Application Context to be created upon the next start
     * (which may have an updated classpath and functionality) without
     * restarting NiFi.
     */
    @OnStopped
    public void closeSpringContext(ProcessContext processContext) {
        if (this.exchanger != null) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Closing Spring Application Context defined in " + this.applicationContextConfigFileName);
                }
                this.exchanger.close();
                if (logger.isInfoEnabled()) {
                    logger.info("Successfully closed Spring Application Context defined in "
                            + this.applicationContextConfigFileName);
                }
            } catch (IOException e) {
                getLogger().warn("Failed while closing Spring Application Context", e);
            }
        }
    }

    /**
     *
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession processSession) throws ProcessException {
        FlowFile flowFileToProcess = processSession.get();
        if (flowFileToProcess != null) {
            this.sendToSpring(flowFileToProcess, context, processSession);
        }
        this.receiveFromSpring(processSession);
    }

    /**
     *
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    /**
     *
     */
    private void sendToSpring(FlowFile flowFileToProcess, ProcessContext context, ProcessSession processSession) {
        byte[] payload = this.extractMessage(flowFileToProcess, processSession);
        boolean sent = false;

        try {
            sent = this.exchanger.send(payload, flowFileToProcess.getAttributes(), this.sendTimeout);
            if (sent) {
                processSession.getProvenanceReporter().send(flowFileToProcess, this.applicationContextConfigFileName);
                processSession.remove(flowFileToProcess);
            } else {
                processSession.transfer(processSession.penalize(flowFileToProcess), REL_FAILURE);
                this.getLogger().error("Timed out while sending FlowFile to Spring Application Context "
                        + this.applicationContextConfigFileName);
                context.yield();
            }
        } catch (Exception e) {
            processSession.transfer(flowFileToProcess, REL_FAILURE);
            this.getLogger().error("Failed while sending FlowFile to Spring Application Context "
                    + this.applicationContextConfigFileName + "; " + e.getMessage(), e);
            context.yield();
        }
    }

    /**
     *
     */
    private void receiveFromSpring(ProcessSession processSession) {
        final SpringResponse<?> msgFromSpring = this.exchanger.receive(this.receiveTimeout);
        if (msgFromSpring != null) {
            FlowFile flowFileToProcess = processSession.create();
            flowFileToProcess = processSession.write(flowFileToProcess, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    Object payload = msgFromSpring.getPayload();
                    byte[] payloadBytes = payload instanceof String ? ((String) payload).getBytes() : (byte[]) payload;
                    out.write(payloadBytes);
                }
            });
            flowFileToProcess = processSession.putAllAttributes(flowFileToProcess,
                    this.extractFlowFileAttributesFromMessageHeaders(msgFromSpring.getHeaders()));
            processSession.transfer(flowFileToProcess, REL_SUCCESS);
            processSession.getProvenanceReporter().receive(flowFileToProcess, this.applicationContextConfigFileName);
        }
    }

    /**
     *
     */
    private Map<String, String> extractFlowFileAttributesFromMessageHeaders(Map<String, Object> messageHeaders) {
        Map<String, String> attributes = new HashMap<>();
        for (Entry<String, Object> entry : messageHeaders.entrySet()) {
            if (entry.getValue() instanceof String) {
                attributes.put(entry.getKey(), (String) entry.getValue());
            }
        }
        return attributes;
    }

    /**
     * Extracts contents of the {@link FlowFile} to byte array.
     */
    private byte[] extractMessage(FlowFile flowFile, ProcessSession processSession) {
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        processSession.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true);
            }
        });
        return messageContent;
    }

    /**
     *
     */
    static class SpringContextConfigValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            String configPath = context.getProperty(CTX_CONFIG_PATH).getValue();
            String libDirPath = context.getProperty(CTX_LIB_PATH).getValue();

            StringBuilder invalidationMessageBuilder = new StringBuilder();
            if (configPath != null && libDirPath != null) {
                validateClassPath(libDirPath, invalidationMessageBuilder);

                if (invalidationMessageBuilder.length() == 0 && !isConfigResolvable(configPath, new File(libDirPath))) {
                    invalidationMessageBuilder.append("'Application Context config path' can not be located "
                            + "in the provided classpath.");
                }
            } else if (StringUtils.isEmpty(configPath)) {
                invalidationMessageBuilder.append("'Application Context config path' must not be empty.");
            } else {
                if (StringUtils.isEmpty(libDirPath)) {
                    invalidationMessageBuilder.append("'Application Context class path' must not be empty.");
                } else {
                    validateClassPath(libDirPath, invalidationMessageBuilder);
                }
            }

            String invalidationMessage = invalidationMessageBuilder.toString();
            ValidationResult vResult = invalidationMessage.length() == 0
                    ? new ValidationResult.Builder().subject(subject).input(input)
                            .explanation("Spring configuration '" + configPath + "' is resolvable "
                                    + "against provided classpath '" + libDirPath + "'.").valid(true).build()
                    : new ValidationResult.Builder().subject(subject).input(input)
                            .explanation("Spring configuration '" + configPath + "' is NOT resolvable "
                                    + "against provided classpath '" + libDirPath + "'. Validation message: " + invalidationMessage).valid(false).build();

            return vResult;
        }
    }

    /**
     *
     */
    private static void validateClassPath(String libDirPath, StringBuilder invalidationMessageBuilder) {
        File libDirPathFile = new File(libDirPath);
        if (!libDirPathFile.exists()) {
            invalidationMessageBuilder.append(
                    "'Application Context class path' does not exist. Was '" + libDirPathFile.getAbsolutePath() + "'.");
        } else if (!libDirPathFile.isDirectory()) {
            invalidationMessageBuilder.append("'Application Context class path' must point to a directory. Was '"
                    + libDirPathFile.getAbsolutePath() + "'.");
        }
    }

    /**
     *
     */
    private static boolean isConfigResolvable(String configPath, File libDirPathFile) {
        List<URL> urls = new ArrayList<>();
        URLClassLoader parentLoader = (URLClassLoader) SpringContextProcessor.class.getClassLoader();
        urls.addAll(Arrays.asList(parentLoader.getURLs()));
        String[] resourceNames = libDirPathFile.list();
        try {
            for (String resourceName : resourceNames) {
                File r = new File(libDirPathFile, resourceName);
                if (!r.isDirectory() && !r.getName().startsWith(".")) {
                    URL url = new File(libDirPathFile, resourceName).toURI().toURL();
                    urls.add(url);
                }
            }
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
        boolean resolvable = false;
        try (URLClassLoader throwawayCl = new URLClassLoader(urls.toArray(new URL[] {}), null)) {
            resolvable = throwawayCl.findResource(configPath) != null;
        } catch (IOException e) {
            // ignore since it can only happen on CL.close()
        }
        return resolvable;
    }
}
