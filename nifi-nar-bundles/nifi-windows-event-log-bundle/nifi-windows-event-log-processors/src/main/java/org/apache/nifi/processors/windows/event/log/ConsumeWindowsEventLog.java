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

package org.apache.nifi.processors.windows.event.log;

import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.Kernel32Util;
import com.sun.jna.platform.win32.WinNT;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.windows.event.log.jna.ErrorLookup;
import org.apache.nifi.processors.windows.event.log.jna.EventSubscribeXmlRenderingCallback;
import org.apache.nifi.processors.windows.event.log.jna.WEvtApi;


@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"ingest", "event", "windows"})
@TriggerSerially
@CapabilityDescription("Registers a Windows Event Log Subscribe Callback to receive FlowFiles from Events on Windows.  These can be filtered via channel and XPath.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Will set a MIME type value of application/xml.")
})
public class ConsumeWindowsEventLog extends AbstractSessionFactoryProcessor {
    public static final String DEFAULT_CHANNEL = "System";
    public static final String DEFAULT_XPATH = "*";
    public static final int DEFAULT_MAX_BUFFER = 1024 * 1024;
    public static final int DEFAULT_MAX_QUEUE_SIZE = 1024;

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor.Builder()
            .name("channel")
            .displayName("Channel")
            .required(true)
            .defaultValue(DEFAULT_CHANNEL)
            .description("The Windows Event Log Channel to listen to.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("query")
            .displayName("XPath Query")
            .required(true)
            .defaultValue(DEFAULT_XPATH)
            .description("XPath Query to filter events. (See https://msdn.microsoft.com/en-us/library/windows/desktop/dd996910(v=vs.85).aspx for examples.)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("maxBuffer")
            .displayName("Maximum Buffer Size")
            .required(true)
            .defaultValue(Integer.toString(DEFAULT_MAX_BUFFER))
            .description("The individual Event Log XMLs are rendered to a buffer." +
                    "  This specifies the maximum size in bytes that the buffer will be allowed to grow to. (Limiting the maximum size of an individual Event XML.)")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_EVENT_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("maxQueue")
            .displayName("Maximum queue size")
            .required(true)
            .defaultValue(Integer.toString(DEFAULT_MAX_QUEUE_SIZE))
            .description("Events are received asynchronously and must be output as FlowFiles when the processor is triggered." +
                    "  This specifies the maximum number of events to queue for transformation into FlowFiles.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor INACTIVE_DURATION_TO_RECONNECT = new PropertyDescriptor.Builder()
            .name("inactiveDurationToReconnect")
            .displayName("Inactive duration to reconnect")
            .description("If no new event logs are processed for the specified time period," +
                    " this processor will try reconnecting to recover from a state where any further messages cannot be consumed." +
                    " Such situation can happen if Windows Event Log service is restarted, or ERROR_EVT_QUERY_RESULT_STALE (15011) is returned." +
                    " Setting no duration, e.g. '0 ms' disables auto-reconnection.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("10 mins")
            .addValidator(StandardValidators.createTimePeriodValidator(0, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.MILLISECONDS))
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(
            Arrays.asList(CHANNEL, QUERY, MAX_BUFFER_SIZE, MAX_EVENT_QUEUE_SIZE, INACTIVE_DURATION_TO_RECONNECT));

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully consumed events.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS)));
    public static final String APPLICATION_XML = "application/xml";
    public static final String UNABLE_TO_SUBSCRIBE = "Unable to subscribe with provided parameters, received the following error code: ";
    public static final String PROCESSOR_ALREADY_SUBSCRIBED = "Processor already subscribed to Event Log, expected cleanup to unsubscribe.";

    private final WEvtApi wEvtApi;
    private final Kernel32 kernel32;
    private final ErrorLookup errorLookup;
    private final String name;

    private Throwable wEvtApiError = null;
    private Throwable kernel32Error = null;

    private BlockingQueue<String> renderedXMLs;
    private WEvtApi.EVT_SUBSCRIBE_CALLBACK evtSubscribeCallback;
    private WinNT.HANDLE subscriptionHandle;
    private ProcessSessionFactory sessionFactory;
    private String provenanceUri;

    private long inactiveDurationToReconnect = 0;
    private long lastActivityTimestamp = 0;

    /**
     * Framework constructor
     */
    public ConsumeWindowsEventLog() {
        this(null, null);
    }

    /**
     * Constructor that allows injection of JNA interfaces
     *
     * @param wEvtApi  event api interface
     * @param kernel32 kernel interface
     */
    public ConsumeWindowsEventLog(WEvtApi wEvtApi, Kernel32 kernel32) {
        this.wEvtApi = wEvtApi == null ? loadWEvtApi() : wEvtApi;
        this.kernel32 = kernel32 == null ? loadKernel32() : kernel32;
        this.errorLookup = new ErrorLookup(this.kernel32);
        if (this.kernel32 != null) {
            name = Kernel32Util.getComputerName();
        } else {
            // Won't be able to use the processor anyway because native libraries didn't load
            name = null;
        }
    }

    private WEvtApi loadWEvtApi() {
        try {
            return WEvtApi.INSTANCE;
        } catch (Throwable e) {
            wEvtApiError = e;
            return null;
        }
    }

    private Kernel32 loadKernel32() {
        try {
            return Kernel32.INSTANCE;
        } catch (Throwable e) {
            kernel32Error = e;
            return null;
        }
    }

    /**
     * Register subscriber via native call
     *
     * @param context the process context
     */
    private String subscribe(ProcessContext context) {
        final String channel = context.getProperty(CHANNEL).evaluateAttributeExpressions().getValue();
        final String query = context.getProperty(QUERY).evaluateAttributeExpressions().getValue();

        renderedXMLs = new LinkedBlockingQueue<>(context.getProperty(MAX_EVENT_QUEUE_SIZE).asInteger());

        try {
            provenanceUri = new URI("winlog", name, "/" + channel, query, null).toASCIIString();
        } catch (URISyntaxException e) {
            getLogger().debug("Failed to construct detailed provenanceUri from channel={}, query={}, use simpler one.", new Object[]{channel, query});
            provenanceUri = String.format("winlog://%s/%s", name, channel);
        }

        inactiveDurationToReconnect = context.getProperty(INACTIVE_DURATION_TO_RECONNECT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        evtSubscribeCallback = new EventSubscribeXmlRenderingCallback(getLogger(), s -> {
            try {
                renderedXMLs.put(s);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Got interrupted while waiting to add to queue.", e);
            }
        }, context.getProperty(MAX_BUFFER_SIZE).asInteger(), wEvtApi, kernel32, errorLookup);

        subscriptionHandle = wEvtApi.EvtSubscribe(null, null, channel, query, null, null,
                evtSubscribeCallback, WEvtApi.EvtSubscribeFlags.SUBSCRIBE_TO_FUTURE | WEvtApi.EvtSubscribeFlags.EVT_SUBSCRIBE_STRICT);

        if (!isSubscribed()) {
            return UNABLE_TO_SUBSCRIBE + errorLookup.getLastError();
        }

        lastActivityTimestamp = System.currentTimeMillis();
        return null;
    }

    private boolean isSubscribed() {
        return subscriptionHandle != null && subscriptionHandle.getPointer() != null;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws AlreadySubscribedException {
        if (isSubscribed()) {
            throw new AlreadySubscribedException(PROCESSOR_ALREADY_SUBSCRIBED);
        }
        String errorMessage = subscribe(context);
        if (errorMessage != null) {
            getLogger().error(errorMessage);
        }
    }

    /**
     * Cleanup
     */
    @OnStopped
    public void stop() {
        unsubscribe();

        if (!renderedXMLs.isEmpty()) {
            if (sessionFactory != null) {
                getLogger().info("Finishing processing leftover events");
                ProcessSession session = sessionFactory.createSession();
                processQueue(session);
            } else {
                throw new ProcessException("Stopping the processor but there is no ProcessSessionFactory stored and there are messages in the internal queue. Removing the processor now will " +
                        "clear the queue but will result in DATA LOSS. This is normally due to starting the processor, receiving events and stopping before the onTrigger happens. The messages " +
                        "in the internal queue cannot finish processing until until the processor is triggered to run.");
            }
        }
        sessionFactory = null;
        provenanceUri = null;
        renderedXMLs = null;
    }

    private void unsubscribe() {
        if (isSubscribed()) {
            wEvtApi.EvtClose(subscriptionHandle);
        }
        subscriptionHandle = null;
        evtSubscribeCallback = null;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        this.sessionFactory = sessionFactory;

        if (!isSubscribed()) {
            String errorMessage = subscribe(context);
            if (errorMessage != null) {
                context.yield();
                getLogger().error(errorMessage);
                return;
            }
        }

        final int flowFileCount = processQueue(sessionFactory.createSession());

        final long now = System.currentTimeMillis();
        if (flowFileCount > 0) {
            lastActivityTimestamp = now;

        } else if (inactiveDurationToReconnect > 0) {
            if ((now - lastActivityTimestamp) > inactiveDurationToReconnect) {
                getLogger().info("Exceeds configured 'inactive duration to reconnect' {} ms. Unsubscribe to reconnect..", new Object[]{inactiveDurationToReconnect});
                unsubscribe();
            }
        }
    }

    /**
     * Create FlowFiles from received logs.
     * @return the number of created FlowFiles
     */
    private int processQueue(ProcessSession session) {
        String xml;
        int flowFileCount = 0;

        while ((xml = renderedXMLs.peek()) != null) {
            FlowFile flowFile = session.create();
            byte[] xmlBytes = xml.getBytes(StandardCharsets.UTF_8);
            flowFile = session.write(flowFile, out -> out.write(xmlBytes));
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), APPLICATION_XML);
            session.getProvenanceReporter().receive(flowFile, provenanceUri);
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
            flowFileCount++;
            if (!renderedXMLs.remove(xml) && getLogger().isWarnEnabled()) {
                getLogger().warn(new StringBuilder("Event ")
                        .append(xml)
                        .append(" had already been removed from queue, FlowFile ")
                        .append(flowFile.getAttribute(CoreAttributes.UUID.key()))
                        .append(" possible duplication of data")
                        .toString());
            }
        }
        return flowFileCount;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        // We need to check to see if the native libraries loaded properly
        List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));
        if (wEvtApiError != null) {
            validationResults.add(new ValidationResult.Builder().valid(false).subject("System Configuration")
                    .explanation("NiFi failed to load wevtapi on this system.  This processor utilizes native Windows APIs and will only work on Windows. ("
                            + wEvtApiError.getMessage() + ")").build());
        }
        if (kernel32Error != null) {
            validationResults.add(new ValidationResult.Builder().valid(false).subject("System Configuration")
                    .explanation("NiFi failed to load kernel32 on this system.  This processor utilizes native Windows APIs and will only work on Windows. ("
                            + kernel32Error.getMessage() + ")").build());
        }
        return validationResults;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }
}
