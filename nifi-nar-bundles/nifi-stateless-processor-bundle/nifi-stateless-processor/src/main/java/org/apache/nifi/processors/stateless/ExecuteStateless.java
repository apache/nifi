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

package org.apache.nifi.processors.stateless;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.SystemResourceConsiderations;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.stateless.retrieval.CachingDataflowProvider;
import org.apache.nifi.processors.stateless.retrieval.DataflowProvider;
import org.apache.nifi.processors.stateless.retrieval.FileSystemDataflowProvider;
import org.apache.nifi.processors.stateless.retrieval.RegistryDataflowProvider;
import org.apache.nifi.registry.VersionedFlowConverter;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stateless.bootstrap.StatelessBootstrap;
import org.apache.nifi.stateless.config.ExtensionClientDefinition;
import org.apache.nifi.stateless.config.ParameterContextDefinition;
import org.apache.nifi.stateless.config.ParameterValueProviderDefinition;
import org.apache.nifi.stateless.config.ReportingTaskDefinition;
import org.apache.nifi.stateless.config.SslContextDefinition;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.DataflowTriggerContext;
import org.apache.nifi.stateless.flow.FailurePortEncounteredException;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TransactionThresholds;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;
import static org.apache.nifi.processor.util.StandardValidators.DATA_SIZE_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.POSITIVE_INTEGER_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.TIME_PERIOD_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.URL_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.createDirectoryExistsValidator;

@Restricted
@SupportsBatching
@SystemResourceConsiderations({
    @SystemResourceConsideration(resource= SystemResource.CPU),
    @SystemResourceConsideration(resource= SystemResource.DISK),
    @SystemResourceConsideration(resource= SystemResource.MEMORY),
    @SystemResourceConsideration(resource= SystemResource.NETWORK)
})
@DynamicProperty(name="Any Parameter name", value="Any value", description = "Any dynamic property that is added will be provided to the stateless flow as a Parameter. The name of the property will" +
    " be the name of the Parameter, and the value of the property will be the value of the Parameter. Because Parameter values may or may not be sensitive, all dynamic properties will be considered" +
    " sensitive in order to protect their integrity.")
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Runs the configured dataflow using the Stateless NiFi engine. Please see documentation in order to understand the differences between the traditional NiFi runtime engine and" +
    " the Stateless NiFi engine. If the Processor is configured with an incoming connection, the incoming FlowFiles will be queued up into the specified Input Port in the dataflow. Data that is" +
    " transferred out of the flow via an Output Port will be sent to the 'output' relationship, and an attribute will be added to indicate which Port that FlowFile was transferred to. See" +
    " Additional Details for more information.")
@WritesAttributes({
    @WritesAttribute(attribute="output.port.name", description = "The name of the Output Port that the FlowFile was transferred to"),
    @WritesAttribute(attribute="failure.port.name", description = "If one or more FlowFiles is routed to one of the Output Ports that is configured as a Failure Port, the input FlowFile (if any) " +
        "will have this attribute added to it, indicating the name of the Port that caused the dataflow to be considered a failure.")
})
public class ExecuteStateless extends AbstractProcessor implements Searchable {
    public static final AllowableValue SPEC_FROM_FILE = new AllowableValue("Use Local File", "Use Local File or URL",
        "Dataflow to run is stored as a file on the NiFi server or at a URL that is accessible to the NiFi server");
    public static final AllowableValue SPEC_FROM_REGISTRY = new AllowableValue("Use NiFi Registry", "Use NiFi Registry", "Dataflow to run is stored in NiFi Registry");

    public static final AllowableValue CONTENT_STORAGE_HEAP = new AllowableValue("Store Content on Heap", "Store Content on Heap",
        "The FlowFile content will be stored on the NiFi JVM's heap. This is the most " +
        "efficient option for small FlowFiles but can quickly exhaust the heap with larger FlowFiles, resulting in Out Of Memory Errors and node instability.");
    public static final AllowableValue CONTENT_STORAGE_DISK = new AllowableValue("Store Content on Disk", "Store Content on Disk",
        "The FlowFile content will be stored on disk, within the configured Work Directory. The content will still be cleared between invocations and will not be persisted across restarts.");

    public static final PropertyDescriptor DATAFLOW_SPECIFICATION_STRATEGY = new Builder()
        .name("Dataflow Specification Strategy")
        .displayName("Dataflow Specification Strategy")
        .description("Specifies how the Processor should obtain a copy of the dataflow that it is to run")
        .required(true)
        .allowableValues(SPEC_FROM_FILE, SPEC_FROM_REGISTRY)
        .defaultValue(SPEC_FROM_FILE.getValue())
        .build();

    public static final PropertyDescriptor DATAFLOW_FILE = new Builder()
        .name("Dataflow File")
        .displayName("Dataflow File/URL")
        .description("The filename or URL that specifies the dataflow that is to be run")
        .required(true)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL)
        .dependsOn(DATAFLOW_SPECIFICATION_STRATEGY, SPEC_FROM_FILE)
        .build();

    public static final PropertyDescriptor REGISTRY_URL = new Builder()
        .name("Registry URL")
        .displayName("Registry URL")
        .description("The URL of the NiFi Registry to retrieve the flow from")
        .required(true)
        .addValidator(URL_VALIDATOR)
        .dependsOn(DATAFLOW_SPECIFICATION_STRATEGY, SPEC_FROM_REGISTRY)
        .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new Builder()
        .name("Registry SSL Context Service")
        .displayName("Registry SSL Context Service")
        .description("The SSL Context Service to use for interacting with the NiFi Registry")
        .required(false)
        .identifiesControllerService(SSLContextService.class)
        .dependsOn(DATAFLOW_SPECIFICATION_STRATEGY, SPEC_FROM_REGISTRY)
        .build();

    public static final PropertyDescriptor COMMS_TIMEOUT = new Builder()
        .name("Communications Timeout")
        .displayName("Communications Timeout")
        .description("Specifies how long to wait before timing out when attempting to communicate with NiFi Registry")
        .required(true)
        .addValidator(TIME_PERIOD_VALIDATOR)
        .dependsOn(DATAFLOW_SPECIFICATION_STRATEGY, SPEC_FROM_REGISTRY)
        .defaultValue("15 secs")
        .build();

    public static final PropertyDescriptor BUCKET = new Builder()
        .name("Registry Bucket")
        .displayName("Registry Bucket")
        .description("The name of the Bucket in the NiFi Registry that the flow should retrieved from")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .dependsOn(DATAFLOW_SPECIFICATION_STRATEGY, SPEC_FROM_REGISTRY)
        .build();

    public static final PropertyDescriptor FLOW_NAME = new Builder()
        .name("Flow Name")
        .displayName("Flow Name")
        .description("The name of the flow in the NiFi Registry")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .dependsOn(DATAFLOW_SPECIFICATION_STRATEGY, SPEC_FROM_REGISTRY)
        .build();

    public static final PropertyDescriptor FLOW_VERSION = new Builder()
        .name("Flow Version")
        .displayName("Flow Version")
        .description("The version of the flow in the NiFi Registry that should be retrieved. If not specified, the latest version will always be used.")
        .required(false)
        .addValidator(POSITIVE_INTEGER_VALIDATOR)
        .dependsOn(DATAFLOW_SPECIFICATION_STRATEGY, SPEC_FROM_REGISTRY)
        .build();

    public static final PropertyDescriptor INPUT_PORT = new Builder()
        .name("Input Port")
        .displayName("Input Port")
        .description("Specifies the name of the Input Port to send incoming FlowFiles to. This property is required if this processor has any incoming connections.")
        .required(false)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor FAILURE_PORTS = new Builder()
        .name("Failure Ports")
        .displayName("Failure Ports")
        .description("A comma-separated list of the names of Output Ports that exist at the root level of the dataflow. If any FlowFile is routed to one of the Ports whose name is listed here, the " +
            "dataflow will be considered a failure, and the incoming FlowFile (if any) will be routed to 'failure'. If not specified, all Output Ports will be considered successful.")
        .required(false)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .build();

    public static final PropertyDescriptor DATAFLOW_TIMEOUT = new Builder()
        .name("Dataflow Timeout")
        .displayName("Dataflow Timeout")
        .description("If the flow does not complete within this amount of time, the incoming FlowFile, if any, will be routed to the timeout relationship," +
            "the dataflow will be cancelled, and the invocation will end.")
        .required(true)
        .addValidator(TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .defaultValue("60 sec")
        .build();

    public static final PropertyDescriptor LIB_DIRECTORY = new Builder()
        .name("NAR Directory")
        .displayName("NAR Directory")
        .description("The directory to retrieve NAR's from")
        .required(true)
        .addValidator(createDirectoryExistsValidator(false, false))
        .defaultValue("./lib")
        .build();

    public static final PropertyDescriptor WORKING_DIRECTORY = new Builder()
        .name("Work Directory")
        .displayName("Work Directory")
        .description("A directory that can be used to create temporary files, such as expanding NAR files, temporary FlowFile content, caching the dataflow, etc.")
        .required(true)
        .addValidator(createDirectoryExistsValidator(false, true))
        .defaultValue("./work")
        .build();

    public static final PropertyDescriptor KRB5_CONF = new Builder()
        .name("Krb5 Conf File")
        .displayName("Krb5 Conf File")
        .description("The KRB5 Conf file to use for configuring components that rely on Kerberos")
        .required(false)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
        .build();

    public static final PropertyDescriptor STATELESS_SSL_CONTEXT_SERVICE = new Builder()
        .name("Stateless SSL Context Service")
        .displayName("Stateless SSL Context Service")
        .description("The SSL Context to use as the Stateless System SSL Context")
        .required(false)
        .identifiesControllerService(SSLContextService.class)
        .build();

    public static final PropertyDescriptor MAX_INGEST_FLOWFILES = new Builder()
        .name("Max Ingest FlowFiles")
        .displayName("Max Ingest FlowFiles")
        .description("During the course of a stateless dataflow, some processors may require more data than they have available in order to proceed. For example, MergeContent may require a minimum " +
            "number of FlowFiles before it can proceed. In this case, the dataflow may bring in additional data from its source Processor. However, this data may all be held in memory, so this " +
            "property provides a mechanism for limiting the maximum number of FlowFiles that the source Processor can ingest before it will no longer be triggered to ingest additional data.")
        .required(false)
        .addValidator(POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .build();

    public static final PropertyDescriptor MAX_INGEST_DATA_SIZE = new Builder()
        .name("Max Ingest Data Size")
        .displayName("Max Ingest Data Size")
        .description("During the course of a stateless dataflow, some processors may require more data than they have available in order to proceed. For example, MergeContent may require a minimum " +
            "number of FlowFiles before it can proceed. In this case, the dataflow may bring in additional data from its source Processor. However, this data may all be held in memory, so this " +
            "property provides a mechanism for limiting the maximum amount of data that the source Processor can ingest before it will no longer be triggered to ingest additional data.")
        .required(false)
        .addValidator(DATA_SIZE_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .build();

    public static final PropertyDescriptor CONTENT_STORAGE_STRATEGY = new Builder()
        .name("Content Storage Strategy")
        .displayName("Content Storage Strategy")
        .description("Specifies where the content of FlowFiles that the Stateless dataflow is operating on should be stored. Note that the data is always considered temporary and may be deleted at " +
            "any time. It is not intended to be persisted across restarted.")
        .required(true)
        .allowableValues(CONTENT_STORAGE_HEAP, CONTENT_STORAGE_DISK)
        .defaultValue(CONTENT_STORAGE_DISK.getValue())
        .build();

    public static final PropertyDescriptor MAX_INPUT_FLOWFILE_SIZE = new Builder()
        .name("Max Input FlowFile Size")
        .displayName("Max Input FlowFile Size")
        .description("This Processor is configured to load all incoming FlowFiles into memory. Because of that, it is important to limit the maximum size of " +
            "any incoming FlowFile that would get loaded into memory, in order to prevent Out Of Memory Errors and excessive Garbage Collection. Any FlowFile whose content " +
            "size is greater than the configured size will be routed to failure and not sent to the Stateless Engine.")
        .required(true)
        .dependsOn(CONTENT_STORAGE_STRATEGY, CONTENT_STORAGE_HEAP)
        .addValidator(DATA_SIZE_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .defaultValue("1 MB")
        .build();

    public static final PropertyDescriptor STATUS_TASK_INTERVAL = new Builder()
            .name("Status Task Interval")
            .displayName("Status Task Interval")
            .description("The Stateless engine periodically logs the status of the dataflow's processors.  This property allows the interval to be changed, or the status logging " +
                    "to be skipped altogether if the property is not set.")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(10, TimeUnit.SECONDS, 24, TimeUnit.HOURS))
            .expressionLanguageSupported(NONE)
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("For any incoming FlowFile that is successfully processed, the original incoming FlowFile will be transferred to this Relationship")
        .autoTerminateDefault(true)
        .build();
    static final Relationship REL_OUTPUT = new Relationship.Builder()
        .name("output")
        .description("Any FlowFiles that are transferred to an Output Port in the configured dataflow will be routed to this Relationship")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If the dataflow fails to process an incoming FlowFile, that FlowFile will be routed to this relationship")
        .build();
    static final Relationship REL_TIMEOUT = new Relationship.Builder()
        .name("timeout")
        .description("If the dataflow fails to complete in the configured amount of time, any incoming FlowFile will be routed to this relationship")
        .build();


    private final BlockingQueue<StatelessDataflow> dataflows = new LinkedBlockingDeque<>();
    private final AtomicInteger dataflowCreationCount = new AtomicInteger(0);
    private volatile Set<String> failurePortNames;
    private volatile VersionedFlowSnapshot flowSnapshot;
    private volatile AbortableTriggerContext triggerContext;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(
            DATAFLOW_SPECIFICATION_STRATEGY,
            DATAFLOW_FILE,
            REGISTRY_URL,
            SSL_CONTEXT_SERVICE,
            COMMS_TIMEOUT,
            BUCKET,
            FLOW_NAME,
            FLOW_VERSION,
            INPUT_PORT,
            FAILURE_PORTS,
            CONTENT_STORAGE_STRATEGY,
            MAX_INPUT_FLOWFILE_SIZE,
            DATAFLOW_TIMEOUT,
            LIB_DIRECTORY,
            WORKING_DIRECTORY,
            MAX_INGEST_FLOWFILES,
            MAX_INGEST_DATA_SIZE,
            STATELESS_SSL_CONTEXT_SERVICE,
            KRB5_CONF,
                STATUS_TASK_INTERVAL);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(REL_ORIGINAL, REL_OUTPUT, REL_FAILURE, REL_TIMEOUT));
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new Builder()
            .name(propertyDescriptorName)
            .defaultValue("Value for the " + propertyDescriptorName + " parameter")
            .addValidator(Validator.VALID)
            .sensitive(true)
            .dynamic(true)
            .build();
    }


    @OnScheduled
    public void parseDataflow(final ProcessContext context) throws IOException {
        final String specificationStrategy = context.getProperty(DATAFLOW_SPECIFICATION_STRATEGY).getValue();

        final DataflowProvider rawRetrieval;
        if (specificationStrategy.equalsIgnoreCase(SPEC_FROM_FILE.getValue())) {
            rawRetrieval = new FileSystemDataflowProvider();
        } else {
            rawRetrieval = new RegistryDataflowProvider(getLogger());
        }

        final DataflowProvider cachedRetrieval = new CachingDataflowProvider(getIdentifier(), getLogger(), rawRetrieval);

        final long start = System.nanoTime();
        final VersionedFlowSnapshot versionedFlowSnapshot = cachedRetrieval.retrieveDataflowContents(context);
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        getLogger().info("Successfully retrieved flow in {} millis", millis);
        this.flowSnapshot = versionedFlowSnapshot;
        this.triggerContext = new AbortableTriggerContext();

        final Set<String> failurePorts = new HashSet<>();
        final String failurePortNames = context.getProperty(FAILURE_PORTS).getValue();
        if (failurePortNames != null) {
            for (final String portName : failurePortNames.split(",")) {
                failurePorts.add(portName.trim());
            }
        }

        this.failurePortNames = failurePorts;
    }

    @OnUnscheduled
    public void abortDataflow() {
        if (triggerContext != null) {
            triggerContext.abort();
        }
    }

    @OnStopped
    public void shutdown() {
        StatelessDataflow dataflow;
        while ((dataflow = dataflows.poll()) != null) {
            dataflow.shutdown();
        }

        dataflows.clear();
        dataflowCreationCount.set(0);
    }

    private StatelessDataflow createDataflow(final ProcessContext context) throws IOException, StatelessConfigurationException {
        final int dataflowIndex = dataflowCreationCount.getAndIncrement();
        final StatelessEngineConfiguration engineConfiguration = createEngineConfiguration(context, dataflowIndex);
        final StatelessBootstrap bootstrap = StatelessBootstrap.bootstrap(engineConfiguration, Thread.currentThread().getContextClassLoader());

        final DataflowDefinition dataflowDefinition = createDataflowDefinition(context, flowSnapshot);

        final StatelessDataflow dataflow = bootstrap.createDataflow(dataflowDefinition);
        dataflow.initialize();
        return dataflow;
    }

    private StatelessDataflow getDataflow(final ProcessContext context) throws IOException, StatelessConfigurationException {
        final StatelessDataflow dataflow = dataflows.poll();
        if (dataflow == null) {
            return createDataflow(context);
        }

        return dataflow;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Fetch a FlowFile, if appropriate
        FlowFile flowFile = null;
        if (context.hasIncomingConnection()) {
            flowFile = session.get();
            if (flowFile == null) {
                return;
            }
        }

        // Get the dataflow to run
        final StatelessDataflow dataflow;
        try {
            dataflow = getDataflow(context);
        } catch (final Exception e) {
            getLogger().error("Could not create dataflow from snapshot", e);
            session.rollback();
            return;
        }

        // Trigger the dataflow and make sure that we always add the StatelessDataflow object back to the queue so that it can be reused.
        try {
            runDataflow(dataflow, flowFile, context, session);
        } finally {
            dataflows.offer(dataflow);
        }
    }


    private void runDataflow(final StatelessDataflow dataflow, final FlowFile flowFile, final ProcessContext context, final ProcessSession session) {
        // Ensure that we get a legitimate timeout value
        final long timeoutMillis;
        try {
            timeoutMillis = context.getProperty(DATAFLOW_TIMEOUT).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            getLogger().error("Failed to determine Dataflow Timeout for {}. Routing to failure", flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Attempt to enqueue the dataflow. If unable, the appropriate log messages will be generated and actions taken by the enqueueFlowFile() method, so we can simply return.
        if (flowFile != null) {
            final boolean enqueued = enqueueFlowFile(flowFile, dataflow, context, session);
            if (!enqueued) {
                return;
            }
        }

        // Reset any counters on the dataflow. This way, we can simply gather the counters after triggering the dataflow,
        // and we know that's how much we need to adjust our counters by.
        dataflow.resetCounters();

        // Trigger the dataflow
        final BulletinRepository bulletinRepository = dataflow.getBulletinRepository();
        final long maxBulletinId = bulletinRepository.getMaxBulletinId();
        final DataflowTrigger trigger = dataflow.trigger(triggerContext);

        // If the timeout is exceeded, transfer original FlowFile to failure and cancel the dataflow invocation.
        Optional<TriggerResult> optionalResult;
        boolean timeoutExceeded = false;
        try {
            optionalResult = trigger.getResult(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            timeoutExceeded = true;
            optionalResult = Optional.empty();
            trigger.cancel();
        } finally {
            surfaceBulletins(bulletinRepository, maxBulletinId);
        }

        if (!optionalResult.isPresent()) {
            timeoutExceeded = true;
        }

        if (timeoutExceeded) {
            getLogger().error("Dataflow did not complete within the allotted time of {} milliseconds for {}. Routing to timeout.", timeoutMillis, flowFile);
            if (flowFile != null) {
                session.transfer(flowFile, REL_TIMEOUT);
            }
            trigger.cancel();
            return;
        }

        // If the datflow was not successful, log an indication of why not and transfer to failure. Then return, as the dataflow has completed and there's nothing left to do.
        final TriggerResult triggerResult = optionalResult.get();
        if (!triggerResult.isSuccessful()) {
            final Optional<Throwable> failureOptional = triggerResult.getFailureCause();
            if (failureOptional.isPresent()) {
                final Throwable cause = failureOptional.get();

                if (flowFile == null) {
                    getLogger().error("Dataflow failed to complete successfully. Yielding.", failureOptional.get());
                } else {
                    getLogger().error("Dataflow failed to complete successfully for {}. Routing to failure and yielding.", flowFile, failureOptional.get());

                    // Add a failure.port.name attribute
                    if (cause instanceof FailurePortEncounteredException) {
                        final String portName = ((FailurePortEncounteredException) cause).getPortName();
                        session.putAttribute(flowFile, "failure.port.name", portName);
                    }
                }

            }

            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }

            adjustCounters(session, dataflow, " (Failed attempts)");
            session.adjustCounter("Failed Invocations", 1, false);
            context.yield();

            return;
        }

        // Create a FlowFile in this NiFi instance for each FlowFile that was output by the Stateless dataflow.
        // We cannot simply transfer the output FlowFiles because they belong to a different, internal session and their content may not be persisted.
        // Therefore, we create our own FlowFile whose parent is the input FlowFile (if one exists) and then add the attributes and contents as necessary.
        final Set<FlowFile> createdSet;
        try {
            createdSet = createOutputFlowFiles(optionalResult.get(), session, flowFile);
        } catch (final IOException e) {
            getLogger().error("Failed to write FlowFile contents that were output from Stateless Flow to the NiFi content repository for {}. Routing to failure.", flowFile, e);
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
            return;
        }

        // Update any counters
        adjustCounters(session, dataflow, null);

        // If dataflow is yielded, yield this processor
        final long yieldExpiration = dataflow.getSourceYieldExpiration();
        if (yieldExpiration > 0) {
            final long now = System.currentTimeMillis();
            final long yieldMillis = yieldExpiration - now;
            if (yieldMillis > 0) {
                context.yield();
            }
        }

        // Transfer the FlowFiles and asynchronously commit the session.
        if (flowFile != null) {
            session.transfer(flowFile, REL_ORIGINAL);
        }
        session.transfer(createdSet, REL_OUTPUT);
        session.commitAsync(triggerResult::acknowledge);

        if (flowFile == null) {
            getLogger().info("Successfully triggered dataflow to run, producing {} output FlowFiles", createdSet.size());
        } else {
            getLogger().info("Successfully triggered dataflow to run against {}, producing {} output FlowFiles", flowFile, createdSet.size());
        }

        session.adjustCounter("Successful Invocations", 1, false);
    }

    private void surfaceBulletins(final BulletinRepository bulletinRepository, final long minBulletinId) {
        // If there are any WARNING or ERROR bulletins, we want to log them for this processor. All of the log messages from the components
        // themselves will already have been logged, but we want to surface any warn/error message as bulletins so we log them again for this processor.
        final BulletinQuery bulletinQuery = new BulletinQuery.Builder()
            .after(minBulletinId)
            .build();

        final List<Bulletin> bulletins = bulletinRepository.findBulletins(bulletinQuery);
        for (final Bulletin bulletin : bulletins) {
            try {
                String level = bulletin.getLevel();
                if (level == null || level.equalsIgnoreCase("WARNING")) {
                    level = "WARN";
                }
                final LogLevel logLevel = LogLevel.valueOf(level);
                if (logLevel == LogLevel.DEBUG || logLevel == LogLevel.INFO) {
                    continue;
                }

                getLogger().log(logLevel, "{} {}[name={}, id={}] {}", bulletin.getTimestamp(), bulletin.getSourceType(), bulletin.getSourceName(), bulletin.getSourceName(), bulletin.getMessage());
            } catch (final Exception e) {
                getLogger().warn("Dataflow emitted a bulletin but failed to surface that bulletin due to {}", e.toString(), e);
            }
        }
    }

    private void adjustCounters(final ProcessSession session, final StatelessDataflow dataflow, final String counterNameSuffix) {
        for (final Map.Entry<String, Long> entry : dataflow.getCounters(false).entrySet()) {
            if (entry.getValue() != 0) {
                final String counterName = counterNameSuffix == null ? entry.getKey() : (entry.getKey() + counterNameSuffix);
                session.adjustCounter(counterName, entry.getValue(), false);
            }
        }
    }

    private Set<FlowFile> createOutputFlowFiles(final TriggerResult triggerResult, final ProcessSession session, final FlowFile flowFile) throws IOException {
        final Set<FlowFile> createdSet = new HashSet<>();
        try {
            final Map<String, List<FlowFile>> outputFlowFiles = triggerResult.getOutputFlowFiles();
            for (final Map.Entry<String, List<FlowFile>> entry : outputFlowFiles.entrySet()) {
                final String outputPortName = entry.getKey();
                final List<FlowFile> outputForPort = entry.getValue();

                for (final FlowFile outputFlowFile : outputForPort) {
                    FlowFile created = flowFile == null ? session.create() : session.create(flowFile);
                    createdSet.add(created);

                    try (final OutputStream out = session.write(created);
                         final InputStream flowFileContents = triggerResult.readContent(outputFlowFile)) {
                        StreamUtils.copy(flowFileContents, out);
                    }

                    final Map<String, String> attributes = new HashMap<>(outputFlowFile.getAttributes());
                    attributes.put("output.port.name", outputPortName);
                    session.putAllAttributes(created, attributes);
                }
            }
        } catch (final Exception e) {
            session.remove(createdSet);
            throw e;
        }

        return createdSet;
    }

    private boolean enqueueFlowFile(final FlowFile flowFile, final StatelessDataflow dataflow, final ProcessContext context, final ProcessSession session) {
        final long maxBytes = context.getProperty(MAX_INPUT_FLOWFILE_SIZE).asDataSize(DataUnit.B).longValue();
        if (flowFile.getSize() > maxBytes) {
            getLogger().warn("Will not process {} because its size of {} bytes exceeds the max configured threshold of {} bytes. Routing to failure",
                flowFile, flowFile.getSize(), maxBytes);

            session.transfer(flowFile, REL_FAILURE);
            return false;
        }

        final Set<String> inputPortNames = dataflow.getInputPortNames();

        // If there is exactly 1 Input Port available in the dataflow, the name isn't required.
        String inputPortName = context.getProperty(INPUT_PORT).evaluateAttributeExpressions(flowFile).getValue();
        if (inputPortName == null || inputPortName.trim().isEmpty()) {
            if (inputPortNames.size() == 1) {
                inputPortName = inputPortNames.iterator().next();
            } else {
                getLogger().error("For {}, determined Input Port Name to be unspecified. Routing to failure.", flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return false;
            }
        }

        if (!inputPortNames.contains(inputPortName)) {
            getLogger().error("For {}, Input Port Name is {}, but that Input Port does not exist in the provided dataflow or is not at the root level. Routing to failure",
                flowFile, inputPortName);
            session.transfer(flowFile, REL_FAILURE);
            return false;
        }

        try (final InputStream in = session.read(flowFile)) {
            dataflow.enqueue(in, flowFile.getAttributes(), inputPortName);
        } catch (final IOException e) {
            getLogger().error("Failed to read contents of FlowFile {} into memory. Routing to failure", flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return false;
        }

        return true;
    }


    private DataflowDefinition createDataflowDefinition(final ProcessContext context, final VersionedFlowSnapshot flowSnapshot) {
        final VersionedExternalFlow externalFlow = VersionedFlowConverter.createVersionedExternalFlow(flowSnapshot);
        final ParameterValueProviderDefinition parameterValueProviderDefinition = new ParameterValueProviderDefinition();
        parameterValueProviderDefinition.setType("org.apache.nifi.stateless.parameter.OverrideParameterValueProvider");
        parameterValueProviderDefinition.setName("Parameter Override");

        final Map<String, String> parameterValues = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            parameterValues.put(entry.getKey().getName(), entry.getValue());
        }

        parameterValueProviderDefinition.setPropertyValues(parameterValues);

        final Integer maxFlowFiles = context.getProperty(MAX_INGEST_FLOWFILES).asInteger();
        final Double maxBytes = context.getProperty(MAX_INGEST_DATA_SIZE).asDataSize(DataUnit.B);
        final long maxTimeNanos = context.getProperty(DATAFLOW_TIMEOUT).asTimePeriod(TimeUnit.NANOSECONDS);

        final TransactionThresholds transactionThresholds = new TransactionThresholds() {
            @Override
            public OptionalLong getMaxFlowFiles() {
                return maxFlowFiles == null ? OptionalLong.empty() : OptionalLong.of(maxFlowFiles);
            }

            @Override
            public OptionalLong getMaxContentSize(final DataUnit dataUnit) {
                return maxBytes == null ? OptionalLong.empty() : OptionalLong.of(maxBytes.longValue());
            }

            @Override
            public OptionalLong getMaxTime(final TimeUnit timeUnit) {
                return OptionalLong.of(timeUnit.convert(maxTimeNanos, TimeUnit.NANOSECONDS));
            }
        };

        return new DataflowDefinition() {
            @Override
            public VersionedExternalFlow getVersionedExternalFlow() {
                return externalFlow;
            }

            @Override
            public String getFlowName() {
                return flowSnapshot.getFlowContents().getName();
            }

            @Override
            public Set<String> getFailurePortNames() {
                return failurePortNames;
            }

            @Override
            public Set<String> getInputPortNames() {
                return Collections.emptySet();
            }

            @Override
            public Set<String> getOutputPortNames() {
                return failurePortNames;
            }

            @Override
            public List<ParameterContextDefinition> getParameterContexts() {
                return null;
            }

            @Override
            public List<ReportingTaskDefinition> getReportingTaskDefinitions() {
                return Collections.emptyList();
            }

            @Override
            public List<ParameterValueProviderDefinition> getParameterValueProviderDefinitions() {
                return Collections.singletonList(parameterValueProviderDefinition);
            }

            @Override
            public TransactionThresholds getTransactionThresholds() {
                return transactionThresholds;
            }
        };
    }

    private StatelessEngineConfiguration createEngineConfiguration(final ProcessContext context, final int contentRepoIndex) {
        final File workingDirectory = new File(context.getProperty(WORKING_DIRECTORY).getValue());
        final File narDirectory = new File(context.getProperty(LIB_DIRECTORY).getValue());
        final ResourceReference krb5Reference = context.getProperty(KRB5_CONF).asResource();
        final File krb5Conf = krb5Reference == null ? null : krb5Reference.asFile();
        final SSLContextService sslContextService = context.getProperty(STATELESS_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        final SslContextDefinition sslContextDefinition;
        if (sslContextService == null) {
            sslContextDefinition = null;
        } else {
            sslContextDefinition = new SslContextDefinition();
            sslContextDefinition.setKeyPass(sslContextService.getKeyPassword());
            sslContextDefinition.setKeystoreFile(sslContextService.getKeyStoreFile());
            sslContextDefinition.setKeystorePass(sslContextService.getKeyStorePassword());
            sslContextDefinition.setKeystoreType(sslContextService.getKeyStoreType());
            sslContextDefinition.setTruststoreFile(sslContextService.getTrustStoreFile());
            sslContextDefinition.setTruststorePass(sslContextService.getTrustStorePassword());
            sslContextDefinition.setTruststoreType(sslContextService.getTrustStoreType());
        }

        final String contentStorageStrategy = context.getProperty(CONTENT_STORAGE_STRATEGY).getValue();
        final File contentRepoDirectory;
        if (CONTENT_STORAGE_DISK.getValue().equals(contentStorageStrategy)) {
            final File contentRepoRootDirectory = new File(workingDirectory, "execute-stateless-flowfile-content");
            final File processorContentRepo = new File(contentRepoRootDirectory, getIdentifier());
            contentRepoDirectory = new File(processorContentRepo, String.valueOf(contentRepoIndex));
        } else {
            contentRepoDirectory = null;
        }

        final String statusTaskInterval = context.getProperty(STATUS_TASK_INTERVAL).getValue();

        return new StatelessEngineConfiguration() {
            @Override
            public File getWorkingDirectory() {
                return workingDirectory;
            }

            @Override
            public File getNarDirectory() {
                return narDirectory;
            }

            @Override
            public File getExtensionsDirectory() {
                return narDirectory;
            }

            @Override
            public Collection<File> getReadOnlyExtensionsDirectories() {
                return Collections.emptyList();
            }

            @Override
            public File getKrb5File() {
                return krb5Conf;
            }

            @Override
            public Optional<File> getContentRepositoryDirectory() {
                return Optional.ofNullable(contentRepoDirectory);
            }

            @Override
            public SslContextDefinition getSslContext() {
                return sslContextDefinition;
            }

            @Override
            public String getSensitivePropsKey() {
                return getIdentifier();
            }

            @Override
            public List<ExtensionClientDefinition> getExtensionClients() {
                return Collections.emptyList();
            }

            @Override
            public boolean isLogExtensionDiscovery() {
                return false;
            }

            @Override
            public String getStatusTaskInterval() {
                return statusTaskInterval;
            }
        };
    }

    @Override
    public Collection<SearchResult> search(final SearchContext context) {
        if (flowSnapshot == null) {
            return Collections.emptyList();
        }

        final VersionedComponentSearchResults results = new VersionedComponentSearchResults(context.getSearchTerm());
        final Bucket bucket = flowSnapshot.getBucket();
        if (bucket != null) {
            results.add(bucket.getIdentifier(), "Bucket ID");
            results.add(bucket.getName(), "Bucket Name");
            results.add(bucket.getDescription(), "Bucket Description");
        }

        final VersionedFlow versionedFlow = flowSnapshot.getFlow();
        if (versionedFlow != null) {
            results.add(versionedFlow.getIdentifier(), "Flow ID");
            results.add(versionedFlow.getName(), "Flow Name");
            results.add(versionedFlow.getDescription(), "Flow Description");
        }

        search(flowSnapshot.getFlowContents(), results);
        return results.toList();
    }

    private void search(final VersionedProcessGroup group, final VersionedComponentSearchResults results) {
        results.add(group.getName(), "Process Group Name");
        results.add(group.getComments(), "Process Group Comments");

        for (final VersionedPort port : group.getInputPorts()) {
            results.add(port.getName(), "Input Port Name");
            results.add(port.getComments(), "Input Port Comments");
            results.add(port.getIdentifier(), "Input Port ID");
        }
        for (final VersionedPort port : group.getOutputPorts()) {
            results.add(port.getName(), "Output Port Name");
            results.add(port.getComments(), "Output Port Comments");
            results.add(port.getIdentifier(), "Output Port ID");
        }
        for (final VersionedLabel label : group.getLabels()) {
            results.add(label.getLabel(), "Label Text");
        }
        for (final VersionedProcessor processor : group.getProcessors()) {
            results.add(processor.getName(), "Processor Name");
            results.add(processor.getType(), "Processor Type");
            results.add(processor.getIdentifier(), "Processor ID");

            for (final Map.Entry<String, String> entry : processor.getProperties().entrySet()) {
                results.add(entry.getKey(), "Processor Property Name");
                results.add(entry.getValue(), "Value of Processor Property " + entry.getKey());
            }
            results.add(processor.getComments(), "Processor Comments");

            final Bundle bundle = processor.getBundle();
            if (bundle != null) {
                results.add(bundle.getGroup(), "Bundle Group ID for Processor " + processor.getType());
                results.add(bundle.getArtifact(), "Bundle Artifact ID for Processor " + processor.getType());
                results.add(bundle.getVersion(), "Bundle Version for Processor " + processor.getType());
            }
        }
        for (final VersionedRemoteProcessGroup rpg : group.getRemoteProcessGroups()) {
            results.add(rpg.getTargetUris(), "RPG Target URI");
            results.add(rpg.getComments(), "RPG Comments");
            results.add(rpg.getIdentifier(), "RPG Identifier");

            for (final VersionedRemoteGroupPort port : rpg.getInputPorts()) {
                results.add(port.getName(), "RPG Input Port Name");
                results.add(port.getIdentifier(), "RPG Input Port ID");
                results.add(port.getTargetId(), "RPG Input Port Target ID");
            }

            for (final VersionedRemoteGroupPort port : rpg.getOutputPorts()) {
                results.add(port.getName(), "RPG Output Port Name");
                results.add(port.getIdentifier(), "RPG Output Port ID");
                results.add(port.getTargetId(), "RPG Output Port Target ID");
            }
        }
        for (final Map.Entry<String, String> entry : group.getVariables().entrySet()) {
            results.add(entry.getKey(), "Variable Name");
            results.add(entry.getValue(), "Value of Variable " + entry.getKey());
        }
        results.add(group.getParameterContextName(), "Parameter Context Name");

        for (final VersionedConnection connection : group.getConnections()) {
            results.add(connection.getIdentifier(), "Connection ID");
            results.add(connection.getName(), "Connection Name");
            if (connection.getSelectedRelationships() != null) {
                results.add(connection.getSelectedRelationships().toString(), "Selected Relationships");
            }
            results.add(connection.getComments(), "Connection Comments");
        }
        for (final VersionedControllerService service : group.getControllerServices()) {
            results.add(service.getName(), "Controller Service Name");
            results.add(service.getType(), "Controller Service Type");
            results.add(service.getIdentifier(), "Controller Service ID");

            for (final Map.Entry<String, String> entry : service.getProperties().entrySet()) {
                results.add(entry.getKey(), "Controller Service Property Name");
                results.add(entry.getValue(), "Value of Controller Service Property " + entry.getKey());
            }
            results.add(service.getComments(), "Controller Service Comments");

            final Bundle bundle = service.getBundle();
            if (bundle != null) {
                results.add(bundle.getGroup(), "Bundle Group ID for Controller Service " + service.getType());
                results.add(bundle.getArtifact(), "Bundle Artifact ID for Controller Service " + service.getType());
                results.add(bundle.getVersion(), "Bundle Version for Controller Service " + service.getType());
            }
        }
        for (final VersionedProcessGroup child : group.getProcessGroups()) {
            search(child, results);
        }
    }


    private static class VersionedComponentSearchResults {
        private final String term;
        private final List<SearchResult> results = new ArrayList<>();

        public VersionedComponentSearchResults(final String term) {
            this.term = term;
        }

        public void add(final String value, final String description) {
            if (value == null) {
                return;
            }

            if (value.contains(term)) {
                results.add(new SearchResult.Builder().match(value).label(description).build());
            }
        }

        public List<SearchResult> toList() {
            return results;
        }
    }

    private static class AbortableTriggerContext implements DataflowTriggerContext {
        private volatile boolean aborted = false;

        @Override
        public boolean isAbort() {
            return aborted;
        }

        public void abort() {
            this.aborted = true;
        }
    }
}
