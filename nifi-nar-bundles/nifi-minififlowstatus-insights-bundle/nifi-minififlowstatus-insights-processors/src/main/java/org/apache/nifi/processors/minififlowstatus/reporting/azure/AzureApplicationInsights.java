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
package org.apache.nifi.processors.minififlowstatus.reporting.azure;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.applicationinsights.TelemetryClient;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.minififlowstatus.minifi.MiNiFiFlowStatus;
import org.apache.nifi.processors.minififlowstatus.minifi.connection.ConnectionStatusBean;
import org.apache.nifi.processors.minififlowstatus.minifi.processor.ProcessorStatusBean;
import org.apache.nifi.processors.minififlowstatus.minifi.rpg.RemoteProcessGroupStatusBean;
import org.apache.nifi.processors.minififlowstatus.minifi.system.ContentRepositoryUsage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"insights metrics"})
@CapabilityDescription("This processor will take the flow file, which is assumed to be JSON representing the FlowStatus" +
        " report from MiNiFi and send it to Azure Application Insights. Every field in the JSON will be sent as a" +
        " metric to Azure. The hierarchy will be represented with '.' between field names. This processor does not send" +
        " health or bulletin related information")
@SeeAlso({})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttribute(attribute = "minifi.hostname", description = "The minifi hostname to use as the basename " +
        "for all metrics that are sent to a metrics endpoint")
@SupportsBatching
public class AzureApplicationInsights extends AbstractProcessor {

    public static final String TRUE = "True";
    public static final String FALSE = "False";

    public static final PropertyDescriptor AZURE_INSTRUMENTATION_KEY = new PropertyDescriptor
            .Builder().name("Azure InstrumentationKey")
            .displayName("Azure InstrumentationKey")
            .description("The instrumentation key used when sending data to Azure. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BASE_METRIC_NAME = new PropertyDescriptor
            .Builder().name("Base Metric Name")
            .displayName("Base Metric Name")
            .description("This is the base name to use, if this is not populated, then it will look for the property " +
                    "minifi.hostname to use as the base name. If neither of these are populated, then it will default to " +
                    " unkown-minifi-host")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SEND_PROCESSOR_STATUS_PROP = new PropertyDescriptor
            .Builder().name("Send Processor Status")
            .displayName("Send Processor Status")
            .description("If send is chosen then all processor statuses will be sent.")
            .required(true)
            .defaultValue(TRUE)
            .allowableValues(TRUE, FALSE)
            .build();

    public static final PropertyDescriptor SEND_CONNECTION_STATUS_PROP = new PropertyDescriptor
            .Builder().name("Send Connection Status")
            .displayName("Send Connection Status")
            .description("If send is chosen then all connection statuses will be sent.")
            .required(true)
            .defaultValue(TRUE)
            .allowableValues(TRUE, FALSE)
            .build();

    public static final PropertyDescriptor SEND_RPG_STATUS_PROP = new PropertyDescriptor
            .Builder().name("Send Remote Processor Group Status")
            .displayName("Send Remote Processor Group Status")
            .description("If send is chosen then all report processor group statuses will be sent.")
            .required(true)
            .defaultValue(TRUE)
            .allowableValues(TRUE, FALSE)
            .build();

    public static final PropertyDescriptor SEND_INSTANCE_STATUS_PROP = new PropertyDescriptor
            .Builder().name("Send Instance Status")
            .displayName("Send Instance Status")
            .description("If send is chosen then all instance statuses will be sent.")
            .required(true)
            .defaultValue(TRUE)
            .allowableValues(TRUE, FALSE)
            .build();
    public static final PropertyDescriptor SEND_SYS_DIAG_STATUS_PROP = new PropertyDescriptor
            .Builder().name("Send System Diagnostic Status")
            .displayName("Send System Diagnostic Status")
            .description("If send is chosen then all system diagnostic statuses will be sent.")
            .required(true)
            .defaultValue(TRUE)
            .allowableValues(TRUE, FALSE)
            .build();

    public static final Relationship SUCCESS_REL = new Relationship.Builder()
            .name("Success")
            .description("Success Relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private TelemetryClient telemetryClient = createTelemetryClient();

    protected TelemetryClient createTelemetryClient() {
        return new TelemetryClient();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(AZURE_INSTRUMENTATION_KEY);
        descriptors.add(BASE_METRIC_NAME);
        descriptors.add(SEND_PROCESSOR_STATUS_PROP);
        descriptors.add(SEND_CONNECTION_STATUS_PROP);
        descriptors.add(SEND_RPG_STATUS_PROP);
        descriptors.add(SEND_INSTANCE_STATUS_PROP);
        descriptors.add(SEND_SYS_DIAG_STATUS_PROP);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_REL);
        this.relationships = Collections.unmodifiableSet(relationships);

        //ensure we do not fail parsing the JSON if MiNiFi FlowStatus changes
        //before this get's updated.
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }


    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (descriptor.equals(AZURE_INSTRUMENTATION_KEY)) {
            if (null != newValue) {
                telemetryClient.getContext().setInstrumentationKey(newValue);
            }
        }
    }

    @OnStopped
    @OnShutdown
    public void drainTelemtryClient() {
        try {
            telemetryClient.flush();
        } catch (Exception ex) {
            //not much we can do here.....
            getLogger().error("Caught exception trying to flush Azure Telemetry Client");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (null != flowFile) {

            if (null == telemetryClient.getContext().getInstrumentationKey()) {
                final String intstrumentationKey = context.getProperty(AZURE_INSTRUMENTATION_KEY).getValue();
                telemetryClient.getContext().setInstrumentationKey(intstrumentationKey);
            }


            session.read(flowFile, new InputStreamCallback() {

                @Override
                public void process(InputStream inputStream) throws IOException {

                    String hostBaseName = "unkown-minifi-host";
                    if (context.getProperty(BASE_METRIC_NAME).isSet()) {
                        hostBaseName = context.getProperty(BASE_METRIC_NAME).getValue();
                    }
                    String minifiHost = flowFile.getAttribute("minifi.hostname");

                    if (null != minifiHost) {
                        hostBaseName = minifiHost;
                    }

                    MiNiFiFlowStatus flowStatus = objectMapper.readValue(inputStream, MiNiFiFlowStatus.class);

                    if (context.getProperty(SEND_CONNECTION_STATUS_PROP).asBoolean()) {
                        sendConnectionStatus(hostBaseName, flowStatus);
                    }

                    if (context.getProperty(SEND_INSTANCE_STATUS_PROP).asBoolean()) {
                        sendInstanceStatus(hostBaseName, flowStatus);
                    }

                    if (context.getProperty(SEND_PROCESSOR_STATUS_PROP).asBoolean()) {
                        sendProcessorStatus(hostBaseName, flowStatus);
                    }

                    if (context.getProperty(SEND_RPG_STATUS_PROP).asBoolean()) {
                        sendRPGStatus(hostBaseName, flowStatus);
                    }
                    if (context.getProperty(SEND_SYS_DIAG_STATUS_PROP).asBoolean()) {
                        sendSystemDiagnosticStatus(hostBaseName, flowStatus);
                    }
                }
            });

            session.transfer(flowFile, SUCCESS_REL);
        }


    }

    private void sendSystemDiagnosticStatus(String hostBaseName, MiNiFiFlowStatus flowStatus) {
        if (null != flowStatus.systemDiagnosticsStatus) {

            for (final ContentRepositoryUsage contentRepositoryUsage : flowStatus.systemDiagnosticsStatus.contentRepositoryUsageList) {

                final String baseName = hostBaseName + ".system.content.repository." + contentRepositoryUsage.name + ".";
                telemetryClient.trackMetric(baseName + "freeSpace", contentRepositoryUsage.freeSpace);
                telemetryClient.trackMetric(baseName + "totalSpace", contentRepositoryUsage.totalSpace);
                telemetryClient.trackMetric(baseName + "usedSpace", contentRepositoryUsage.usedSpace);
                telemetryClient.trackMetric(baseName + "diskUtilization", contentRepositoryUsage.diskUtilization);
            }

            if (null != flowStatus.systemDiagnosticsStatus.flowfileRepositoryUsage) {
                final String baseName = hostBaseName + ".system.flowfile.repository.";
                telemetryClient.trackMetric(baseName + "freeSpace", flowStatus.systemDiagnosticsStatus.flowfileRepositoryUsage.freeSpace);
                telemetryClient.trackMetric(baseName + "totalSpace", flowStatus.systemDiagnosticsStatus.flowfileRepositoryUsage.totalSpace);
                telemetryClient.trackMetric(baseName + "usedSpace", flowStatus.systemDiagnosticsStatus.flowfileRepositoryUsage.usedSpace);
                telemetryClient.trackMetric(baseName + "diskUtilization", flowStatus.systemDiagnosticsStatus.flowfileRepositoryUsage.diskUtilization);
            }
            if (null != flowStatus.systemDiagnosticsStatus.heapStatus) {
                final String baseName = hostBaseName + ".system.heap.";
                telemetryClient.trackMetric(baseName + "totalHeap", flowStatus.systemDiagnosticsStatus.heapStatus.totalHeap);
                telemetryClient.trackMetric(baseName + "maxHeap", flowStatus.systemDiagnosticsStatus.heapStatus.maxHeap);
                telemetryClient.trackMetric(baseName + "freeHeap", flowStatus.systemDiagnosticsStatus.heapStatus.freeHeap);
                telemetryClient.trackMetric(baseName + "usedHeap", flowStatus.systemDiagnosticsStatus.heapStatus.usedHeap);
                telemetryClient.trackMetric(baseName + "heapUtilization", flowStatus.systemDiagnosticsStatus.heapStatus.heapUtilization);
                telemetryClient.trackMetric(baseName + "totalNonHeap", flowStatus.systemDiagnosticsStatus.heapStatus.totalNonHeap);
                telemetryClient.trackMetric(baseName + "maxNonHeap", flowStatus.systemDiagnosticsStatus.heapStatus.maxNonHeap);
                telemetryClient.trackMetric(baseName + "freeNonHeap", flowStatus.systemDiagnosticsStatus.heapStatus.freeNonHeap);
                telemetryClient.trackMetric(baseName + "usedNonHeap", flowStatus.systemDiagnosticsStatus.heapStatus.usedNonHeap);
                telemetryClient.trackMetric(baseName + "nonHeapUtilization", flowStatus.systemDiagnosticsStatus.heapStatus.nonHeapUtilization);

            }
        }
    }

    private void sendRPGStatus(String hostBaseName, MiNiFiFlowStatus flowStatus) {
        if (null != flowStatus.remoteProcessGroupStatusList) {
            for (final RemoteProcessGroupStatusBean remoteProcessGroupStatusBean : flowStatus.remoteProcessGroupStatusList) {

                final String baseName = hostBaseName + ".rpg." + URI.create(remoteProcessGroupStatusBean.name).getHost() + ".";
                if (null != remoteProcessGroupStatusBean.remoteProcessGroupStats) {
                    telemetryClient.trackMetric(baseName + "activeThreads", remoteProcessGroupStatusBean.remoteProcessGroupStats.activeThreads);
                    telemetryClient.trackMetric(baseName + "sentCount", remoteProcessGroupStatusBean.remoteProcessGroupStats.sentCount);
                    telemetryClient.trackMetric(baseName + "sentContentSize", remoteProcessGroupStatusBean.remoteProcessGroupStats.sentContentSize);
                }
            }

        }
    }

    private void sendProcessorStatus(String hostBaseName, MiNiFiFlowStatus flowStatus) {
        if (null != flowStatus.processorStatusList) {
            for (final ProcessorStatusBean processorStatus : flowStatus.processorStatusList) {

                final String baseName = hostBaseName + ".processor." + processorStatus.name.replace('/', '-') + ".";
                telemetryClient.trackMetric(baseName + "activeThreads", processorStatus.processorStats.activeThreads);
                telemetryClient.trackMetric(baseName + "flowfilesReceived", processorStatus.processorStats.flowfilesReceived);
                telemetryClient.trackMetric(baseName + "flowfilesSent", processorStatus.processorStats.flowfilesSent);
                telemetryClient.trackMetric(baseName + "bytesRead", processorStatus.processorStats.bytesRead);
                telemetryClient.trackMetric(baseName + "bytesWritten", processorStatus.processorStats.bytesWritten);
                telemetryClient.trackMetric(baseName + "invocations", processorStatus.processorStats.invocations);
                telemetryClient.trackMetric(baseName + "processingNanos", processorStatus.processorStats.processingNanos);

            }

        }
    }

    private void sendInstanceStatus(String hostBaseName, MiNiFiFlowStatus flowStatus) {
        if (null != flowStatus.instanceStatus) {
            if (null != flowStatus.instanceStatus.instanceStats) {
                final String baseName = hostBaseName + ".instance.";
                telemetryClient.trackMetric(baseName + "bytesRead", flowStatus.instanceStatus.instanceStats.bytesRead);
                telemetryClient.trackMetric(baseName + "bytesWritten", flowStatus.instanceStatus.instanceStats.bytesWritten);
                telemetryClient.trackMetric(baseName + "bytesSent", flowStatus.instanceStatus.instanceStats.bytesSent);
                telemetryClient.trackMetric(baseName + "flowfilesSent", flowStatus.instanceStatus.instanceStats.flowfilesSent);
                telemetryClient.trackMetric(baseName + "bytesTransferred", flowStatus.instanceStatus.instanceStats.bytesTransferred);
                telemetryClient.trackMetric(baseName + "flowfilesTransferred", flowStatus.instanceStatus.instanceStats.flowfilesTransferred);
                telemetryClient.trackMetric(baseName + "bytesReceived", flowStatus.instanceStatus.instanceStats.bytesReceived);
                telemetryClient.trackMetric(baseName + "flowfilesReceived", flowStatus.instanceStatus.instanceStats.flowfilesReceived);
            }
        }
    }

    private void sendConnectionStatus(String hostBaseName, MiNiFiFlowStatus flowStatus) {

        if (null != flowStatus.connectionStatusList) {
            for (final ConnectionStatusBean connectionStatus : flowStatus.connectionStatusList) {

                final String baseName = hostBaseName + ".connection." + connectionStatus.name.replace('/', '-') + ".";
                telemetryClient.trackMetric(baseName + "inputCount", connectionStatus.connectionStats.inputCount);
                telemetryClient.trackMetric(baseName + "inputBytes", connectionStatus.connectionStats.inputBytes);
                telemetryClient.trackMetric(baseName + "outputCount", connectionStatus.connectionStats.outputCount);
                telemetryClient.trackMetric(baseName + "outputBytes", connectionStatus.connectionStats.outputBytes);
            }
        }
    }

}
