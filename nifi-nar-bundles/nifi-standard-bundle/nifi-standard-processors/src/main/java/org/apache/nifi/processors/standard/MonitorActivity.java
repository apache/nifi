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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

@SideEffectFree
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"monitor", "flow", "active", "inactive", "activity", "detection"})
@CapabilityDescription("Monitors the flow for activity and sends out an indicator when the flow has not had any data for "
        + "some specified amount of time and again when the flow's activity is restored")
@WritesAttributes({
    @WritesAttribute(attribute = "inactivityStartMillis", description = "The time at which Inactivity began, in the form of milliseconds since Epoch"),
    @WritesAttribute(attribute = "inactivityDurationMillis", description = "The number of milliseconds that the inactivity has spanned")})
@Stateful(scopes = Scope.CLUSTER, description = "MonitorActivity stores the last timestamp at each node as state, so that it can examine activity at cluster wide." +
        "If 'Copy Attribute' is set to true, then flow file attributes are also persisted.")
public class MonitorActivity extends AbstractProcessor {

    public static final AllowableValue SCOPE_NODE = new AllowableValue("node");
    public static final AllowableValue SCOPE_CLUSTER = new AllowableValue("cluster");
    public static final AllowableValue REPORT_NODE_ALL = new AllowableValue("all");
    public static final AllowableValue REPORT_NODE_PRIMARY = new AllowableValue("primary");

    public static final PropertyDescriptor THRESHOLD = new PropertyDescriptor.Builder()
            .name("Threshold Duration")
            .description("Determines how much time must elapse before considering the flow to be inactive")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("5 min")
            .build();
    public static final PropertyDescriptor CONTINUALLY_SEND_MESSAGES = new PropertyDescriptor.Builder()
            .name("Continually Send Messages")
            .description("If true, will send inactivity indicator continually every Threshold Duration amount of time until activity is restored; "
                    + "if false, will send an indicator only when the flow first becomes inactive")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor ACTIVITY_RESTORED_MESSAGE = new PropertyDescriptor.Builder()
            .name("Activity Restored Message")
            .description("The message that will be the content of FlowFiles that are sent to 'activity.restored' relationship")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("Activity restored at time: ${now():format('yyyy/MM/dd HH:mm:ss')} after being inactive for ${inactivityDurationMillis:toNumber():divide(60000)} minutes")
            .build();
    public static final PropertyDescriptor INACTIVITY_MESSAGE = new PropertyDescriptor.Builder()
            .name("Inactivity Message")
            .description("The message that will be the content of FlowFiles that are sent to the 'inactive' relationship")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("Lacking activity as of time: ${now():format('yyyy/MM/dd HH:mm:ss')}; flow has been inactive for ${inactivityDurationMillis:toNumber():divide(60000)} minutes")
            .build();
    public static final PropertyDescriptor COPY_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Copy Attributes")
            .description("If true, will copy all flow file attributes from the flow file that resumed activity to the newly created indicator flow file")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor MONITORING_SCOPE = new PropertyDescriptor.Builder()
            .name("Monitoring Scope")
            .description("Specify how to determine activeness of the flow. 'node' means that activeness is examined at individual node separately." +
                    " It can be useful if DFM expects each node should receive flow files in a distributed manner." +
                    " With 'cluster', it defines the flow is active while at least one node receives flow files actively." +
                    " If NiFi is running as standalone mode, this should be set as 'node'," +
                    " if it's 'cluster', NiFi logs a warning message and act as 'node' scope.")
            .required(true)
            .allowableValues(SCOPE_NODE, SCOPE_CLUSTER)
            .defaultValue(SCOPE_NODE.getValue())
            .build();
    public static final PropertyDescriptor REPORTING_NODE = new PropertyDescriptor.Builder()
            .name("Reporting Node")
            .description("Specify which node should send notification flow-files to inactive and activity.restored relationships." +
                    " With 'all', every node in this cluster send notification flow-files." +
                    " 'primary' means flow-files will be sent only from a primary node." +
                    " If NiFi is running as standalone mode, this should be set as 'all'," +
                    " even if it's 'primary', NiFi act as 'all'.")
            .required(true)
            .allowableValues(REPORT_NODE_ALL, REPORT_NODE_PRIMARY)
            .addValidator(((subject, input, context) -> {
                boolean invalid = REPORT_NODE_PRIMARY.equals(input) && SCOPE_NODE.equals(context.getProperty(MONITORING_SCOPE).getValue());
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation("'" + REPORT_NODE_PRIMARY + "' is only available with '" + SCOPE_CLUSTER + "' scope.").valid(!invalid).build();
            }))
            .defaultValue(REPORT_NODE_ALL.getValue())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All incoming FlowFiles are routed to success")
            .build();
    public static final Relationship REL_INACTIVE = new Relationship.Builder()
            .name("inactive")
            .description("This relationship is used to transfer an Inactivity indicator when no FlowFiles are routed to 'success' for Threshold "
                    + "Duration amount of time")
            .build();
    public static final Relationship REL_ACTIVITY_RESTORED = new Relationship.Builder()
            .name("activity.restored")
            .description("This relationship is used to transfer an Activity Restored indicator when FlowFiles are routing to 'success' following a "
                    + "period of inactivity")
            .build();
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private final AtomicLong latestSuccessTransfer = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong latestReportedNodeState = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean inactive = new AtomicBoolean(false);
    private final AtomicLong lastInactiveMessage = new AtomicLong(System.currentTimeMillis());
    public static final String STATE_KEY_LATEST_SUCCESS_TRANSFER = "MonitorActivity.latestSuccessTransfer";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(THRESHOLD);
        properties.add(CONTINUALLY_SEND_MESSAGES);
        properties.add(INACTIVITY_MESSAGE);
        properties.add(ACTIVITY_RESTORED_MESSAGE);
        properties.add(COPY_ATTRIBUTES);
        properties.add(MONITORING_SCOPE);
        properties.add(REPORTING_NODE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_INACTIVE);
        relationships.add(REL_ACTIVITY_RESTORED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Check configuration.
        isClusterScope(context, true);
        resetLastSuccessfulTransfer();
        inactive.set(false);
    }


    protected void resetLastSuccessfulTransfer() {
        setLastSuccessfulTransfer(System.currentTimeMillis());
    }

    protected final void setLastSuccessfulTransfer(final long timestamp) {
        latestSuccessTransfer.set(timestamp);
        latestReportedNodeState.set(timestamp);
    }

    private boolean isClusterScope(final ProcessContext context, boolean logInvalidConfig) {
        if (SCOPE_CLUSTER.equals(context.getProperty(MONITORING_SCOPE).getValue())) {
            if (getNodeTypeProvider().isClustered()) {
                return true;
            }
            if (logInvalidConfig) {
                getLogger().warn("NiFi is running as a Standalone mode, but 'cluster' scope is set." +
                        " Fallback to 'node' scope. Fix configuration to stop this message.");
            }
        }
        return false;
    }

    private boolean shouldReportOnlyOnPrimary(boolean isClusterScope, final ProcessContext context) {
        if (REPORT_NODE_PRIMARY.equals(context.getProperty(REPORTING_NODE).getValue())) {
            if (isClusterScope) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final long thresholdMillis = context.getProperty(THRESHOLD).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final long now = System.currentTimeMillis();

        final ComponentLog logger = getLogger();
        final boolean copyAttributes = context.getProperty(COPY_ATTRIBUTES).asBoolean();
        final boolean isClusterScope = isClusterScope(context, false);
        final boolean shouldReportOnlyOnPrimary = shouldReportOnlyOnPrimary(isClusterScope, context);
        final List<FlowFile> flowFiles = session.get(50);

        boolean isInactive = false;
        long updatedLatestSuccessTransfer = -1;
        StateMap clusterState = null;

        if (flowFiles.isEmpty()) {
            final long previousSuccessMillis = latestSuccessTransfer.get();

            boolean sendInactiveMarker = false;

            isInactive = (now >= previousSuccessMillis + thresholdMillis);
            logger.debug("isInactive={}, previousSuccessMillis={}, now={}", new Object[]{isInactive, previousSuccessMillis, now});
            if (isInactive && isClusterScope) {
                // Even if this node has been inactive, there may be other nodes handling flow actively.
                // However, if this node is active, we don't have to look at cluster state.
                try {
                    clusterState = context.getStateManager().getState(Scope.CLUSTER);
                    if (clusterState != null && !StringUtils.isEmpty(clusterState.get(STATE_KEY_LATEST_SUCCESS_TRANSFER))) {
                        final long latestReportedClusterActivity = Long.valueOf(clusterState.get(STATE_KEY_LATEST_SUCCESS_TRANSFER));
                        isInactive = (now >= latestReportedClusterActivity + thresholdMillis);
                        if (!isInactive) {
                            // This node has been inactive, but other node has more recent activity.
                            updatedLatestSuccessTransfer = latestReportedClusterActivity;
                        }
                        logger.debug("isInactive={}, latestReportedClusterActivity={}", new Object[]{isInactive, latestReportedClusterActivity});
                    }
                } catch (IOException e) {
                    logger.error("Failed to access cluster state. Activity will not be monitored properly until this is addressed.", e);
                }
            }

            if (isInactive) {
                final boolean continual = context.getProperty(CONTINUALLY_SEND_MESSAGES).asBoolean();
                sendInactiveMarker = !inactive.getAndSet(true) || (continual && (now > lastInactiveMessage.get() + thresholdMillis));
            }

            if (sendInactiveMarker && shouldThisNodeReport(isClusterScope, shouldReportOnlyOnPrimary)) {
                lastInactiveMessage.set(System.currentTimeMillis());

                FlowFile inactiveFlowFile = session.create();
                inactiveFlowFile = session.putAttribute(inactiveFlowFile, "inactivityStartMillis", String.valueOf(previousSuccessMillis));
                inactiveFlowFile = session.putAttribute(inactiveFlowFile, "inactivityDurationMillis", String.valueOf(now - previousSuccessMillis));

                final byte[] outBytes = context.getProperty(INACTIVITY_MESSAGE).evaluateAttributeExpressions(inactiveFlowFile).getValue().getBytes(UTF8);
                inactiveFlowFile = session.write(inactiveFlowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        out.write(outBytes);
                    }
                });

                session.getProvenanceReporter().create(inactiveFlowFile);
                session.transfer(inactiveFlowFile, REL_INACTIVE);
                logger.info("Transferred {} to 'inactive'", new Object[]{inactiveFlowFile});
            } else {
                context.yield();    // no need to dominate CPU checking times; let other processors run for a bit.
            }

        } else {
            session.transfer(flowFiles, REL_SUCCESS);
            updatedLatestSuccessTransfer = now;
            logger.info("Transferred {} FlowFiles to 'success'", new Object[]{flowFiles.size()});

            final long latestStateReportTimestamp = latestReportedNodeState.get();
            if (isClusterScope
                    && (now - latestStateReportTimestamp) > (thresholdMillis / 3)) {
                // We don't want to hit the state manager every onTrigger(), but often enough to detect activeness.
                try {
                    final StateManager stateManager = context.getStateManager();
                    final StateMap state = stateManager.getState(Scope.CLUSTER);

                    final Map<String, String> newValues = new HashMap<>();

                    // Persist attributes so that other nodes can copy it
                    if (copyAttributes) {
                        newValues.putAll(flowFiles.get(0).getAttributes());
                    }
                    newValues.put(STATE_KEY_LATEST_SUCCESS_TRANSFER, String.valueOf(now));

                    if (state == null || state.getVersion() == -1) {
                        stateManager.setState(newValues, Scope.CLUSTER);
                    } else {
                        final String existingTimestamp = state.get(STATE_KEY_LATEST_SUCCESS_TRANSFER);
                        if (StringUtils.isEmpty(existingTimestamp)
                                || Long.parseLong(existingTimestamp) < now) {
                            // If this returns false due to race condition, it's not a problem since we just need
                            // the latest active timestamp.
                            stateManager.replace(state, newValues, Scope.CLUSTER);
                        } else {
                            logger.debug("Existing state has more recent timestamp, didn't update state.");
                        }
                    }
                    latestReportedNodeState.set(now);
                } catch (IOException e) {
                    logger.error("Failed to access cluster state. Activity will not be monitored properly until this is addressed.", e);
                }
            }
        }

        if (!isInactive) {
            final long inactivityStartMillis = latestSuccessTransfer.get();
            if (updatedLatestSuccessTransfer > -1) {
                latestSuccessTransfer.set(updatedLatestSuccessTransfer);
            }
            if (inactive.getAndSet(false) && shouldThisNodeReport(isClusterScope, shouldReportOnlyOnPrimary)) {
                FlowFile activityRestoredFlowFile = session.create();

                if (copyAttributes) {

                    final Map<String, String> attributes = new HashMap<>();
                    if (flowFiles.size() > 0) {
                        // copy attributes from the first flow file in the list
                        attributes.putAll(flowFiles.get(0).getAttributes());
                    } else if (clusterState != null) {
                        attributes.putAll(clusterState.toMap());
                        attributes.remove(STATE_KEY_LATEST_SUCCESS_TRANSFER);
                    }
                    // don't copy the UUID
                    attributes.remove(CoreAttributes.UUID.key());
                    activityRestoredFlowFile = session.putAllAttributes(activityRestoredFlowFile, attributes);
                }

                activityRestoredFlowFile = session.putAttribute(activityRestoredFlowFile, "inactivityStartMillis", String.valueOf(inactivityStartMillis));
                activityRestoredFlowFile = session.putAttribute(activityRestoredFlowFile, "inactivityDurationMillis", String.valueOf(now - inactivityStartMillis));

                final byte[] outBytes = context.getProperty(ACTIVITY_RESTORED_MESSAGE).evaluateAttributeExpressions(activityRestoredFlowFile).getValue().getBytes(UTF8);
                activityRestoredFlowFile = session.write(activityRestoredFlowFile, out -> out.write(outBytes));

                session.getProvenanceReporter().create(activityRestoredFlowFile);
                session.transfer(activityRestoredFlowFile, REL_ACTIVITY_RESTORED);
                logger.info("Transferred {} to 'activity.restored'", new Object[]{activityRestoredFlowFile});
            }
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        if (getNodeTypeProvider().isPrimary()) {
            final StateManager stateManager = context.getStateManager();
            try {
                stateManager.clear(Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().error("Failed to clear cluster state due to " + e, e);
            }
        }
    }

    private boolean shouldThisNodeReport(boolean isClusterScope, boolean isReportOnlyOnPrimary) {
        return !isClusterScope
                || !isReportOnlyOnPrimary
                || getNodeTypeProvider().isPrimary();
    }

}
