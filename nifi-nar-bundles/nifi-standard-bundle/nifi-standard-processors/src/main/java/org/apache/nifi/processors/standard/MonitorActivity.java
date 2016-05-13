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
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
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
public class MonitorActivity extends AbstractProcessor {

    public static final PropertyDescriptor THRESHOLD = new PropertyDescriptor.Builder()
            .name("Threshold Duration")
            .description("Determines how much time must elapse before considering the flow to be inactive")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
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
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("Activity restored at time: ${now():format('yyyy/MM/dd HH:mm:ss')} after being inactive for ${inactivityDurationMillis:toNumber():divide(60000)} minutes")
            .build();
    public static final PropertyDescriptor INACTIVITY_MESSAGE = new PropertyDescriptor.Builder()
            .name("Inactivity Message")
            .description("The message that will be the content of FlowFiles that are sent to the 'inactive' relationship")
            .required(true)
            .expressionLanguageSupported(true)
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
    private final AtomicBoolean inactive = new AtomicBoolean(false);
    private final AtomicLong lastInactiveMessage = new AtomicLong(System.currentTimeMillis());

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(THRESHOLD);
        properties.add(CONTINUALLY_SEND_MESSAGES);
        properties.add(INACTIVITY_MESSAGE);
        properties.add(ACTIVITY_RESTORED_MESSAGE);
        properties.add(COPY_ATTRIBUTES);
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
    public void resetLastSuccessfulTransfer() {
        setLastSuccessfulTransfer(System.currentTimeMillis());
    }

    protected final void setLastSuccessfulTransfer(final long timestamp) {
        latestSuccessTransfer.set(timestamp);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final long thresholdMillis = context.getProperty(THRESHOLD).asTimePeriod(TimeUnit.MILLISECONDS);
        final long now = System.currentTimeMillis();

        final ComponentLog logger = getLogger();
        final List<FlowFile> flowFiles = session.get(50);
        if (flowFiles.isEmpty()) {
            final long previousSuccessMillis = latestSuccessTransfer.get();
            boolean sendInactiveMarker = false;

            if (now >= previousSuccessMillis + thresholdMillis) {
                final boolean continual = context.getProperty(CONTINUALLY_SEND_MESSAGES).asBoolean();
                sendInactiveMarker = !inactive.getAndSet(true) || (continual && (now > lastInactiveMessage.get() + thresholdMillis));
            }

            if (sendInactiveMarker) {
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
            logger.info("Transferred {} FlowFiles to 'success'", new Object[]{flowFiles.size()});

            final long inactivityStartMillis = latestSuccessTransfer.getAndSet(now);
            if (inactive.getAndSet(false)) {
                FlowFile activityRestoredFlowFile = session.create();

                final boolean copyAttributes = context.getProperty(COPY_ATTRIBUTES).asBoolean();
                if (copyAttributes) {

                    // copy attributes from the first flow file in the list
                    Map<String, String> attributes = new HashMap<>(flowFiles.get(0).getAttributes());
                    // don't copy the UUID
                    attributes.remove(CoreAttributes.UUID.key());
                    activityRestoredFlowFile = session.putAllAttributes(activityRestoredFlowFile, attributes);
                }

                activityRestoredFlowFile = session.putAttribute(activityRestoredFlowFile, "inactivityStartMillis", String.valueOf(inactivityStartMillis));
                activityRestoredFlowFile = session.putAttribute(activityRestoredFlowFile, "inactivityDurationMillis", String.valueOf(now - inactivityStartMillis));

                final byte[] outBytes = context.getProperty(ACTIVITY_RESTORED_MESSAGE).evaluateAttributeExpressions(activityRestoredFlowFile).getValue().getBytes(UTF8);
                activityRestoredFlowFile = session.write(activityRestoredFlowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        out.write(outBytes);
                    }
                });

                session.getProvenanceReporter().create(activityRestoredFlowFile);
                session.transfer(activityRestoredFlowFile, REL_ACTIVITY_RESTORED);
                logger.info("Transferred {} to 'activity.restored'", new Object[]{activityRestoredFlowFile});
            }
        }
    }
}
