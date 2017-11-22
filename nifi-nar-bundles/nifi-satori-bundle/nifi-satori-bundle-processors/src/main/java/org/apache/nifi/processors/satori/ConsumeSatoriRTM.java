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
package org.apache.nifi.processors.satori;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.satori.rtm.*;
import com.satori.rtm.auth.*;
import com.satori.rtm.model.*;

@Tags({"pubsub", "satori", "rtm", "realtime", "json"})
@CapabilityDescription("Consumes a streaming data feed from Satori RTM (https://www.satori.com/docs/using-satori/overview)")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@SupportsBatching
public class ConsumeSatoriRTM extends AbstractProcessor {

    private static final AllowableValue SUB_MODE_SIMPLE = new AllowableValue("SIMPLE", "SIMPLE",
            "RTM doesn't track the position value for the subscription. Instead, when RTM resubscribes following a reconnection, "
                    + "it fast-forwards to the earliest position that points to a non-expired message.");

    private static final AllowableValue SUB_MODE_RELIABLE = new AllowableValue("RELIABLE", "RELIABLE",
            "RTM tracks the position value for the subscription and tries to use it when resubscribing after the connection drops and the client reconnects. "
                    + "If the position points to an expired message, RTM fast-forwards to the earliest position that points to a non-expired message.");

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
            .Builder().name("ENDPOINT")
            .displayName("Endpoint")
            .description("Entry point to RTM")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("wss://open-data.api.satori.com")
            .build();

    public static final PropertyDescriptor APPKEY = new PropertyDescriptor
            .Builder().name("APPKEY")
            .displayName("Appkey")
            .description("String used by RTM to identify the client when it connects to RTM")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor ROLE = new PropertyDescriptor
            .Builder().name("ROLE")
            .displayName("Role")
            .description("Role and Role Secret Key are only required when connecting to a channel that is explicitly "
                    + "configured with channel permissions requiring authentication. Not required for consuming Open Data Channels.")
            .required(false)
            .build();

    public static final PropertyDescriptor ROLE_SECRET_KEY = new PropertyDescriptor
            .Builder().name("ROLE_SECRET_KEY")
            .displayName("Role Secret Key")
            .description("Role and Role Secret Key are only required when connecting to a channel that is explicitly "
                    + "configured with channel permissions requiring authentication. Not required for consuming Open Data Channels.")
            .required(false)
            .addValidator(Validator.VALID)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_MODE = new PropertyDescriptor
            .Builder().name("SUBSCRIPTION_MODE")
            .displayName("Subscription Mode")
            .description("Determines how the position in the queue will be tracked")
            .required(true)
            .allowableValues(SUB_MODE_SIMPLE,SUB_MODE_RELIABLE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("SIMPLE")
            .build();

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor
            .Builder().name("CHANNEL")
            .displayName("Channel")
            .description("Name of channel to consume from")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILTER = new PropertyDescriptor
            .Builder().name("FILTER")
            .displayName("Filter")
            .description("Stream SQL used to filter or aggregate data consumed from the RTM channel")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder().name("BATCH_SIZE")
            .displayName("Minimum Batch Size")
            .description("Configures the number of messages to be grouped into a single FlowFile (accepts values up to 10000). Setting no value will map a single message per FlowFile.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(false)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from RTM. Depending on batching strategy it is a flow file per message or a bundle of messages grouped together.")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;


    private volatile RtmClient client;
    private volatile BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(10000);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ENDPOINT);
        descriptors.add(APPKEY);
        descriptors.add(ROLE);
        descriptors.add(ROLE_SECRET_KEY);
        descriptors.add(SUBSCRIPTION_MODE);
        descriptors.add(CHANNEL);
        descriptors.add(FILTER);
        descriptors.add(BATCH_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        // Get the user defined configuration properties
        String endpoint = context.getProperty(ENDPOINT).getValue();
        String appkey = context.getProperty(APPKEY).getValue();
        String role = context.getProperty(ROLE).getValue();
        String roleSecretKey = context.getProperty(ROLE_SECRET_KEY).getValue();
        String channel = context.getProperty(CHANNEL).getValue();
        String filter = context.getProperty(FILTER).getValue();
        String subMode = context.getProperty(SUBSCRIPTION_MODE).getValue();
        boolean shouldAuthenticate = context.getProperty(ROLE).isSet();
        boolean useFilter = context.getProperty(FILTER).isSet();

        // Connect to satori
        final RtmClientBuilder builder = new RtmClientBuilder(endpoint, appkey)
                .setListener(new RtmClientAdapter() {
                    @Override
                    public void onConnectingError(RtmClient client, Exception ex) {
                        String msg = String.format("RTM client failed to connect to '%s': %s",
                                endpoint, ex.getMessage());
                        getLogger().error(msg);
                    }

                    @Override
                    public void onError(RtmClient client, Exception ex) {
                        String msg = String.format("RTM client failed: %s", ex.getMessage());
                        getLogger().error(msg);
                    }

                    @Override
                    public void onEnterConnected(RtmClient client) {
                        getLogger().info("Connected to Satori!");
                    }

                });

        // Authenticate with role and secret key if required
        if (shouldAuthenticate) {
            builder.setAuthProvider(new RoleSecretAuthProvider(role, roleSecretKey));
        }

        client = builder.build();

        getLogger().info(String.format(
                "RTM connection config:\n" +
                        "\tendpoint='%s'\n" +
                        "\tappkey='%s'\n" +
                        "\tauthenticate?=%b", endpoint, appkey, shouldAuthenticate));

        client.start();

        // Set up Satori Subscription Config
        SubscriptionConfig subConfig = new SubscriptionConfig(
                (subMode.equals("SIMPLE")) ? SubscriptionMode.SIMPLE : SubscriptionMode.RELIABLE, // Set sub mode
                new SubscriptionAdapter() {
                    @Override
                    public void onEnterSubscribed(SubscribeRequest request, SubscribeReply reply) {
                        // when subscription is established (confirmed by RTM)
                        getLogger().info("Subscribed to the channel: " + channel);
                    }

                    @Override
                    public void onSubscriptionError(SubscriptionError error) {
                        // when a subscribe or subscription error occurs
                        getLogger().error("Failed to subscribe: " + error.getReason());
                    }

                    @Override
                    public void onSubscriptionData(SubscriptionData data) {
                        // when incoming messages arrive
                        for (AnyJson json : data.getMessages()) {
                            try {
                                messageQueue.add(json.toString());
                            } catch (NullPointerException e) {
                                getLogger().error(e.toString());
                            }
                        }
                    }
                });

        // Apply SQL filter if supplied
        if (useFilter) {
            subConfig.setFilter(filter);
        }

        // Create the subscription!
        try {
            client.createSubscription(channel,subConfig);
        } catch (Exception e) {
            getLogger().error(e.toString());
        }

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        try {

            // Get required user properties
            String endpoint = context.getProperty(ENDPOINT).getValue();
            String channel = context.getProperty(CHANNEL).getValue();
            String batchSizeStr = context.getProperty(BATCH_SIZE).getValue();
            int batchSize = 1;
            boolean batch;

            try {
                batchSize = Integer.parseInt(batchSizeStr);
                batch = true;
            } catch (NumberFormatException e) {
                batch = false;
            }

            // Check messageQueue for new messages
            if (messageQueue.isEmpty()) {
                context.yield();
                return;
            }

            // Get message(s) from queue
            final String message;

            if (batch)
            {
                // Wait for minimum number of messages as configured in the batch size
                if (messageQueue.size() < batchSize) {
                    context.yield();
                    return;
                } else {
                    List<String> messages = new ArrayList<>();
                    messageQueue.drainTo(messages);

                    // Merge into a single new-line delimited message
                    message = String.join("\n", messages);
                }
            } else {
                // Get a single message from the queue
                message = messageQueue.poll(100, TimeUnit.MILLISECONDS);
            }

            // Write message to FlowFile
            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream outputStream) throws IOException {
                    outputStream.write(message.getBytes());
                }
            });

            // Set relevant attributes
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
            attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
            attributes.put("endpoint", endpoint);
            attributes.put("channel", channel);
            flowFile = session.putAllAttributes(flowFile, attributes);

            //Success!
            session.transfer(flowFile, SUCCESS);

        } catch (InterruptedException e) {
            getLogger().error(e.toString());
        }

    }

    @OnStopped
    public void shutdownClient() {
        if (client != null) {
            client.stop();
        }
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        // Destroy all messages in the queue when configurations are changed.
        messageQueue.clear();
    }
}
