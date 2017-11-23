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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.satori.rtm.Ack;
import com.satori.rtm.RtmClient;
import com.satori.rtm.RtmClientAdapter;
import com.satori.rtm.RtmClientBuilder;
import com.satori.rtm.auth.RoleSecretAuthProvider;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.util.StreamDemarcator;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Tags({"pubsub", "satori", "rtm", "realtime", "json"})
@CapabilityDescription("Publishes incoming messages to Satori RTM (https://www.satori.com/docs/using-satori/overview)")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
public class PublishSatoriRTM extends AbstractProcessor {

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

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor
            .Builder().name("CHANNEL")
            .displayName("Channel")
            .description("Name of channel to publish to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MSG_DEMARCATOR = new PropertyDescriptor
            .Builder().name("MSG_DEMARCATOR")
            .displayName("Message Demarcator")
            .description("Specifies the string to use for demarcating multiple messages within a single FlowFile. "
                    + "If not specified, the entire content of the FlowFile will be used as a single message. "
                    + "If specified, the contents of the FlowFile will be split on this delimiter and each section sent "
                    + "as a separate RTM message.")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles published successfully to RTM.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles failed to publish to RTM.")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;


    private volatile RtmClient client;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ENDPOINT);
        descriptors.add(APPKEY);
        descriptors.add(ROLE);
        descriptors.add(ROLE_SECRET_KEY);
        descriptors.add(CHANNEL);
        descriptors.add(MSG_DEMARCATOR);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
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
        boolean shouldAuthenticate = context.getProperty(ROLE).isSet();

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

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final ConcurrentMap<FlowFile, Exception> failures = new ConcurrentHashMap<>();

        // Get required user properties
        String channel = context.getProperty(CHANNEL).getValue();
        boolean useDemarcator = context.getProperty(MSG_DEMARCATOR).isSet();

        // Set demarcator value if required
        final byte[] demarcatorBytes;
        if (useDemarcator) {
            demarcatorBytes = context.getProperty(MSG_DEMARCATOR).getValue().getBytes(StandardCharsets.UTF_8);
        } else {
            demarcatorBytes = null;
        }

        // Read incoming FlowFile & publish message(s) to Satori
        session.read(flowFile, rawIn -> {
            try (final InputStream in = new BufferedInputStream(rawIn)) {

                // Split stream based on demarcator
                // Note: Satori limits all user generated payloads to 64kB (so we will hardcode maxDataSize here)
                try (final StreamDemarcator demarcator = new StreamDemarcator(in, demarcatorBytes, 64000)) {
                    byte[] messageBytes;

                    while ((messageBytes = demarcator.nextToken()) != null) {
                        // Publish message to Satori
                        String message = new String(messageBytes);

                        //RTM currently uses JSON data
                        try {
                            // try for JSON object
                            JsonObject obj = new JsonParser().parse(message).getAsJsonObject();
                            client.publish(channel, obj, Ack.YES);
                        } catch (final JsonSyntaxException e) {
                            // Send invalid JSON messages as raw string
                            client.publish(channel, message, Ack.YES);
                        } catch (final Exception e) {
                            // Failure!
                            failures.putIfAbsent(flowFile, e);
                        }

                        // If we have a failure, don't try to send anything else.
                        if (!failures.isEmpty()) {
                            return;
                        }
                    }

                } catch (final Exception e) {
                    // Failure!
                    failures.putIfAbsent(flowFile, e);
                }

            }
        });


        if (!failures.isEmpty()) {
            // Deal with failures
            getLogger().error("Failed to publish message to Satori: " + failures.get(flowFile).toString());
            failures.clear();
            session.transfer(flowFile, FAILURE);
            context.yield();
        } else {
            // Success!
            session.transfer(flowFile, SUCCESS);
        }
    }

    @OnStopped
    public void shutdownClient() {
        if (client != null) {
            client.stop();
        }
    }
}
