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
package org.apache.nifi.processors.slack;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import javax.json.stream.JsonParsingException;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"put", "slack", "notify"})
@CapabilityDescription("Publishes a message to Slack")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "A JSON object to add to Slack's \"attachments\" JSON payload.", value = "JSON-formatted string to add to Slack's payload JSON appended to the \"attachments\" JSON array.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Converts the contents of each value specified by the Dynamic Property's value to JSON and appends it to the payload being sent to Slack.")
public class PutSlack extends AbstractProcessor {

    public static final PropertyDescriptor WEBHOOK_URL = new PropertyDescriptor
            .Builder()
            .name("webhook-url")
            .displayName("Webhook URL")
            .description("The POST URL provided by Slack to send messages into a channel.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor WEBHOOK_TEXT = new PropertyDescriptor
            .Builder()
            .name("webhook-text")
            .displayName("Webhook Text")
            .description("The text sent in the webhook message")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor
            .Builder()
            .name("channel")
            .displayName("Channel")
            .description("A public channel using #channel or direct message using @username. If not specified, " +
                    "the default webhook channel as specified in Slack's Incoming Webhooks web interface is used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor THREAD_TS = new PropertyDescriptor.Builder()
        .name("Thread Timestamp")
        .description("The timestamp of the parent message, also known as a thread_ts, or Thread Timestamp. If not specified, the message will be send to the channel " +
            "as an independent message. If this value is populated, the message will be sent as a reply to the message whose timestamp is equal to the given value.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder()
            .name("username")
            .displayName("Username")
            .description("The displayed Slack username")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ICON_URL = new PropertyDescriptor
            .Builder()
            .name("icon-url")
            .displayName("Icon URL")
            .description("Icon URL to be used for the message")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ICON_EMOJI = new PropertyDescriptor
            .Builder()
            .name("icon-emoji")
            .displayName("Icon Emoji")
            .description("Icon Emoji to be used for the message. Must begin and end with a colon, e.g. :ghost:")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(new EmojiValidator())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent to Slack")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent to Slack")
            .build();

    private final SortedSet<PropertyDescriptor> attachmentDescriptors = Collections.synchronizedSortedSet(new TreeSet<>());

    private static final List<PropertyDescriptor> descriptors = List.of(
        WEBHOOK_URL,
        WEBHOOK_TEXT,
        CHANNEL,
        USERNAME,
        THREAD_TS,
        ICON_URL,
        ICON_EMOJI);

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_FAILURE);


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }


    @OnScheduled
    public void initialize(final ProcessContext context) {
        attachmentDescriptors.clear();
        for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            PropertyDescriptor descriptor = property.getKey();
            if (descriptor.isDynamic()) {
                attachmentDescriptors.add(descriptor);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final JsonObjectBuilder builder = Json.createObjectBuilder();
        final String text = context.getProperty(WEBHOOK_TEXT).evaluateAttributeExpressions(flowFile).getValue();
        if (isEmpty(text)) {
            getLogger().error("FlowFile should have non-empty " + WEBHOOK_TEXT.getDisplayName());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        builder.add("text", text);

        final String channel = context.getProperty(CHANNEL).evaluateAttributeExpressions(flowFile).getValue();
        if (!isEmpty(channel)) {
            builder.add("channel", channel);
        }

        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
        if (!isEmpty(username)) {
            builder.add("username", username);
        }

        final String iconUrl = context.getProperty(ICON_URL).evaluateAttributeExpressions(flowFile).getValue();
        if (!isEmpty(iconUrl)) {
            builder.add("icon_url", iconUrl);
        }

        final String threadTs = context.getProperty(THREAD_TS).evaluateAttributeExpressions(flowFile).getValue();
        if (!isEmpty(threadTs)) {
            builder.add("thread_ts", threadTs);
        }

        final String iconEmoji = context.getProperty(ICON_EMOJI).evaluateAttributeExpressions(flowFile).getValue();
        if (!isEmpty(iconEmoji)) {
            builder.add("icon_emoji", iconEmoji);
        }

        try {
            // Get Attachments Array
            if (!attachmentDescriptors.isEmpty()) {
                final JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
                for (final PropertyDescriptor attachmentDescriptor : attachmentDescriptors) {
                    final String attachmentString = context.getProperty(attachmentDescriptor).evaluateAttributeExpressions(flowFile).getValue();
                    final JsonReader reader = Json.createReader(new StringReader(attachmentString));
                    final JsonObject attachmentJson = reader.readObject();
                    jsonArrayBuilder.add(attachmentJson);
                }

                builder.add("attachments", jsonArrayBuilder);
            }

            final JsonObject jsonObject = builder.build();
            final StringWriter stringWriter = new StringWriter();
            final JsonWriter jsonWriter = Json.createWriter(stringWriter);
            jsonWriter.writeObject(jsonObject);
            jsonWriter.close();

            final URL url = URI.create(context.getProperty(WEBHOOK_URL).evaluateAttributeExpressions(flowFile).getValue()).toURL();
            final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            final DataOutputStream outputStream = new DataOutputStream(conn.getOutputStream());
            final String payload = "payload=" + URLEncoder.encode(stringWriter.getBuffer().toString(), StandardCharsets.UTF_8);
            outputStream.writeBytes(payload);
            outputStream.close();

            final int responseCode = conn.getResponseCode();
            if (responseCode >= 200 && responseCode < 300) {
                getLogger().info("Successfully posted message to Slack");
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().send(flowFile, url.toString());
            } else {
                getLogger().error("Failed to post message to Slack with response code {}", responseCode);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                context.yield();
            }
        } catch (final JsonParsingException e) {
            getLogger().error("Failed to parse JSON", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } catch (final IOException e) {
            getLogger().error("Failed to open connection", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private boolean isEmpty(final String value) {
        return value == null || value.isEmpty();
    }

    private static class EmojiValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (input.startsWith(":") && input.endsWith(":") && input.length() > 2) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

            return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                    .explanation("Must begin and end with a colon")
                    .build();
        }
    }
}
