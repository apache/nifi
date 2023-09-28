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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.slack.api.app_backend.events.payload.EventsApiPayload;
import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.bolt.context.builtin.EventContext;
import com.slack.api.bolt.context.builtin.SlashCommandContext;
import com.slack.api.bolt.request.builtin.SlashCommandRequest;
import com.slack.api.bolt.response.Response;
import com.slack.api.bolt.socket_mode.SocketModeApp;
import com.slack.api.model.event.MessageEvent;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.function.Supplier;
import java.util.regex.Pattern;


@PrimaryNodeOnly
@DefaultSettings(yieldDuration = "250 millis")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Set to application/json, as the output will always be in JSON format")
})
@SeeAlso({ConsumeSlack.class, PostSlack.class, PutSlack.class})
@Tags({"slack", "real-time", "event", "message", "command", "listen", "receive", "social media", "team", "text", "unstructured"})
@CapabilityDescription("Retrieves real-time messages or Slack commands from one or more Slack conversations. The messages are written out in JSON format. " +
    "Note that this Processor should be used to obtain real-time messages and commands from Slack and does not provide a mechanism for obtaining historical messages. " +
    "The ConsumeSlack Processor should be used for an initial load of messages from a channel. " +
    "See Usage / Additional Details for more information about how to configure this Processor and enable it to retrieve messages and commands from Slack.")
public class ListenSlack extends AbstractProcessor {

    private static final ObjectMapper objectMapper;
    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    static final AllowableValue RECEIVE_MESSAGE_EVENTS = new AllowableValue("Receive Message Events", "Receive Message Events",
        "The Processor is to receive Slack Message Events");
    static final AllowableValue RECEIVE_COMMANDS = new AllowableValue("Receive Commands", "Receive Commands",
        "The Processor is to receive Commands from Slack that are specific to your application. The Processor will not receive Message Events.");


    static PropertyDescriptor APP_TOKEN = new PropertyDescriptor.Builder()
        .name("App Token")
        .description("The Application Token that is registered to your Slack application")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(true)
        .sensitive(true)
        .build();

    static PropertyDescriptor BOT_TOKEN = new PropertyDescriptor.Builder()
        .name("Bot Token")
        .description("The Bot Token that is registered to your Slack application")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .required(true)
        .sensitive(true)
        .build();

    static final PropertyDescriptor EVENT_TYPE = new PropertyDescriptor.Builder()
        .name("Event Type to Receive")
        .description("Specifies whether the Processor should receive Slack Message Events or commands issued by users (e.g., /nifi do something)")
        .required(true)
        .defaultValue(RECEIVE_MESSAGE_EVENTS.getValue())
        .allowableValues(RECEIVE_MESSAGE_EVENTS, RECEIVE_COMMANDS)
        .build();


    static Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are created will be sent to this Relationship.")
        .build();

    private final TransferQueue<EventWrapper> eventTransferQueue = new LinkedTransferQueue<>();
    private volatile SocketModeApp socketModeApp;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(
            APP_TOKEN,
            BOT_TOKEN,
            EVENT_TYPE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }


    @OnScheduled
    public void establishWebsocketEndpoint(final ProcessContext context) throws Exception {
        final String appToken = context.getProperty(APP_TOKEN).getValue();
        final String botToken = context.getProperty(BOT_TOKEN).getValue();

        final AppConfig appConfig = AppConfig.builder()
            .singleTeamBotToken(botToken)
            .build();

        final App slackApp = new App(appConfig);

        // Register the event types that make sense
        if (context.getProperty(EVENT_TYPE).getValue().equals(RECEIVE_MESSAGE_EVENTS.getValue())) {
            slackApp.event(MessageEvent.class, this::handleEvent);
        } else {
            slackApp.command(Pattern.compile(".*"), this::handleCommand);
        }

        socketModeApp = new SocketModeApp(appToken, slackApp);
        socketModeApp.startAsync();
    }

    private Response handleEvent(final EventsApiPayload<?> eventsApiPayload, final EventContext context) {
        return handleNotification(eventsApiPayload.getEvent(), context::ack);
    }

    private Response handleCommand(final SlashCommandRequest request, final SlashCommandContext context) {
        return handleNotification(request.getPayload(), context::ack);
    }

    private Response handleNotification(final Object notification, final Supplier<Response> ackSupplier) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        eventTransferQueue.add(new EventWrapper(notification, countDownLatch));

        try {
            final boolean acknowledged = countDownLatch.await(5, TimeUnit.SECONDS);
            return acknowledged ? ackSupplier.get() : Response.error(503);
        } catch (final InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for event to be processed", e);
        }
    }

    @OnStopped
    public void onStopped() throws Exception {
        socketModeApp.stop();
        socketModeApp.close();
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final EventWrapper eventWrapper;
        try {
            eventWrapper = eventTransferQueue.poll(1, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if (eventWrapper == null) {
            context.yield();
            return;
        }

        final Object messageEvent = eventWrapper.getEvent();

        FlowFile flowFile = session.create();
        try (final OutputStream out = session.write(flowFile);
             final JsonGenerator generator = objectMapper.createGenerator(out)) {

            generator.writeObject(messageEvent);
        } catch (final IOException e) {
            getLogger().error("Failed to write out Slack message", e);
            session.remove(flowFile);
            return;
        }

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.getProvenanceReporter().receive(flowFile, socketModeApp.getClient().getWssUri().toString());
        session.transfer(flowFile, REL_SUCCESS);

        // Commit the session asynchronously and upon success allow the message to be acknowledged.
        session.commitAsync(() -> eventWrapper.getCountDownLatch().countDown());
    }


    private static class EventWrapper {
        private final Object event;
        private final CountDownLatch countDownLatch;

        public EventWrapper(final Object event, final CountDownLatch countDownLatch) {
            this.event = event;
            this.countDownLatch = countDownLatch;
        }

        public Object getEvent() {
            return event;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }
    }
}
