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

import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.slack.controllers.SlackConnectionService;

@Tags({"slack", "listen", "json", "events"})
@CapabilityDescription("Listens on the provided slack connection controller service and " +
  "forwards the messages in JSON text format.")
@SeeAlso({SlackConnectionService.class, FetchSlack.class})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@PrimaryNodeOnly
@TriggerSerially
public class ConsumeSlack extends AbstractProcessor {

  private volatile SlackConnectionService slackConnectionService;

  private volatile List<String> matchingTypes;

  private volatile boolean matchAny;

  private BlockingQueue<SlackMessage> messageQueue;

  static final PropertyDescriptor SLACK_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
    .name("slack-connection-service")
    .displayName("Slack Connection ControllerService")
    .description("A ControllerService that provides a connection to slack service.")
    .required(true)
    .identifiesControllerService(SlackConnectionService.class)
    .build();

  static final PropertyDescriptor MESSAGE_TYPES = new PropertyDescriptor
    .Builder()
    .name("message-types")
    .displayName("Message types")
    .description("Message types to listen to. It will filter messages with the given types or " +
      "listen to every type if empty. It is a coma separated list like: message,file_shared")
    .required(false)
    .addValidator(Validator.VALID)
    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
    .build();

  public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder()
    .name("Max Size of Message Queue")
    .description("The maximum size of the internal queue used to buffer messages being transferred from the underlying channel to the processor. " +
      "Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total " +
      "memory used by the processor.")
    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
    .defaultValue("10000")
    .required(true)
    .build();

  static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
    .name("success")
    .description("Incoming messages that matches the given types")
    .build();

  private static final String JSON_OBJECT_TYPE_KEY = "type";

  private static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
    SLACK_CONNECTION_SERVICE, MESSAGE_TYPES, MAX_MESSAGE_QUEUE_SIZE));

  private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
    SUCCESS_RELATIONSHIP)));

  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return DESCRIPTORS;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    String messageTypes = context.getProperty(MESSAGE_TYPES).evaluateAttributeExpressions().getValue();
    messageTypes = messageTypes == null ? "" : messageTypes;
    matchAny = messageTypes.isEmpty();
    matchingTypes = Arrays.asList(messageTypes.split(","));
    messageQueue = new LinkedBlockingQueue<>(context.getProperty(MAX_MESSAGE_QUEUE_SIZE).asInteger());
  }

  @OnUnscheduled
  @OnShutdown
  public void onUnscheduled() {
    if (isProcessorRegisteredToService()) {
      deregister();
    }
  }

  private void deregister() {
    slackConnectionService.deregisterProcessor(this);
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    if (!isProcessorRegisteredToService()) {
      registerProcessorToService(context);
    }

    SlackMessage slackMessage = messageQueue.poll();

    if (slackMessage != null) {
      FlowFile flowFile = session.create();
      session.write(flowFile, outputStream -> outputStream.write(slackMessage.getJson().getBytes()));
      session.getProvenanceReporter().receive(flowFile, "slack");
      session.putAttribute(flowFile, "mime.type", "application/json");
      session.putAttribute(flowFile, "slack.type", String.valueOf(slackMessage.getType()));
      session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    } else {
      context.yield();
    }
  }

  private boolean isProcessorRegisteredToService() {
    return slackConnectionService != null
      && slackConnectionService.isProcessorRegistered(this);
  }

  private void registerProcessorToService(ProcessContext context) {
      try {
        slackConnectionService = context.getProperty(SLACK_CONNECTION_SERVICE)
          .asControllerService(SlackConnectionService.class);
        slackConnectionService.registerProcessor(this, getMessageHandler());
      } catch (Exception e) {
        getLogger().error("Error while creating slack client", e);
        slackConnectionService.deregisterProcessor(this);
        context.yield();
      }
  }

  private Consumer<String> getMessageHandler() {
    return message -> {
      try {
        String messageType = getMessageType(message);
        if (matches(messageType)) {
          boolean queued = messageQueue.offer(new SlackMessage(messageType, message), 100, TimeUnit.MILLISECONDS);
          if (!queued) {
            getLogger().error("Internal queue at maximum capacity, could not queue event");
          }
        }
      } catch (InterruptedException ie) {

      }
    };
  }

  private boolean matches(String type) {
    return matchAny || matchingTypes.contains(type);
  }

  private String getMessageType(String message) {
    try (JsonReader reader = Json.createReader(new StringReader(message))) {
      JsonObject jsonObject = reader.readObject();
      if (Objects.nonNull(jsonObject)) {
        return jsonObject.getString(JSON_OBJECT_TYPE_KEY, null);
      }
    } catch (JsonException e) {
      getLogger().warn("Error while parsing message:", e);
    }
    return null;
  }

  private static class SlackMessage {
    String type;
    String json;

    public SlackMessage(String type, String json) {
      this.type = type;
      this.json = json;
    }

    public String getType() {
      return type;
    }

    public String getJson() {
      return json;
    }
  }
}
