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
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonReader;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.slack.controllers.SlackConnectionService;

@Tags({"slack", "listen", "json", "events"})
@CapabilityDescription("Listens on the provided slack connection controller service and " +
  "forwards the messages in JSON text format.")
@SeeAlso({SlackConnectionService.class, FetchSlack.class})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class ListenSlack extends AbstractSessionFactoryProcessor {

  private volatile ProcessSessionFactory processSessionFactory;

  private volatile SlackConnectionService slackConnectionService;

  private volatile List<String> matchingTypes;

  private volatile boolean matchAny;

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
    .build();

  static final Relationship MATCHED_MESSAGES_RELATIONSHIP = new Relationship.Builder()
    .name("matched")
    .description("Incoming messages that matches the given types")
    .build();

  static final Relationship UNMATCHED_MESSAGES_RELATIONSHIP = new Relationship.Builder()
    .name("unmatched")
    .description("Incoming messages that does not match the given types")
    .autoTerminateDefault(true)
    .build();

  private static final String JSON_OBJECT_TYPE_KEY = "type";

  private static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
    SLACK_CONNECTION_SERVICE, MESSAGE_TYPES));

  private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
    MATCHED_MESSAGES_RELATIONSHIP, UNMATCHED_MESSAGES_RELATIONSHIP)));

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
    String messageTypes = context.getProperty(MESSAGE_TYPES).getValue();
    messageTypes = messageTypes == null ? "" : messageTypes;
    matchAny = messageTypes.isEmpty();
    matchingTypes = Arrays.asList(messageTypes.split(","));
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
  public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
    if (processSessionFactory == null) {
      processSessionFactory = sessionFactory;
    }
    if (!isProcessorRegisteredToService()) {
      registerProcessorToService(context);
    }
    context.yield();
  }

  private boolean isProcessorRegisteredToService() {
    return slackConnectionService != null
      && slackConnectionService.isProcessorRegistered(this);
  }

  private void registerProcessorToService(ProcessContext context) {
      try {
        slackConnectionService = context.getProperty(SLACK_CONNECTION_SERVICE)
          .asControllerService(SlackConnectionService.class);
        slackConnectionService.registerProcessor(this, getMessageHandler(processSessionFactory));
      } catch (Exception e) {
        getLogger().error("Error while creating slack client", e);
        slackConnectionService.deregisterProcessor(this);
        context.yield();
      }
  }

  private Consumer<String> getMessageHandler(ProcessSessionFactory sessionFactory) {
    return message -> {
      ProcessSession session = sessionFactory.createSession();
      try {
        FlowFile flowFile = session.create();
        session.write(flowFile, outputStream -> outputStream.write(message.getBytes()));
        session.getProvenanceReporter().receive(flowFile, "slack");

        if (matches(message)) {
          session.transfer(flowFile, MATCHED_MESSAGES_RELATIONSHIP);
        } else {
          session.transfer(flowFile, UNMATCHED_MESSAGES_RELATIONSHIP);
        }
        session.commit();
      } catch (Exception e) {
        session.rollback();
      }
    };
  }

  private boolean matches(String message) {
    if (matchAny) {
      return true;
    }

    try (JsonReader reader = Json.createReader(new StringReader(message))) {
      return Optional.ofNullable(reader.readObject())
        .map(jsonObject -> jsonObject.getString(JSON_OBJECT_TYPE_KEY, null))
        .filter(Objects::nonNull)
        .map(matchingTypes::contains)
        .orElse(false);
    } catch (JsonException e) {
      getLogger().warn("Error while parsing message:", e);
    }

    return false;
  }
}
