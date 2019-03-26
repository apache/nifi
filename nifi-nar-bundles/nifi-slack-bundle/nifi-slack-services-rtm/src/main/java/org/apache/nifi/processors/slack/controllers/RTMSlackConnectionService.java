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
package org.apache.nifi.processors.slack.controllers;

import com.github.seratch.jslack.Slack;
import com.github.seratch.jslack.api.rtm.RTMClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.websocket.DeploymentException;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.util.StandardValidators;
@CapabilityDescription("Implementation of SlackConnectionService." +
  "This service connects to the Real Time Messaging API of slack.")
public class RTMSlackConnectionService extends AbstractControllerService implements SlackConnectionService {

  private static final PropertyDescriptor API_TOKEN = new PropertyDescriptor.Builder()
    .name("api-token")
    .displayName("API token")
    .description("Slack auth token required for Real Time Messaging API")
    .sensitive(true)
    .required(true)
    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  private ConcurrentHashMap<String, Consumer<String>> map = new ConcurrentHashMap<>();

  private RTMClient rtmClient;

  @OnEnabled
  public void startClient(ConfigurationContext context) {
    try {
      rtmClient = getRtmClient(context);
      rtmClient.addMessageHandler(this::sendMessage);
      rtmClient.addCloseHandler(closeReason -> getLogger().info("Slack RTM Client closed: " + closeReason.toString()));
      rtmClient.addErrorHandler(throwable -> getLogger().error("Slack RTM Client error:", throwable));
      rtmClient.connect();
    } catch (IOException|DeploymentException e) {
      throw new RuntimeException("Error while starting RTM client", e);
    }

  }

  RTMClient getRtmClient(ConfigurationContext context) throws IOException {
    return new Slack().rtm(context.getProperty(API_TOKEN).evaluateAttributeExpressions().getValue());
  }

  @OnDisabled
  @OnShutdown
  public void stopClient() {
    if (rtmClient != null) {
      try {
        rtmClient.close();
        rtmClient = null;
      } catch (IOException e) {
        getLogger().error("Error while closing Slack RTM client", e);
      }
    }
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return Collections.singletonList(API_TOKEN);
  }

  @Override
  public void registerProcessor(Processor processor, Consumer<String> messageHandler) {
    map.putIfAbsent(processor.getIdentifier(), messageHandler);
  }

  @Override
  public boolean isProcessorRegistered(Processor processor) {
    return map.containsKey(processor.getIdentifier());
  }

  @Override
  public void deregisterProcessor(Processor processor) {
    map.remove(processor.getIdentifier());
  }

  @Override
  public void sendMessage(String message) {
    map.values().forEach(stringConsumer -> stringConsumer.accept(message));
  }
}
