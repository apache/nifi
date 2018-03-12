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
package org.apache.nifi.processors.riemann;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.client.RiemannClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"riemann", "monitoring", "metrics"})
@DynamicProperty(name = "Custom Event Attribute", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
  description = "These values will be attached to the Riemann event as a custom attribute",
  value = "Any value or expression")
@CapabilityDescription("Send events to Riemann (http://riemann.io) when FlowFiles pass through this processor. " +
  "You can use events to notify Riemann that a FlowFile passed through, or you can attach a more " +
  "meaningful metric, such as, the time a FlowFile took to get to this processor. All attributes attached to " +
  "events support the NiFi Expression Language.")
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
public class PutRiemann extends AbstractProcessor {
  protected enum Transport {
    TCP, UDP
  }

  protected volatile RiemannClient riemannClient = null;
  protected volatile Transport transport;

  public static final Relationship REL_SUCCESS = new Relationship.Builder()
    .name("success")
    .description("Metrics successfully written to Riemann")
    .build();

  public static final Relationship REL_FAILURE = new Relationship.Builder()
    .name("failure")
    .description("Metrics which failed to write to Riemann")
    .build();


  public static final PropertyDescriptor RIEMANN_HOST = new PropertyDescriptor.Builder()
    .name("Riemann Address")
    .description("Hostname of Riemann server")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor RIEMANN_PORT = new PropertyDescriptor.Builder()
    .name("Riemann Port")
    .description("Port that Riemann is listening on")
    .required(true)
    .defaultValue("5555")
    .addValidator(StandardValidators.PORT_VALIDATOR)
    .build();

  public static final PropertyDescriptor TRANSPORT_PROTOCOL = new PropertyDescriptor.Builder()
    .name("Transport Protocol")
    .description("Transport protocol to speak to Riemann in")
    .required(true)
    .allowableValues(new Transport[]{Transport.TCP, Transport.UDP})
    .defaultValue("TCP")
    .build();

  public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
    .name("Batch Size")
    .description("Batch size for incoming FlowFiles")
    .required(false)
    .defaultValue("100")
    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
    .build();

  // Attributes Mappings
  public static final PropertyDescriptor ATTR_SERVICE = new PropertyDescriptor.Builder()
    .name("Service")
    .description("Name of service associated to this event (e.g. FTP File Fetched)")
    .required(false)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(Validator.VALID)
    .build();

  public static final PropertyDescriptor ATTR_STATE = new PropertyDescriptor.Builder()
    .name("State")
    .description("State of service associated to this event in string form (e.g. ok, warning, foo)")
    .required(false)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(Validator.VALID)
    .build();

  public static final PropertyDescriptor ATTR_TIME = new PropertyDescriptor.Builder()
    .name("Time")
    .description("Time of event in unix epoch seconds (long), default: (current time)")
    .required(false)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(Validator.VALID)
    .build();

  public static final PropertyDescriptor ATTR_HOST = new PropertyDescriptor.Builder()
    .name("Host")
    .description("A hostname associated to this event (e.g. nifi-app1)")
    .required(false)
    .defaultValue("${hostname()}")
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(Validator.VALID)
    .build();

  public static final PropertyDescriptor ATTR_TTL = new PropertyDescriptor.Builder()
    .name("TTL")
    .description("Floating point value in seconds until Riemann considers this event as \"expired\"")
    .required(false)
    .addValidator(Validator.VALID)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .build();

  public static final PropertyDescriptor ATTR_METRIC = new PropertyDescriptor.Builder()
    .name("Metric")
    .description("Floating point number associated to this event")
    .required(false)
    .addValidator(Validator.VALID)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .build();

  public static final PropertyDescriptor ATTR_DESCRIPTION = new PropertyDescriptor.Builder()
    .name("Description")
    .description("Description associated to the event")
    .required(false)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(Validator.VALID)
    .build();


  public static final PropertyDescriptor ATTR_TAGS = new PropertyDescriptor.Builder()
    .name("Tags")
    .description("Comma separated list of tags associated to the event")
    .required(false)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .addValidator(Validator.VALID)
    .build();

  public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
    .name("Timeout")
    .description("Timeout in milliseconds when writing events to Riemann")
    .required(true)
    .defaultValue("1000")
    .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
    .build();

  private volatile List<PropertyDescriptor> customAttributes = new ArrayList<>();
  private static final Set<Relationship> RELATIONSHIPS = new HashSet<>();
  private static final List<PropertyDescriptor> LOCAL_PROPERTIES = new ArrayList<>();

  private volatile int batchSize = -1;
  private volatile long writeTimeout = 1000;

  static {
    RELATIONSHIPS.add(REL_SUCCESS);
    RELATIONSHIPS.add(REL_FAILURE);
    LOCAL_PROPERTIES.add(RIEMANN_HOST);
    LOCAL_PROPERTIES.add(RIEMANN_PORT);
    LOCAL_PROPERTIES.add(TRANSPORT_PROTOCOL);
    LOCAL_PROPERTIES.add(TIMEOUT);
    LOCAL_PROPERTIES.add(BATCH_SIZE);
    LOCAL_PROPERTIES.add(ATTR_DESCRIPTION);
    LOCAL_PROPERTIES.add(ATTR_SERVICE);
    LOCAL_PROPERTIES.add(ATTR_STATE);
    LOCAL_PROPERTIES.add(ATTR_METRIC);
    LOCAL_PROPERTIES.add(ATTR_TTL);
    LOCAL_PROPERTIES.add(ATTR_TAGS);
    LOCAL_PROPERTIES.add(ATTR_HOST);
    LOCAL_PROPERTIES.add(ATTR_TIME);
  }


  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return LOCAL_PROPERTIES;
  }

  @Override
  protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
    return new PropertyDescriptor.Builder()
      .name(propertyDescriptorName)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .addValidator(Validator.VALID)
      .required(false)
      .dynamic(true)
      .build();
  }

  @OnStopped
  public final void cleanUpClient() {
    if (riemannClient != null) {
      this.riemannClient.close();
    }
    this.riemannClient = null;
    this.batchSize = -1;
    this.customAttributes.clear();
  }

  @OnScheduled
  public void onScheduled(ProcessContext context) throws ProcessException {
    if (batchSize == -1) {
      batchSize = context.getProperty(BATCH_SIZE).asInteger();
    }
    if (riemannClient == null || !riemannClient.isConnected()) {
      transport = Transport.valueOf(context.getProperty(TRANSPORT_PROTOCOL).getValue());
      String host = context.getProperty(RIEMANN_HOST).getValue().trim();
      int port = context.getProperty(RIEMANN_PORT).asInteger();
      writeTimeout = context.getProperty(TIMEOUT).asLong();
      RiemannClient client = null;
      try {
        switch (transport) {
          case TCP:
            client = RiemannClient.tcp(host, port);
            break;
          case UDP:
            client = RiemannClient.udp(host, port);
            break;
        }
        client.connect();
        riemannClient = client;
      } catch (IOException e) {
        if (client != null) {
          client.close();
        }
        context.yield();
        throw new ProcessException(String.format("Unable to connect to Riemann [%s:%d] (%s)\n%s", host, port, transport, e.getMessage()));
      }
    }

    if (customAttributes.size() == 0) {
      for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
        // only custom defined properties
        if (!getSupportedPropertyDescriptors().contains(property.getKey())) {
          customAttributes.add(property.getKey());
        }
      }
    }
  }


  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    // Check if the client is currently connected, as a previous trigger could have detected a failure
    // in the connection.
    if (riemannClient == null || !riemannClient.isConnected()) {
      // clean up the client and attempt to re-initialize the processor
      cleanUpClient();
      onScheduled(context);
    }

    List<FlowFile> incomingFlowFiles = session.get(batchSize);
    List<FlowFile> successfulFlowFiles = new ArrayList<>(incomingFlowFiles.size());
    List<Event> eventsQueue = new ArrayList<>(incomingFlowFiles.size());
    for (FlowFile flowFile : incomingFlowFiles) {
      try {
        eventsQueue.add(FlowFileToEvent.fromAttributes(context, customAttributes, flowFile));
        successfulFlowFiles.add(flowFile);
      } catch (NumberFormatException e) {
        getLogger().warn("Unable to create Riemann event.", e);
        session.transfer(flowFile, REL_FAILURE);
      }
    }
    try {
      if (transport == Transport.TCP) {
        Proto.Msg returnMessage = riemannClient.sendEvents(eventsQueue).deref(writeTimeout, TimeUnit.MILLISECONDS);
        if (returnMessage == null) {
          context.yield();
          throw new ProcessException("Timed out writing to Riemann!");
        }
      } else {
        riemannClient.sendEvents(eventsQueue);
      }
      riemannClient.flush();
      session.transfer(successfulFlowFiles, REL_SUCCESS);
      session.commit();
    } catch (Exception e) {
      context.yield();
      session.transfer(incomingFlowFiles);
      session.commit();
      throw new ProcessException("Failed writing to Riemann\n" + e.getMessage());
    }
  }

  /**
   * Converts a FlowFile into a Riemann Protobuf Event
   */
  private static class FlowFileToEvent {
    protected static Event fromAttributes(ProcessContext context, List<PropertyDescriptor> customProperties,
                                          FlowFile flowFile) {
      Event.Builder builder = Event.newBuilder();

      PropertyValue service = context.getProperty(ATTR_SERVICE).evaluateAttributeExpressions(flowFile);
      if (StringUtils.isNotBlank(service.getValue())) {
        builder.setService(service.getValue());
      }
      PropertyValue description = context.getProperty(ATTR_DESCRIPTION).evaluateAttributeExpressions(flowFile);
      if (StringUtils.isNotBlank(description.getValue())) {
        builder.setDescription(description.getValue());
      }
      PropertyValue metric = context.getProperty(ATTR_METRIC).evaluateAttributeExpressions(flowFile);
      if (StringUtils.isNotBlank(metric.getValue())) {
        builder.setMetricF(metric.asFloat());
      }
      PropertyValue time = context.getProperty(ATTR_TIME).evaluateAttributeExpressions(flowFile);
      if (StringUtils.isNotBlank(time.getValue())) {
        builder.setTime(time.asLong());
      }
      PropertyValue state = context.getProperty(ATTR_STATE).evaluateAttributeExpressions(flowFile);
      if (StringUtils.isNotBlank(state.getValue())) {
        builder.setState(state.getValue());
      }
      PropertyValue ttl = context.getProperty(ATTR_TTL).evaluateAttributeExpressions(flowFile);
      if (StringUtils.isNotBlank(ttl.getValue())) {
        builder.setTtl(ttl.asFloat());
      }
      PropertyValue host = context.getProperty(ATTR_HOST).evaluateAttributeExpressions(flowFile);
      if (StringUtils.isNotBlank(host.getValue())) {
        builder.setHost(host.getValue());
      }
      PropertyValue tags = context.getProperty(ATTR_TAGS).evaluateAttributeExpressions(flowFile);
      if (StringUtils.isNotBlank(tags.getValue())) {
        String[] splitTags = tags.getValue().split(",");
        for (String splitTag : splitTags) {
          builder.addTags(splitTag.trim());
        }
      }
      PropertyValue customAttributeValue;
      for (PropertyDescriptor customProperty : customProperties) {
        customAttributeValue = context.getProperty(customProperty).evaluateAttributeExpressions(flowFile);
        if (StringUtils.isNotBlank(customAttributeValue.getValue())) {
          builder.addAttributes(Proto.Attribute.newBuilder()
            .setKey(customProperty.getName())
            .setValue(customAttributeValue.getValue())
            .build());
        }
      }
      return builder.build();
    }
  }
}
