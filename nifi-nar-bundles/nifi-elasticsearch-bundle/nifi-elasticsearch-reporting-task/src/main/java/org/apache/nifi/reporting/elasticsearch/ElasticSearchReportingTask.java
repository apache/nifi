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
package org.apache.nifi.reporting.elasticsearch;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

@Tags({ "reporting", "elasticsearch", "metrics" })
@CapabilityDescription("Publishes metrics from NiFi to elasticsearch.")
public class ElasticSearchReportingTask extends AbstractReportingTask {

  private static final ObjectMapper mapper = new ObjectMapper();

  static final AllowableValue SCHEME_HTTP = new AllowableValue("HTTP", "HTTP", "Metrics will be sent via HTTP");

  static final AllowableValue SCHEME_HTTPS = new AllowableValue("HTTPS", "HTTPS", "Metrics will be sent via HTTPS");

  static final PropertyDescriptor ELASTIC_SCHEME = new PropertyDescriptor.Builder()
      .name("Elastic Scheme")
      .description("Scheme of elastic server to send metrics to")
      .required(true)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .allowableValues(SCHEME_HTTP, SCHEME_HTTPS)
      .defaultValue(SCHEME_HTTP.getValue()).build();

  static final PropertyDescriptor ELASTIC_HOST = new PropertyDescriptor.Builder()
      .name("Elastic Host")
      .description("Hostname of elastic server to send metrics to")
      .required(true)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .defaultValue("elasticsearch").build();

  static final PropertyDescriptor ELASTIC_PORT = new PropertyDescriptor.Builder()
      .name("Elastic Port")
      .description("Port elastic server is listening on")
      .required(false)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .addValidator(StandardValidators.INTEGER_VALIDATOR)
      .defaultValue("9200").build();

  static final PropertyDescriptor ELASTIC_INDEX = new PropertyDescriptor.Builder()
      .name("Elastic Index")
      .description("Index to store metrics in").required(true)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .defaultValue("nifi")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

  private Logger logger = LoggerFactory.getLogger(getClass().getName());

  private RestClient client;

  private String elasticIndex, elasticScheme, elasticHost;

  private int elasticPort;

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(ELASTIC_SCHEME);
    properties.add(ELASTIC_HOST);
    properties.add(ELASTIC_PORT);
    properties.add(ELASTIC_INDEX);
    return properties;
  }

  @OnScheduled
  public void setup(final ConfigurationContext context) {
    elasticIndex = context.getProperty(ELASTIC_INDEX).evaluateAttributeExpressions().getValue();
    elasticScheme = context.getProperty(ELASTIC_SCHEME).evaluateAttributeExpressions().getValue();
    elasticHost = context.getProperty(ELASTIC_HOST).evaluateAttributeExpressions().getValue();
    elasticPort = Integer.valueOf(context.getProperty(ELASTIC_PORT).evaluateAttributeExpressions().getValue());

    client = RestClient.builder(new HttpHost(elasticHost, elasticPort, elasticScheme)).build();
  }

  @Override
  public void onTrigger(ReportingContext context) {
    final ProcessGroupStatus status = context.getEventAccess().getControllerStatus();

    try {
      HttpEntity entity = new NStringEntity(mapper.writeValueAsString(ImmutableMap.of(
          "controllerStatus", status,
          "@timestamp", ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT)
      )), ContentType.APPLICATION_JSON);

      String path = "/" + elasticIndex + "/controllerStatus";

      client.performRequest("POST", path, Collections.emptyMap(), entity);
    } catch (JsonProcessingException e) {
      logger.error("Failure parsing metrics", e);
    } catch (IOException e) {
      logger.warn("Failed to report metrics to elastic", e);
    }
  }

  @OnUnscheduled
  public void shutdown(final ConfigurationContext context) {
    try {
      client.close();
    } catch (IOException e) {
      logger.warn("Failed to cleanly close elastic connection", e);
    }
  }
}
