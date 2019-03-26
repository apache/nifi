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


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"slack", "download", "fetch", "attachment"})
@CapabilityDescription("Downloading attachments from slack messages." +
  "The input is the JSON string created by ConsumeSlack, the output is a new" +
  "FlowFile with the content of the downloaded file and attributes are added" +
  "based on the original message and the HTTP response.")
@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
  @WritesAttribute(attribute = "response.filename", description = "File name parsed from HTTP response."),
  @WritesAttribute(attribute = "response.content-type", description = "Content-Type parsed from HTTP response."),
  @WritesAttribute(attribute = "response.content-length", description = "Content-Length parsed from HTTP response."),
  @WritesAttribute(attribute = "response.date", description = "Date parsed from HTTP response."),
  @WritesAttribute(attribute = "slack.file.name", description = "File name in the slack JSON."),
  @WritesAttribute(attribute = "slack.file.mimetype", description = "Mime type in the slack JSON."),
  @WritesAttribute(attribute = "slack.file.filetype", description = "File type in the slack JSON."),
  @WritesAttribute(attribute = "slack.file.id", description = "File id in the slack JSON."),
  @WritesAttribute(attribute = "slack.file.title", description = "File title in the slack JSON."),
  @WritesAttribute(attribute = "slack.file.size", description = "File size in the slack JSON."),
  @WritesAttribute(attribute = "slack.file.created", description = "Created timestamp in the slack JSON.")
})
public class FetchSlack extends AbstractProcessor {

  private volatile CloseableHttpClient client;
  private static final Pattern filenamePattern = Pattern.compile(".*filename=\"([^\"]+)\".*");

  static final PropertyDescriptor API_TOKEN = new PropertyDescriptor.Builder()
    .name("api-token")
    .displayName("API Token")
    .description("Token for slack to use when downloading attachments.")
    .required(true)
    .sensitive(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
    .build();

  static final Relationship REL_SUCCESS_REQ = new Relationship.Builder()
    .name("Original")
    .description("The original FlowFile will be routed upon success (2xx status codes)")
    .build();

  static final Relationship REL_RESPONSE = new Relationship.Builder()
    .name("Response")
    .description("A Response FlowFile will be routed upon success (2xx status codes)")
    .build();

  static final Relationship REL_RETRY = new Relationship.Builder()
    .name("Retry")
    .description("The original FlowFile will be routed on any status code that can be retried (5xx status codes)")
    .build();

  static final Relationship REL_NO_RETRY = new Relationship.Builder()
    .name("No Retry")
    .description("The original FlowFile will be routed on any status code that should NOT be retried (1xx, 3xx, 4xx status codes)")
    .build();

  static final Relationship REL_FAILURE = new Relationship.Builder()
    .name("Failure")
    .description("The original FlowFile will be routed on any type of connection failure, timeout or general exception.")
    .build();

  private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
    REL_SUCCESS_REQ, REL_RESPONSE, REL_RETRY, REL_NO_RETRY, REL_FAILURE)));

  private static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
    API_TOKEN));

  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return DESCRIPTORS;
  }

  @OnScheduled
  public void createClient(final ProcessContext context) {
    Header header = new BasicHeader(
      HttpHeaders.AUTHORIZATION, "Bearer " + context.getProperty(API_TOKEN).evaluateAttributeExpressions().getValue());
    List<Header> headers = Collections.singletonList(header);
    client = HttpClients.custom().setDefaultHeaders(headers).build();
  }

  @OnUnscheduled
  @OnShutdown
  public void releaseClient() {
    try {
      client.close();
    } catch (IOException e) {
      getLogger().warn("Error while closing HttpClient", e);
    }
    client = null;
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    FlowFile requestFlowFile = session.get();
    if (requestFlowFile == null) {
      context.yield();

      return;
    }

    try {
      List<JsonObject> filesToDownload;
      try (InputStream requestInputStream = session.read(requestFlowFile)) {
        filesToDownload = collectFilesToDownload(requestInputStream);
      }
      if (filesToDownload == null || filesToDownload.isEmpty()) {
        session.transfer(requestFlowFile, REL_SUCCESS_REQ);
      } else {
        downloadFiles(session, requestFlowFile, filesToDownload);
      }
    } catch (JsonException | IOException e) {
      session.transfer(requestFlowFile, REL_FAILURE);
    }
  }

  private void downloadFiles(ProcessSession session, FlowFile requestFlowFile, List<JsonObject> filesToDownload) {
    try {
      ArrayList<FlowFile> successfulResponses = new ArrayList<>();
      for (JsonObject file : filesToDownload) {
        String url = file.getString("url_private_download");

        HttpUriRequest request = RequestBuilder.get().setUri(url).build();
        HttpResponse response = client.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();

        if (inStatusCodeFamily(200, statusCode)) {
          FlowFile responseFlowFile = session.create(requestFlowFile);
          session.write(responseFlowFile, out -> response.getEntity().writeTo(out));

          Map<String, String> attributes = fillAttributes(requestFlowFile.getAttributes(), file, response);
          responseFlowFile = session.putAllAttributes(responseFlowFile, attributes);

          session.getProvenanceReporter().fetch(responseFlowFile, url);
          successfulResponses.add(responseFlowFile);
        } else if (inStatusCodeFamily(100, statusCode)
          || inStatusCodeFamily(300, statusCode)
          || inStatusCodeFamily(400, statusCode)) {
          session.transfer(requestFlowFile, REL_NO_RETRY);
          break;
        } else if (inStatusCodeFamily(500, statusCode)) {
          session.transfer(requestFlowFile, REL_RETRY);
          break;
        }
      }

      if (successfulResponses.size() == filesToDownload.size()) {
        session.transfer(successfulResponses, REL_RESPONSE);
        session.transfer(requestFlowFile, REL_SUCCESS_REQ);
      } else {
        successfulResponses.forEach(session::remove);
      }
    } catch (IOException e) {
      session.transfer(requestFlowFile, REL_FAILURE);
    }
  }

  private List<JsonObject> collectFilesToDownload(InputStream requestInputStream) {
    List<JsonObject> filesToDownload = new ArrayList<>();
    try (JsonReader reader = Json.createReader(requestInputStream)) {
      JsonObject jsonObject = reader.readObject();
      JsonArray files = jsonObject.getJsonArray("files");
      if (files != null) {
        for (JsonValue file : files) {
          String url = ((JsonObject) file).getString("url_private_download");
          if (url != null) {
            filesToDownload.add((JsonObject) file);
          }
        }
      }
    }

    return filesToDownload;
  }

  private HashMap<String, String> fillAttributes(Map<String, String> attributes, JsonObject file, HttpResponse response) {
    HashMap<String, String> map = new HashMap<>(attributes);
    String value = response.getFirstHeader("Content-Disposition").getValue();
    Matcher matcher = filenamePattern.matcher(value);
    if (matcher.matches()) {
      map.put("response.filename", matcher.group(1));
    }

    fromheader(map, response, "Content-Length", "response.content-length");
    fromheader(map, response, "Content-type", "response.content-type");
    fromheader(map, response, "Date", "response.date");

    fromJson(map, file, "name", "slack.file.name");
    fromJson(map, file, "mimetype", "slack.file.mimetype");
    fromJson(map, file, "filetype", "slack.file.filetype");
    fromJson(map, file, "title", "slack.file.title");
    fromJson(map, file, "id", "slack.file.id");
    fromJson(map, file, "created", "slack.file.created");
    fromJson(map, file, "size", "slack.file.size");

    return map;
  }

  private void fromJson(Map<String, String> attributes, JsonObject file, String jsonKey, String attribute) {
    Optional.ofNullable(file.getOrDefault(jsonKey, null)).ifPresent(
      jsonValue -> {
        ValueType valueType = jsonValue.getValueType();
        String stringValue = null;
        if (valueType.equals(ValueType.NUMBER)) {
          stringValue = String.valueOf(file.getInt(jsonKey));
        } else if (valueType.equals(ValueType.STRING)) {
          stringValue = file.getString(jsonKey);
        }

        if (stringValue != null) {
          attributes.put(attribute, stringValue);
        }
      }
    );

  }

  private void fromheader(Map<String, String> attributes, HttpResponse response, String header, String attribute) {
    Header firstHeader = response.getFirstHeader(header);
    if (firstHeader != null)
      attributes.put(attribute, firstHeader.getValue());
  }

  private boolean inStatusCodeFamily(int family, int statusCode) {
    return statusCode >= family && statusCode < family + 100;
  }

}

