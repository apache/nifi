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
package org.apache.nifi.services.slack;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StringUtils;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.stream.JsonParsingException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tags({"slack", "record", "sink"})
@CapabilityDescription("Format and send Records using Slack. This controller service uses Slack Web API methods to post " +
        "messages to a specific channel. Before using SlackRecordSink, you need to create a Slack App, to add a Bot User " +
        "to the app, and to install the app in your Slack workspace. After the app installed, you can get " +
        "the Bot User's OAuth Bearer Token that will be needed to authenticate and authorize " +
        "your SlackRecordSink controller service to Slack.")
public class SlackRecordSink extends AbstractControllerService implements RecordSinkService {

    private static final String SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage";

    public static final PropertyDescriptor POST_MESSAGE_URL = new PropertyDescriptor.Builder()
            .name("post-message-url")
            .displayName("Post Message URL")
            .description("Slack Web API URL for posting text messages to channels." +
                    " It only needs to be changed if Slack changes its API URL.")
            .required(true)
            .defaultValue(SLACK_POST_MESSAGE_URL)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("Access Token")
            .description("Bot OAuth Token used for authenticating/authorizing the Slack request sent by NiFi.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHANNEL_ID = new PropertyDescriptor.Builder()
            .name("channel-id")
            .displayName("Channel ID")
            .description("Slack channel, private group, or IM channel to send the message to. Use Channel ID instead of the name.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private volatile PoolingHttpClientConnectionManager connManager;
    private volatile CloseableHttpClient client;

    private volatile RecordSetWriterFactory writerFactory;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(
                POST_MESSAGE_URL,
                ACCESS_TOKEN,
                CHANNEL_ID,
                RECORD_WRITER_FACTORY
        ));
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        connManager = new PoolingHttpClientConnectionManager();

        client = HttpClientBuilder.create()
                .setConnectionManager(connManager)
                .build();

        this.writerFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
    }

    @OnDisabled
    public void onDisabled() {
        try {
            if (client != null) {
                client.close();
                client = null;
            }
            if (connManager != null) {
                connManager.close();
                connManager = null;
            }
        } catch (IOException e) {
            getLogger().error("Could not properly close HTTP connections.", e);
        }
    }

    @Override
    public WriteResult sendData(final RecordSet recordSet, final Map<String, String> attributes, final boolean sendZeroResults) throws IOException {
        CloseableHttpResponse response = null;
        WriteResult writeResult;
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), recordSet.getSchema(), out, attributes)) {
                writer.beginRecordSet();
                Record record = recordSet.next();
                while (record != null) {
                    writer.write(record);
                    writer.flush();
                    record = recordSet.next();
                }
                writeResult = writer.finishRecordSet();
                writer.flush();
            } catch (final SchemaNotFoundException e) {
                final String errorMessage = String.format("RecordSetWriter could not be created because the schema was not found. The schema name for the RecordSet to write is %s",
                        recordSet.getSchema().getSchemaName());
                throw new ProcessException(errorMessage, e);
            }

            try {
                sendMessage(getConfigurationContext(), out.toString());
            } catch (final SlackRecordSinkException e) {
                getLogger().error("Failed to send message to Slack.", e);
                closeHttpResponse(response);
                throw new ProcessException(e);
            }
        }
        return writeResult;
    }

    private void sendMessage(final ConfigurationContext context, final String message) throws SlackRecordSinkException {
        final JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
        final String channel = context.getProperty(CHANNEL_ID).getValue();

        if (StringUtils.isEmpty(channel)) {
            throw new SlackRecordSinkException("The channel must be specified.");
        }
        jsonBuilder.add("channel", channel);

        if (StringUtils.isEmpty(message)) {
            throw new SlackRecordSinkException("No message to be sent with this record.");
        }
        jsonBuilder.add("text", message);

        final HttpEntity requestBody = new StringEntity(jsonBuilder.build().toString(), Charset.forName("UTF-8"));

        final String url = context.getProperty(POST_MESSAGE_URL).getValue();

        final HttpPost request = new HttpPost(url);
        request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + context.getProperty(ACCESS_TOKEN).getValue());
        request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());

        request.setEntity(requestBody);
        CloseableHttpResponse response = null;
        try {
            response = client.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (!(statusCode >= 200 && statusCode < 300)) {
                throw new SlackRecordSinkException("HTTP error code: " + statusCode);
            }

            final JsonObject responseJson;
            try {
                responseJson = Json.createReader(response.getEntity().getContent()).readObject();
            } catch (JsonParsingException e) {
                throw new SlackRecordSinkException("Slack response JSON cannot be parsed.", e);
            }

            try {
                if (!responseJson.getBoolean("ok")) {
                    throw new SlackRecordSinkException("Slack error response: " + responseJson.getString("error"));
                }
            } catch (NullPointerException | ClassCastException e) {
                throw new SlackRecordSinkException("Slack response JSON does not contain 'ok' key or it has invalid value.", e);
            }

            final JsonString warning = responseJson.getJsonString("warning");
            if (warning != null) {
                getLogger().warn("Slack warning message: " + warning.getString());
            }
        } catch (final IOException e) {
            closeHttpResponse(response);
        }
    }

    private void closeHttpResponse(final CloseableHttpResponse response) {
        if (response != null) {
            try {
                // consume the entire content of the response (entity)
                // so that the manager can release the connection back to the pool
                EntityUtils.consume(response.getEntity());
                response.close();
            } catch (final IOException e) {
                getLogger().error("Could not properly close HTTP response.", e);
            }
        }
    }

    private static class SlackRecordSinkException extends Exception {
        SlackRecordSinkException(String message) {
            super(message);
        }

        SlackRecordSinkException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
