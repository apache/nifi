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

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.stream.JsonParsingException;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Tags({"slack", "post", "notify", "upload", "message"})
@CapabilityDescription("Sends a message on Slack. The FlowFile content (e.g. an image) can be uploaded and attached to the message.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "<Arbitrary name>", value = "JSON snippet specifying a Slack message \"attachment\"",
        description = "The property value will be converted to JSON and will be added to the array of attachments in the JSON payload being sent to Slack." +
                " The property name will not be used by the processor.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@WritesAttribute(attribute="slack.file.url", description = "The Slack URL of the uploaded file. It will be added if 'Upload FlowFile' has been set to 'Yes'.")
public class PostSlack extends AbstractProcessor {

    private static final String SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage";

    private static final String SLACK_FILE_UPLOAD_URL = "https://slack.com/api/files.upload";

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

    public static final PropertyDescriptor FILE_UPLOAD_URL = new PropertyDescriptor.Builder()
            .name("file-upload-url")
            .displayName("File Upload URL")
            .description("Slack Web API URL for uploading files to channels." +
                    " It only needs to be changed if Slack changes its API URL.")
            .required(true)
            .defaultValue(SLACK_FILE_UPLOAD_URL)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("Access Token")
            .description("OAuth Access Token used for authenticating/authorizing the Slack request sent by NiFi.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor.Builder()
            .name("channel")
            .displayName("Channel")
            .description("Slack channel, private group, or IM channel to send the message to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TEXT = new PropertyDescriptor.Builder()
            .name("text")
            .displayName("Text")
            .description("Text of the Slack message to send. Only required if no attachment has been specified and 'Upload File' has been set to 'No'.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final AllowableValue UPLOAD_FLOWFILE_YES = new AllowableValue(
            "true",
            "Yes",
            "Upload and attach FlowFile content to the Slack message."
    );

    public static final AllowableValue UPLOAD_FLOWFILE_NO = new AllowableValue(
            "false",
            "No",
            "Don't upload and attach FlowFile content to the Slack message."
    );

    public static final PropertyDescriptor UPLOAD_FLOWFILE = new PropertyDescriptor.Builder()
            .name("upload-flowfile")
            .displayName("Upload FlowFile")
            .description("Whether or not to upload and attach the FlowFile content to the Slack message.")
            .allowableValues(UPLOAD_FLOWFILE_YES, UPLOAD_FLOWFILE_NO)
            .required(true)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor FILE_TITLE = new PropertyDescriptor.Builder()
            .name("file-title")
            .displayName("File Title")
            .description("Title of the file displayed in the Slack message." +
                    " The property value will only be used if 'Upload FlowFile' has been set to 'Yes'.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("file-name")
            .displayName("File Name")
            .description("Name of the file to be uploaded." +
                    " The property value will only be used if 'Upload FlowFile' has been set to 'Yes'." +
                    " If the property evaluated to null or empty string, then the file name will be set to 'file' in the Slack message.")
            .defaultValue("${" + CoreAttributes.FILENAME.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FILE_MIME_TYPE = new PropertyDescriptor.Builder()
            .name("file-mime-type")
            .displayName("File Mime Type")
            .description("Mime type of the file to be uploaded." +
                    " The property value will only be used if 'Upload FlowFile' has been set to 'Yes'." +
                    " If the property evaluated to null or empty string, then the mime type will be set to 'application/octet-stream' in the Slack message.")
            .defaultValue("${" + CoreAttributes.MIME_TYPE.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent to Slack")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent to Slack")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(POST_MESSAGE_URL, FILE_UPLOAD_URL, ACCESS_TOKEN, CHANNEL, TEXT, UPLOAD_FLOWFILE, FILE_TITLE, FILE_NAME, FILE_MIME_TYPE));

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    private final SortedSet<PropertyDescriptor> attachmentProperties = Collections.synchronizedSortedSet(new TreeSet<>());

    private volatile PoolingHttpClientConnectionManager connManager;
    private volatile CloseableHttpClient client;

    private static final ContentType MIME_TYPE_PLAINTEXT_UTF8 = ContentType.create("text/plain", Charset.forName("UTF-8"));

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Slack Attachment JSON snippet that will be added to the message. The property value will only be used if 'Upload FlowFile' has been set to 'No'." +
                        " If the property evaluated to null or empty string, or contains invalid JSON, then it will not be added to the Slack message.")
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void initDynamicProperties(ProcessContext context) {
        attachmentProperties.clear();
        attachmentProperties.addAll(
                context.getProperties().keySet()
                        .stream()
                        .filter(PropertyDescriptor::isDynamic)
                        .collect(Collectors.toList()));
    }

    @OnScheduled
    public void initHttpResources() {
        connManager = new PoolingHttpClientConnectionManager();

        client = HttpClientBuilder.create()
                .setConnectionManager(connManager)
                .build();
    }

    @OnStopped
    public void closeHttpResources() {
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> validationResults = new ArrayList<>();

        boolean textSpecified = validationContext.getProperty(TEXT).isSet();
        boolean attachmentSpecified = validationContext.getProperties().keySet()
                .stream()
                .anyMatch(PropertyDescriptor::isDynamic);
        boolean uploadFileYes = validationContext.getProperty(UPLOAD_FLOWFILE).asBoolean();

        if (!textSpecified && !attachmentSpecified && !uploadFileYes) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(TEXT.getDisplayName())
                    .valid(false)
                    .explanation("it is required if no attachment has been specified, nor 'Upload FlowFile' has been set to 'Yes'.")
                    .build());
        }

        return validationResults;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        CloseableHttpResponse response = null;
        try {
            String url;
            String contentType;
            HttpEntity requestBody;

            if (!context.getProperty(UPLOAD_FLOWFILE).asBoolean()) {
                url = context.getProperty(POST_MESSAGE_URL).getValue();
                contentType = ContentType.APPLICATION_JSON.toString();
                requestBody = createTextMessageRequestBody(context, flowFile);
            } else {
                url = context.getProperty(FILE_UPLOAD_URL).getValue();
                contentType = null; // it will be set implicitly by HttpClient in case of multipart post request
                requestBody = createFileMessageRequestBody(context, session, flowFile);
            }

            HttpPost request = new HttpPost(url);
            request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + context.getProperty(ACCESS_TOKEN).getValue());
            if (contentType != null) {
                request.setHeader(HttpHeaders.CONTENT_TYPE, contentType);
            }
            request.setEntity(requestBody);

            response = client.execute(request);

            int statusCode = response.getStatusLine().getStatusCode();
            getLogger().debug("Status code: " + statusCode);

            if (!(statusCode >= 200 && statusCode < 300)) {
                throw new PostSlackException("HTTP error code: " + statusCode);
            }

            JsonObject responseJson;
            try {
                responseJson = Json.createReader(response.getEntity().getContent()).readObject();
            } catch (JsonParsingException e) {
                throw new PostSlackException("Slack response JSON cannot be parsed.", e);
            }

            getLogger().debug("Slack response: " + responseJson.toString());

            try {
                if (!responseJson.getBoolean("ok")) {
                    throw new PostSlackException("Slack error response: " + responseJson.getString("error"));
                }
            } catch (NullPointerException | ClassCastException e) {
                throw new PostSlackException("Slack response JSON does not contain 'ok' key or it has invalid value.", e);
            }

            JsonString warning = responseJson.getJsonString("warning");
            if (warning != null) {
                getLogger().warn("Slack warning message: " + warning.getString());
            }

            if (context.getProperty(UPLOAD_FLOWFILE).asBoolean()) {
                JsonObject file = responseJson.getJsonObject("file");
                if (file != null) {
                    JsonString fileUrl = file.getJsonString("url_private");
                    if (fileUrl != null) {
                        session.putAttribute(flowFile, "slack.file.url", fileUrl.getString());
                    }
                }
            }

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, url);

        } catch (IOException | PostSlackException e) {
            getLogger().error("Failed to send message to Slack.", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        } finally {
            if (response != null) {
                try {
                    // consume the entire content of the response (entity)
                    // so that the manager can release the connection back to the pool
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    getLogger().error("Could not properly close HTTP response.", e);
                }
            }
        }
    }

    private HttpEntity createTextMessageRequestBody(ProcessContext context, FlowFile flowFile) throws PostSlackException, UnsupportedEncodingException {
        JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();

        String channel = context.getProperty(CHANNEL).evaluateAttributeExpressions(flowFile).getValue();
        if (channel == null || channel.isEmpty()) {
            throw new PostSlackException("The channel must be specified.");
        }
        jsonBuilder.add("channel", channel);

        String text = context.getProperty(TEXT).evaluateAttributeExpressions(flowFile).getValue();
        if (text != null && !text.isEmpty()){
            jsonBuilder.add("text", text);
        } else {
            if (attachmentProperties.isEmpty()) {
                throw new PostSlackException("The text of the message must be specified if no attachment has been specified and 'Upload File' has been set to 'No'.");
            }
        }

        if (!attachmentProperties.isEmpty()) {
            JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
            for (PropertyDescriptor attachmentProperty : attachmentProperties) {
                String propertyValue = context.getProperty(attachmentProperty).evaluateAttributeExpressions(flowFile).getValue();
                if (propertyValue != null && !propertyValue.isEmpty()) {
                    try {
                        jsonArrayBuilder.add(Json.createReader(new StringReader(propertyValue)).readObject());
                    } catch (JsonParsingException e) {
                        getLogger().warn(attachmentProperty.getName() + " property contains no valid JSON, has been skipped.");
                    }
                } else {
                    getLogger().warn(attachmentProperty.getName() + " property has no value, has been skipped.");
                }
            }
            jsonBuilder.add("attachments", jsonArrayBuilder);
        }

        return new StringEntity(jsonBuilder.build().toString(), Charset.forName("UTF-8"));
    }

    private HttpEntity createFileMessageRequestBody(ProcessContext context, ProcessSession session, FlowFile flowFile) throws PostSlackException {
        MultipartEntityBuilder multipartBuilder = MultipartEntityBuilder.create();

        String channel = context.getProperty(CHANNEL).evaluateAttributeExpressions(flowFile).getValue();
        if (channel == null || channel.isEmpty()) {
            throw new PostSlackException("The channel must be specified.");
        }
        multipartBuilder.addTextBody("channels", channel, MIME_TYPE_PLAINTEXT_UTF8);

        String text = context.getProperty(TEXT).evaluateAttributeExpressions(flowFile).getValue();
        if (text != null && !text.isEmpty()) {
            multipartBuilder.addTextBody("initial_comment", text, MIME_TYPE_PLAINTEXT_UTF8);
        }

        String title = context.getProperty(FILE_TITLE).evaluateAttributeExpressions(flowFile).getValue();
        if (title != null && !title.isEmpty()) {
            multipartBuilder.addTextBody("title", title, MIME_TYPE_PLAINTEXT_UTF8);
        }

        String fileName = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        if (fileName == null || fileName.isEmpty()) {
            fileName = "file";
            getLogger().warn("File name not specified, has been set to {}.", new Object[]{ fileName });
        }
        multipartBuilder.addTextBody("filename", fileName, MIME_TYPE_PLAINTEXT_UTF8);

        ContentType mimeType;
        String mimeTypeStr = context.getProperty(FILE_MIME_TYPE).evaluateAttributeExpressions(flowFile).getValue();
        if (mimeTypeStr == null || mimeTypeStr.isEmpty()) {
            mimeType = ContentType.APPLICATION_OCTET_STREAM;
            getLogger().warn("Mime type not specified, has been set to {}.", new Object[]{ mimeType.getMimeType() });
        } else {
            mimeType = ContentType.getByMimeType(mimeTypeStr);
            if (mimeType == null) {
                mimeType = ContentType.APPLICATION_OCTET_STREAM;
                getLogger().warn("Unknown mime type specified ({}), has been set to {}.", new Object[]{ mimeTypeStr, mimeType.getMimeType() });
            }
        }

        multipartBuilder.addBinaryBody("file", session.read(flowFile), mimeType, fileName);

        return multipartBuilder.build();
    }

    private static class PostSlackException extends Exception {
        PostSlackException(String message) {
            super(message);
        }

        PostSlackException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
