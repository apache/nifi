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
package org.apache.nifi.processors.livy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.api.livy.exception.SessionManagerException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import org.apache.nifi.processor.util.StandardValidators;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.apache.nifi.controller.api.livy.LivySessionService;
import org.apache.nifi.expression.ExpressionLanguageScope;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"spark", "livy", "http", "execute"})
@CapabilityDescription("Execute Spark Code over a Livy-managed HTTP session to a live Spark context. Supports cached RDD sharing.")
public class ExecuteSparkInteractive extends AbstractProcessor {

    public static final PropertyDescriptor LIVY_CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
            .name("exec-spark-iactive-livy-controller-service")
            .displayName("Livy Controller Service")
            .description("The controller service to use for Livy-managed session(s).")
            .required(true)
            .identifiesControllerService(LivySessionService.class)
            .build();

    public static final PropertyDescriptor CODE = new PropertyDescriptor.Builder()
            .name("exec-spark-iactive-code")
            .displayName("Code")
            .description("The code to execute in the session. This property can be empty, a constant value, or built from attributes "
                    + "using Expression Language. If this property is specified, it will be used regardless of the content of "
                    + "incoming flowfiles. If this property is empty, the content of the incoming flow file is expected "
                    + "to contain valid code to be issued by the processor to the session. Note that Expression "
                    + "Language is not evaluated for flow file contents.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    /**
     * Points to the charset name corresponding to the incoming flow file's
     * encoding.
     */
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("exec-spark-iactive-charset")
            .displayName("Character Set")
            .description("The character set encoding for the incoming flow file.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor STATUS_CHECK_INTERVAL = new PropertyDescriptor.Builder()
            .name("exec-spark-iactive-status-check-interval")
            .displayName("Status Check Interval")
            .description("The amount of time to wait between checking the status of an operation.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("1 sec")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully processed are sent to this relationship")
            .build();

    public static final Relationship REL_WAIT = new Relationship.Builder()
            .name("wait")
            .description("FlowFiles that are waiting on an available Spark session will be sent to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship when they cannot be parsed")
            .build();

    private volatile List<PropertyDescriptor> properties;
    private volatile Set<Relationship> relationships;

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(LIVY_CONTROLLER_SERVICE);
        properties.add(CODE);
        properties.add(CHARSET);
        properties.add(STATUS_CHECK_INTERVAL);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_WAIT);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog log = getLogger();
        final LivySessionService livySessionService = context.getProperty(LIVY_CONTROLLER_SERVICE).asControllerService(LivySessionService.class);
        final Map<String, String> livyController;
        try {
            livyController = livySessionService.getSession();
            if (livyController == null || livyController.isEmpty()) {
                log.debug("No Spark session available (yet), routing flowfile to wait");
                session.transfer(flowFile, REL_WAIT);
                context.yield();
                return;
            }
        } catch (SessionManagerException sme) {
            log.error("Error opening spark session, routing flowfile to wait", sme);
            session.transfer(flowFile, REL_WAIT);
            context.yield();
            return;
        }
        final long statusCheckInterval = context.getProperty(STATUS_CHECK_INTERVAL).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());

        String sessionId = livyController.get("sessionId");
        String livyUrl = livyController.get("livyUrl");
        String code = context.getProperty(CODE).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(code)) {
            try (InputStream inputStream = session.read(flowFile)) {
                // If no code was provided, assume it is in the content of the incoming flow file
                code = IOUtils.toString(inputStream, charset);
            } catch (IOException ioe) {
                log.error("Error reading input flowfile, penalizing and routing to failure", flowFile, ioe.getMessage(), ioe);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        code = StringEscapeUtils.escapeJson(code);
        String payload = "{\"code\":\"" + code + "\"}";
        try {
            final JSONObject result = submitAndHandleJob(livyUrl, livySessionService, sessionId, payload, statusCheckInterval);
            log.debug("ExecuteSparkInteractive Result of Job Submit: " + result);
            if (result == null) {
                session.transfer(flowFile, REL_FAILURE);
            } else {
                try {
                    final JSONObject output = result.getJSONObject("data");
                    flowFile = session.write(flowFile, out -> out.write(output.toString().getBytes(charset)));
                    flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), LivySessionService.APPLICATION_JSON);
                    session.transfer(flowFile, REL_SUCCESS);
                } catch (JSONException je) {
                    // The result doesn't contain the data, just send the output object as the flow file content to failure (after penalizing)
                    log.error("Spark Session returned an error, sending the output JSON object as the flow file content to failure (after penalizing)");
                    flowFile = session.write(flowFile, out -> out.write(result.toString().getBytes(charset)));
                    flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), LivySessionService.APPLICATION_JSON);
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
        } catch (IOException | SessionManagerException e) {
            log.error("Failure processing flowfile {} due to {}, penalizing and routing to failure", flowFile, e.getMessage(), e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private JSONObject submitAndHandleJob(String livyUrl, LivySessionService livySessionService, String sessionId, String payload, long statusCheckInterval)
        throws IOException, SessionManagerException {
        ComponentLog log = getLogger();
        String statementUrl = livyUrl + "/sessions/" + sessionId + "/statements";
        JSONObject output = null;
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", LivySessionService.APPLICATION_JSON);
        headers.put("X-Requested-By", LivySessionService.USER);
        headers.put("Accept", "application/json");

        log.debug("submitAndHandleJob() Submitting Job to Spark via: " + statementUrl);
        try {
            JSONObject jobInfo = readJSONObjectFromUrlPOST(statementUrl, livySessionService, headers, payload);
            log.debug("submitAndHandleJob() Job Info: " + jobInfo);
            String statementId = String.valueOf(jobInfo.getInt("id"));
            statementUrl = statementUrl + "/" + statementId;
            jobInfo = readJSONObjectFromUrl(statementUrl, livySessionService, headers);
            String jobState = jobInfo.getString("state");

            log.debug("submitAndHandleJob() New Job Info: " + jobInfo);
            Thread.sleep(statusCheckInterval);
            if (jobState.equalsIgnoreCase("available")) {
                log.debug("submitAndHandleJob() Job status is: " + jobState + ". returning output...");
                output = jobInfo.getJSONObject("output");
            } else if (jobState.equalsIgnoreCase("running") || jobState.equalsIgnoreCase("waiting")) {
                while (!jobState.equalsIgnoreCase("available")) {
                    log.debug("submitAndHandleJob() Job status is: " + jobState + ". Waiting for job to complete...");
                    Thread.sleep(statusCheckInterval);
                    jobInfo = readJSONObjectFromUrl(statementUrl, livySessionService, headers);
                    jobState = jobInfo.getString("state");
                }
                output = jobInfo.getJSONObject("output");
            } else if (jobState.equalsIgnoreCase("error")
                    || jobState.equalsIgnoreCase("cancelled")
                    || jobState.equalsIgnoreCase("cancelling")) {
                log.debug("Job status is: " + jobState + ". Job did not complete due to error or has been cancelled. Check SparkUI for details.");
                throw new IOException(jobState);
            }
        } catch (JSONException | InterruptedException e) {
            throw new IOException(e);
        }
        return output;
    }

    private JSONObject readJSONObjectFromUrlPOST(String urlString, LivySessionService livySessionService, Map<String, String> headers, String payload)
        throws IOException, JSONException, SessionManagerException {
        HttpClient httpClient = livySessionService.getConnection();

        HttpPost request = new HttpPost(urlString);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }
        HttpEntity httpEntity = new StringEntity(payload);
        request.setEntity(httpEntity);
        HttpResponse response = httpClient.execute(request);

        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode() + " : " + response.getStatusLine().getReasonPhrase());
        }

        InputStream content = response.getEntity().getContent();
        return readAllIntoJSONObject(content);
    }

    private JSONObject readJSONObjectFromUrl(String urlString, LivySessionService livySessionService, Map<String, String> headers) throws IOException, JSONException, SessionManagerException {
        HttpClient httpClient = livySessionService.getConnection();

        HttpGet request = new HttpGet(urlString);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }
        HttpResponse response = httpClient.execute(request);

        InputStream content = response.getEntity().getContent();
        return readAllIntoJSONObject(content);
    }

    private JSONObject readAllIntoJSONObject(InputStream content) throws IOException, JSONException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(content, StandardCharsets.UTF_8));
        String jsonText = IOUtils.toString(rd);
        return new JSONObject(jsonText);
    }

}
