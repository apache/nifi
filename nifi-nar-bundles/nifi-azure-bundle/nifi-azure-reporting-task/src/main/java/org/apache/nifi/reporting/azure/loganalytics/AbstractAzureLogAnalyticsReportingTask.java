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
package org.apache.nifi.reporting.azure.loganalytics;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;

/**
 * Abstract ReportingTask to send metrics from Apache NiFi and JVM to Azure
 * Monitor.
 */
public abstract class AbstractAzureLogAnalyticsReportingTask extends AbstractReportingTask {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final String HMAC_SHA256_ALG = "HmacSHA256";
    private static final DateTimeFormatter RFC_1123_DATE_TIME = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss O");

    static final PropertyDescriptor LOG_ANALYTICS_WORKSPACE_ID = new PropertyDescriptor.Builder()
            .name("Log Analytics Workspace Id").description("Log Analytics Workspace Id").required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).build();
    static final PropertyDescriptor LOG_ANALYTICS_WORKSPACE_KEY = new PropertyDescriptor.Builder()
            .name("Log Analytics Workspace Key").description("Azure Log Analytic Worskspace Key").required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).build();
    static final PropertyDescriptor APPLICATION_ID = new PropertyDescriptor.Builder().name("Application ID")
            .description("The Application ID to be included in the metrics sent to Azure Log Analytics WS")
            .required(true).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    static final PropertyDescriptor INSTANCE_ID = new PropertyDescriptor.Builder().name("Instance ID")
            .description("Id of this NiFi instance to be included in the metrics sent to Azure Log Analytics WS")
            .required(true).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("${hostname(true)}").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    static final PropertyDescriptor PROCESS_GROUP_IDS = new PropertyDescriptor.Builder().name("Process group ID(s)")
            .description(
                    "If specified, the reporting task will send metrics the configured ProcessGroup(s) only. Multiple IDs should be separated by a comma. If"
                            + " none of the group-IDs could be found or no IDs are defined, the Root Process Group is used and global metrics are sent.")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.createListValidator(true, true,
                    StandardValidators.createRegexMatchingValidator(Pattern.compile("[0-9a-z-]+"))))
            .build();
    static final PropertyDescriptor JOB_NAME = new PropertyDescriptor.Builder().name("Job Name")
            .description("The name of the exporting job").defaultValue("nifi_reporting_job")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    static final PropertyDescriptor LOG_ANALYTICS_URL_ENDPOINT_FORMAT = new PropertyDescriptor.Builder()
            .name("Log Analytics URL Endpoint Format").description("Log Analytics URL Endpoint Format").required(false)
            .defaultValue("https://{0}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

    protected String createAuthorization(String workspaceId, String key, int contentLength, String rfc1123Date) {
        try {
            String signature = String.format("POST\n%d\napplication/json\nx-ms-date:%s\n/api/logs", contentLength,
                    rfc1123Date);
            Mac mac = Mac.getInstance(HMAC_SHA256_ALG);
            mac.init(new SecretKeySpec(DatatypeConverter.parseBase64Binary(key), HMAC_SHA256_ALG));
            String hmac = DatatypeConverter.printBase64Binary(mac.doFinal(signature.getBytes(UTF8)));
            return String.format("SharedKey %s:%s", workspaceId, hmac);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(LOG_ANALYTICS_WORKSPACE_ID);
        properties.add(LOG_ANALYTICS_WORKSPACE_KEY);
        properties.add(APPLICATION_ID);
        properties.add(INSTANCE_ID);
        properties.add(PROCESS_GROUP_IDS);
        properties.add(JOB_NAME);
        properties.add(LOG_ANALYTICS_URL_ENDPOINT_FORMAT);
        return properties;
    }

    /**
     * Construct HttpPost and return it
     *
     * @param urlFormat   URL format to Azure Log Analytics Endpoint
     * @param workspaceId your azure log analytics workspace id
     * @param logName     log table name where metrics will be pushed
     * @return HttpsURLConnection to your azure log analytics workspace
     * @throws IllegalArgumentException if dataCollectorEndpoint url is invalid
     */
    protected HttpPost getHttpPost(final String urlFormat, final String workspaceId, final String logName)
            throws IllegalArgumentException {
        String dataCollectorEndpoint = MessageFormat.format(urlFormat, workspaceId);
        HttpPost post = new HttpPost(dataCollectorEndpoint);
        post.addHeader("Content-Type", "application/json");
        post.addHeader("Log-Type", logName);
        return post;
    }

    protected void sendToLogAnalytics(final HttpPost request, final String workspaceId, final String linuxPrimaryKey,
            final String rawJson) throws IllegalArgumentException, RuntimeException, IOException {

        final int bodyLength = rawJson.getBytes(UTF8).length;
        final ZonedDateTime zNow = ZonedDateTime.now(ZoneOffset.UTC);
        final String nowRfc1123 = zNow.format(DateTimeFormatter.RFC_1123_DATE_TIME);
        final String nowISO8601 = zNow.format(DateTimeFormatter.ISO_DATE_TIME);
        final String createAuthorization = createAuthorization(workspaceId, linuxPrimaryKey, bodyLength, nowRfc1123);
        request.addHeader("Authorization", createAuthorization);
        request.addHeader("x-ms-date", nowRfc1123);
        request.addHeader("time-generated-field", nowISO8601);
        request.setEntity(new StringEntity(rawJson));
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            postRequest(httpClient, request);
        }
    }

    /**
     * post request with httpClient and httpPost
     *
     * @param httpClient HttpClient
     * @param request    HttpPost
     * @throws IOException      if httpClient.execute fails
     * @throws RuntimeException if post request status return other than 200
     */
    protected void postRequest(final CloseableHttpClient httpClient, final HttpPost request)
            throws IOException, RuntimeException {

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            if (response != null && response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException(response.getStatusLine().toString());
            }
        }
    }
}
