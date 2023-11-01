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
package org.apache.nifi.doris;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.doris.util.Result;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.processor.util.StandardValidators;

@RequiresInstanceClassLoading
@Tags({"doris", "client", "stream_lode"})
@CapabilityDescription("A controller service for accessing an Apache Doris client.")
public class DorisClientServiceImpl extends AbstractControllerService implements DorisClientService {
    private String feHost;
    private String userName;
    private String password;
    private String httpPort;
    private String loginLabel;

    private String loadUrlFormat;
    private String loginActionUrlFormat;
    private String httpTimeOut;
    private ConfigurationContext context;
    private String columnSeparator;
    private String dataFormat;
    private String stripOuterArray;

    public static final PropertyDescriptor FE_HOST = new PropertyDescriptor
            .Builder().name("Fe Host")
            .description("Apache Doris Frontend address")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder().name("Username")
            .description("Connect to the Apache Doris username.Note that this user needs to have read and write access to the corresponding table")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("Password")
            .description("Password to connect to Apache Doris")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    public static final PropertyDescriptor HTTP_PORT = new PropertyDescriptor
            .Builder().name("Http Port")
            .description("http server port on the FE")
            .required(false)
            .defaultValue("8030")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor LOAD_URL_FORMAT = new PropertyDescriptor
            .Builder().name("Load URL")
            .description("Stream Load URL. The placeholders are, in turn, feHost, httpPort, destDatabase, destTableName, as described in the Apache Doris documentation")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("http://%s:%s/api/%s/%s/_stream_load")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor LOGIN_ACTION_URL_FORMAT = new PropertyDescriptor
            .Builder().name("Login Action URL Format")
            .description("For login services.This connection is invoked when the Service is in the onEnabled " +
                    "state to test that the communication with the Doris server is normal")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("http://%s:%s/rest/v1/login")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor LABEL = new PropertyDescriptor
            .Builder().name("Label")
            .description("Doris import jobs can all set a Label. This Label is usually a user-defined string that has certain business logic properties.\n" +
                    "The main purpose of Label is to uniquely identify an import task and ensure that the same Label will only be successfully imported once.\n" +
                    "The Label mechanism can ensure that the imported data is not lost and not heavy. If the upstream data source can guarantee the At-Least-Once semantics," +
                    " then with the Label mechanism of Doris, the Exactly-Once semantics can be guaranteed.\n" +
                    "Label is unique under a database. The default retention period for labels is 3 days. That is, after 3 days, the finished labels are automatically cleaned up, " +
                    "after which the labels can be reused." +
                    "Note that this label validates the user login and does nothing else")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("label-" + UUID.randomUUID())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    public static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor
            .Builder().name("Data Format")
            .description("The underlying format of the data, which defaults to json data")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("json")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor HTTP_TIME_OUT = new PropertyDescriptor
            .Builder().name("Http Time Out")
            .description("http connection timeout")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("10000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor COLUMN_SEPARATOR = new PropertyDescriptor
            .Builder().name("Column Separator")
            .description("Column separator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(",")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor STRIP_OUTER_ARRAY = new PropertyDescriptor
            .Builder().name("Strip Outer Array")
            .description("As it parses, Doris expands the array and parses each Object as a row.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("true")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    private List<PropertyDescriptor> properties;


    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(FE_HOST);
        props.add(USERNAME);
        props.add(PASSWORD);
        props.add(HTTP_PORT);
        props.add(LABEL);
        props.add(DATA_FORMAT);
        props.add(LOAD_URL_FORMAT);
        props.add(LOGIN_ACTION_URL_FORMAT);
        props.add(HTTP_TIME_OUT);
        props.add(COLUMN_SEPARATOR);
        props.add(STRIP_OUTER_ARRAY);
        this.properties = Collections.unmodifiableList(props);
    }


    HashMap<String, HttpPut> connect = new HashMap();
    final CloseableHttpClient client = httpClientBuilder.build();
    private final static HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    // If the connection target is FE, you need to deal with 307 redirect。
                    return true;
                }
            });

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException {
        this.context = context;
        httpPort = context.getProperty(HTTP_PORT).evaluateAttributeExpressions().getValue();
        loginLabel = context.getProperty(LABEL).evaluateAttributeExpressions().getValue();
        feHost = context.getProperty(FE_HOST).evaluateAttributeExpressions().getValue();
        userName = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        loadUrlFormat = context.getProperty(LOAD_URL_FORMAT).evaluateAttributeExpressions().getValue();
        loginActionUrlFormat = context.getProperty(LOGIN_ACTION_URL_FORMAT).evaluateAttributeExpressions().getValue();
        httpTimeOut = context.getProperty(HTTP_TIME_OUT).evaluateAttributeExpressions().getValue();
        dataFormat = context.getProperty(DATA_FORMAT).evaluateAttributeExpressions().getValue();
        columnSeparator = context.getProperty(COLUMN_SEPARATOR).evaluateAttributeExpressions().getValue();
        stripOuterArray = context.getProperty(STRIP_OUTER_ARRAY).evaluateAttributeExpressions().getValue();
        testConnectivity(context);
    }

    @OnDisabled
    public void shutdown() {
        if (null != client) {
            try {
                client.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void testConnectivity(final ConfigurationContext context) throws IOException {

        String loginUrl = String.format(loginActionUrlFormat, feHost, httpPort);
        HttpPost httpPost = new HttpPost(loginUrl);
        httpPost.removeHeaders(HttpHeaders.CONTENT_LENGTH);
        httpPost.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
        httpPost.setHeader(HttpHeaders.EXPECT, "100-continue");
        httpPost.setHeader(HttpHeaders.TIMEOUT, httpTimeOut);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(userName, password));
        httpPost.setHeader("label", loginLabel);

        CloseableHttpResponse response = client.execute(httpPost);

        if (response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                throw new RuntimeException(String.format("Doris Stream load failed. status: %s load result: %s", statusCode, loadResult));
            }
        } else {
            throw new RuntimeException("Doris Stream load failed.");
        }
    }

    @Override
    public HashMap<String, HttpPut> setClient(String destDatabase, String destTableName, String columns) {

        if (null == connect.get(destDatabase + destTableName)) {
            String loadUrl = String.format(loadUrlFormat, feHost, httpPort, destDatabase, destTableName);
            HttpPut httpPut = new HttpPut(loadUrl);
            httpPut.removeHeaders(HttpHeaders.CONTENT_LENGTH);
            httpPut.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
            httpPut.setHeader(HttpHeaders.TIMEOUT, httpTimeOut);
            httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
            httpPut.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(userName, password));

            httpPut.setHeader("column_separator", columnSeparator);
            httpPut.setHeader("format", dataFormat);
            httpPut.setHeader("columns", columns);
            httpPut.setHeader("strip_outer_array", stripOuterArray);
            connect.put(destDatabase + destTableName, httpPut);
        }
        return connect;
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }


    @Override
    public void select() {

    }


    @Override
    public Result putJson(String jsonData, String destDatabase, String destTableName) {
        try {
            HttpPut httpPut = connect.get(destDatabase + destTableName);
            Result result = new Result();
            if (null == httpPut) {
                getLogger().error("Description Failed to initialize the doris connection.");
                throw new RuntimeException();
            }
            httpPut.setEntity(new StringEntity(jsonData));
            CloseableHttpResponse response = client.execute(httpPut);
            if (response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                result.setLoadResult(loadResult);
                int statusCode = response.getStatusLine().getStatusCode();
                result.setStatusCode(statusCode);
            } else {
                throw new RuntimeException("Doris Stream load failed.");
            }
            return result;

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (ClientProtocolException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
