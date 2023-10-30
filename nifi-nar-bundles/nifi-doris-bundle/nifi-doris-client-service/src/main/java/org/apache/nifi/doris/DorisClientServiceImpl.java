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
import java.util.*;

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
@CapabilityDescription("Example ControllerService implementation of MyService.")
public class DorisClientServiceImpl extends AbstractControllerService implements DorisClientService {

    public static final PropertyDescriptor FE_HOST = new PropertyDescriptor
            .Builder().name("FE_HOST")
            .displayName("fe_host")
            .description("FE_HOST")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder().name("USERNAME")
            .displayName("user_name")
            .description("USERNAME")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("PASSWORD")
            .displayName("password")
            .description("PASSWORD")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    public static final PropertyDescriptor HTTP_PORT = new PropertyDescriptor
            .Builder().name("HTTP_PORT")
            .displayName("http_port")
            .description("HTTP_PORT")
            .required(false)
            .defaultValue("8030")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor LOAD_URL_FORMAT = new PropertyDescriptor
            .Builder().name("LOAD_URL")
            .displayName("load_url")
            .description("LOAD_URL")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("http://%s:%s/api/%s/%s/_stream_load")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor LOGIN_ACTION_URL_FORMAT = new PropertyDescriptor
            .Builder().name("LOGIN_ACTION_URL_FORMAT")
            .displayName("login_action_url_format")
            .description("LOGIN_ACTION_URL_FORMAT")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("http://%s:%s/rest/v1/login")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor LABEL = new PropertyDescriptor
            .Builder().name("LABEL")
            .displayName("label")
            .description("LABEL")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("label-" + UUID.randomUUID())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    public static final PropertyDescriptor FORMAT = new PropertyDescriptor
            .Builder().name("FORMAT")
            .displayName("format")
            .description("FORMAT")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("json")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    private List<PropertyDescriptor> properties;
    private ConfigurationContext context = null;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(FE_HOST);
        props.add(USERNAME);
        props.add(PASSWORD);
        props.add(HTTP_PORT);
        props.add(LABEL);
        props.add(FORMAT);
        props.add(LOAD_URL_FORMAT);
        props.add(LOGIN_ACTION_URL_FORMAT);
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
        String feHost = null;
        String userName = null;
        String password = null;
        String httpPort = null;

        httpPort = context.getProperty(HTTP_PORT).evaluateAttributeExpressions().getValue();
        String label = context.getProperty(LABEL).evaluateAttributeExpressions().getValue();
        if (context.getProperty(FE_HOST).isSet()) {
            feHost = context.getProperty(FE_HOST).evaluateAttributeExpressions().getValue();
        }
        if (context.getProperty(USERNAME).isSet()) {
            userName = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        }
        if (context.getProperty(PASSWORD).isSet()) {
            password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        }
        String loginActionUrlFormat = context.getProperty(LOGIN_ACTION_URL_FORMAT).evaluateAttributeExpressions().getValue();
        String loginUrl = String.format(loginActionUrlFormat, feHost, httpPort);
        HttpPost httpPost = new HttpPost(loginUrl);
        httpPost.removeHeaders(HttpHeaders.CONTENT_LENGTH);
        httpPost.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
        httpPost.setHeader(HttpHeaders.EXPECT, "100-continue");
        httpPost.setHeader(HttpHeaders.TIMEOUT, "10000");
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(userName, password));
        httpPost.setHeader("label", label);

        CloseableHttpResponse response = client.execute(httpPost);

        if (response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            getLogger().error("------------------loadResult : {}-------------------", loadResult);
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
        String feHost = null;
        String userName = null;
        String password = null;
        String httpPort = null;

        if (null == connect.get(destDatabase + destTableName)) {
            httpPort = context.getProperty(HTTP_PORT).evaluateAttributeExpressions().getValue();
            if (context.getProperty(FE_HOST).isSet()) {
                feHost = context.getProperty(FE_HOST).evaluateAttributeExpressions().getValue();
            }
            if (context.getProperty(USERNAME).isSet()) {
                userName = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            }
            if (context.getProperty(PASSWORD).isSet()) {
                password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
            }

            String loadUrlFormat = context.getProperty(LOAD_URL_FORMAT).evaluateAttributeExpressions().getValue();
            String loadUrl = String.format(loadUrlFormat, feHost, httpPort, destDatabase, destTableName);
            HttpPut httpPut = new HttpPut(loadUrl);
            httpPut.removeHeaders(HttpHeaders.CONTENT_LENGTH);
            httpPut.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
            httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
            httpPut.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(userName, password));

            //httpPut.setHeader("label", label);
            httpPut.setHeader("column_separator", ",");
            httpPut.setHeader("format", "json");
            httpPut.setHeader("columns", columns);
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
    public Result insert(String jsonData, String destDatabase, String destTableName) {
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
                if (statusCode != 200) {
                    throw new RuntimeException(String.format("Doris Stream load failed. status: %s load result: %s", statusCode, loadResult));
                }
                result.setExecuteStatus("success");
            } else {
                result.setExecuteStatus("failure");
                result.setStatusCode(-1);
                result.setLoadResult(null);
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

    @Override
    public void delete() {

    }

    @Override
    public void update() {

    }

}
