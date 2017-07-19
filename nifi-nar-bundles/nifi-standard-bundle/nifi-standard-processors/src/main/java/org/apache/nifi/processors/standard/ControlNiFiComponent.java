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
package org.apache.nifi.processors.standard;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;

import org.apache.commons.lang.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

@Tags({"control", "rest", "api", "nifi"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor can be used to start/stop NiFi components based on incoming flow files.")
@DynamicProperty(name = "Property name", value = "Property value", supportsExpressionLanguage = true,
        description = "Specifies a property that will be updated on the specified component (except for process groups).")
public class ControlNiFiComponent extends AbstractProcessor {

    public static final String START = "START";
    public static final String STOP = "STOP";
    public static final String START_STOP = "START/STOP";
    public static final String STOP_START = "STOP/START";

    private static final String PROCESSOR_VALUE = "/processors/";
    private static final String PROCESS_GROUP_VALUE = "/flow/process-groups/";
    private static final String CONTROLLER_SERVICE_VALUE = "/controller-services/";
    private static final String REPORTING_TASK_VALUE = "/reporting-tasks/";

    public static final AllowableValue PROCESSOR = new AllowableValue(PROCESSOR_VALUE, "Processor");
    public static final AllowableValue PROCESS_GROUP = new AllowableValue(PROCESS_GROUP_VALUE, "Process Group");
    public static final AllowableValue CONTROLLER_SERVICE = new AllowableValue(CONTROLLER_SERVICE_VALUE, "Controller Service",
            "This will not start/stop the referencing components of the controller service.");
    public static final AllowableValue REPORTING_TASK = new AllowableValue(REPORTING_TASK_VALUE, "Reporting Task");

    private static final Configuration STRICT_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new JacksonJsonProvider()).build();
    private static final JsonProvider JSON_PROVIDER = STRICT_PROVIDER_CONFIGURATION.jsonProvider();

    public static final PropertyDescriptor NIFI_API_URL = new PropertyDescriptor.Builder()
            .displayName("NiFi REST API URL")
            .name("control-nifi-url")
            .description("URL to be used for the NiFi REST API.")
            .required(true)
            .defaultValue("http://localhost:8080/nifi-api")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .displayName("Username")
            .name("control-nifi-username")
            .description("Username to be used in case NiFi is secured")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .displayName("Password")
            .name("control-nifi-password")
            .description("Password to be used in case NiFi is secured")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .displayName("SSL Context Service")
            .name("control-nifi-ssl")
            .description("The SSL Context Service to be used in case NiFi is secured.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor COMPONENT_TYPE = new PropertyDescriptor.Builder()
            .displayName("Component type")
            .name("control-nifi-type")
            .description("The type of the component on which the action will be performed.")
            .required(true)
            .allowableValues(PROCESSOR, PROCESS_GROUP, CONTROLLER_SERVICE, REPORTING_TASK)
            .defaultValue(PROCESSOR.getValue())
            .build();

    public static final PropertyDescriptor UUID = new PropertyDescriptor.Builder()
            .displayName("Component UUID")
            .name("control-nifi-uuid")
            .description("The UUID of the component on which the action will be performed.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ACTION = new PropertyDescriptor.Builder()
            .displayName("Action to perform")
            .name("control-nifi-action")
            .description("")
            .required(true)
            .allowableValues(START, STOP, START_STOP, STOP_START)
            .defaultValue(START)
            .build();

    public static final PropertyDescriptor SLEEP = new PropertyDescriptor.Builder()
            .displayName("Sleep duration")
            .name("control-nifi-sleep")
            .description("The time to wait between two calls when executing")
            .required(true)
            .defaultValue("1 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Flow files generating successful requests will be routed to this relationship.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Flow files generating un-successful requests will be routed to this relationship.")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(NIFI_API_URL);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(COMPONENT_TYPE);
        descriptors.add(UUID);
        descriptors.add(ACTION);
        descriptors.add(SLEEP);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .expressionLanguageSupported(true)
            .dynamic(true)
            .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        okHttpClientAtomicReference.set(null);
        OkHttpClient okHttpClient = new OkHttpClient();

        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslService == null ? null : sslService.createSSLContext(ClientAuth.NONE);
        if (sslContext != null) {
            okHttpClient.setSslSocketFactory(sslContext.getSocketFactory());
        }

        okHttpClientAtomicReference.set(okHttpClient);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(2);

        final boolean isSecured = validationContext.getProperty(NIFI_API_URL).getValue().toLowerCase().startsWith("https");

        if(isSecured) {

            boolean isUsernameSet = validationContext.getProperty(USERNAME).isSet();
            boolean isPasswordSet = validationContext.getProperty(PASSWORD).isSet();
            boolean isSslContextSet = validationContext.getProperty(SSL_CONTEXT_SERVICE).isSet();

            if(!isSslContextSet) {
                results.add(new ValidationResult.Builder().subject(SSL_CONTEXT_SERVICE.getDisplayName()).valid(false)
                        .explanation("In case NiFi is secured, SSL context with a truststore must be set.").build());
            } else {
                final SSLContextService sslService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
                if(!sslService.isTrustStoreConfigured()) {
                    results.add(new ValidationResult.Builder().subject(SSL_CONTEXT_SERVICE.getDisplayName()).valid(false)
                            .explanation("In case NiFi is secured, SSL context with a truststore must be set.").build());
                }
                if(!sslService.isKeyStoreConfigured() && (!isUsernameSet && !isPasswordSet)) {
                    results.add(new ValidationResult.Builder().subject(USERNAME.getDisplayName()).valid(false)
                            .explanation("In case NiFi is secured and no keystore is configured, username and password must be set.").build());
                }
            }

        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        OkHttpClient okHttpClient = okHttpClientAtomicReference.get();

        FlowFile requestFlowFile = session.get();
        if(requestFlowFile == null) {
            return;
        }

        // get dynamic properties
        Map<PropertyDescriptor, String> processorProperties = context.getProperties();
        Map<String, String> propertiesToUpdate = new HashMap<String, String>();
        for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
            PropertyDescriptor property = entry.getKey();
            if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                String dynamicValue = context.getProperty(property).evaluateAttributeExpressions(requestFlowFile).getValue();
                propertiesToUpdate.put(property.getName(), dynamicValue);
            }
        }

        try {

            final String url = trimToEmpty(context.getProperty(NIFI_API_URL).getValue());
            final String uuid = context.getProperty(UUID).evaluateAttributeExpressions(requestFlowFile).getValue();
            final String componentType = context.getProperty(COMPONENT_TYPE).getValue();
            String token = null;
            String revision = null;

            if(context.getProperty(USERNAME).isSet()) {
                token = getToken(okHttpClient, url, context);
            }

            revision = getRevision(okHttpClient, url, token, uuid, componentType);

            switch (context.getProperty(ACTION).getValue()) {
                case START:
                    execute(okHttpClient, url + componentType, token, uuid, getPayload("RUNNING", uuid, revision, componentType, propertiesToUpdate));
                    break;
                case STOP:
                    execute(okHttpClient, url + componentType, token, uuid, getPayload("STOPPED", uuid, revision, componentType, propertiesToUpdate));
                    break;
                case START_STOP:
                    execute(okHttpClient, url + componentType, token, uuid, getPayload("RUNNING", uuid, revision, componentType, propertiesToUpdate));
                    Thread.sleep(context.getProperty(SLEEP).asTimePeriod(TimeUnit.MILLISECONDS));
                    revision = getRevision(okHttpClient, url, token, uuid, componentType);
                    execute(okHttpClient, url + componentType, token, uuid, getPayload("STOPPED", uuid, revision, componentType, null));
                    break;
                case STOP_START:
                    execute(okHttpClient, url + componentType, token, uuid, getPayload("STOPPED", uuid, revision, componentType, propertiesToUpdate));
                    Thread.sleep(context.getProperty(SLEEP).asTimePeriod(TimeUnit.MILLISECONDS));
                    revision = getRevision(okHttpClient, url, token, uuid, componentType);
                    execute(okHttpClient, url + componentType, token, uuid, getPayload("RUNNING", uuid, revision, componentType, null));
                    break;
                default:
                    break;
            }

            session.transfer(requestFlowFile, SUCCESS);

        } catch (IOException | InterruptedException e) {
            getLogger().error(e.getLocalizedMessage(), e);
            requestFlowFile = session.putAttribute(requestFlowFile, "error.message", e.getMessage());
            requestFlowFile = session.penalize(requestFlowFile);
            session.transfer(requestFlowFile, FAILURE);
        }

    }

    private String getRevision(OkHttpClient okHttpClient, String url, String token, String uuid, String componentType) throws IOException {
        if(componentType.equals(PROCESS_GROUP_VALUE)) {
            return null;
        }

        Request.Builder requestBuilder = new Request.Builder();
        requestBuilder = requestBuilder.url(url + componentType + uuid);

        if(token != null) {
            requestBuilder = requestBuilder.header("Authorization", "Bearer " + token);
        }

        Response responseHttp = okHttpClient.newCall(requestBuilder.get().build()).execute();

        if (responseHttp.code() != 200) {
            throw new IOException(responseHttp.code() + " - " + responseHttp.message() + " - " + responseHttp.body().string());
        }

        Object result = JsonPath.using(STRICT_PROVIDER_CONFIGURATION).parse(responseHttp.body().string()).read(JsonPath.compile("$.revision"));
        return JSON_PROVIDER.toJson(result);
    }

    private String getPayload(final String state, final String uuid, final String revision, final String componentType, Map<String, String> propertiesToUpdate) {
        String config = "";
        List<String> properties= new ArrayList<String>();

        if(propertiesToUpdate != null) {
            for (String propertyName : propertiesToUpdate.keySet()) {
                properties.add("\"" + propertyName + "\": \"" + propertiesToUpdate.get(propertyName) + "\"");
            }
        }

        switch(componentType) {
            case PROCESSOR_VALUE:
            default:
                if(propertiesToUpdate != null) {
                    config = ",\"config\": { \"properties\": { " + StringUtils.join(properties, ',') +" }}";
                }
                return "{\"revision\":" + revision + ", \"component\":{\"id\":\"" + uuid + "\",\"state\":\"" + state + "\"" + config  + "}}";

            case REPORTING_TASK_VALUE:
                if(propertiesToUpdate != null) {
                    config = ",\"properties\": { " + StringUtils.join(properties, ',') +" }";
                }
                return "{\"revision\":" + revision + ", \"component\":{\"id\":\"" + uuid + "\",\"state\":\"" + state + "\"" + config  + "}}";

            case PROCESS_GROUP_VALUE:
                return "{\"id\":\"" + uuid + "\",\"state\":\"" + state + "\"}";

            case CONTROLLER_SERVICE_VALUE:
                String controllerState = state.equals("RUNNING") ? "ENABLED" : "DISABLED";
                if(propertiesToUpdate != null) {
                    config = ",\"properties\": { " + StringUtils.join(properties, ',') +" }";
                }
                return "{\"revision\":" + revision + ", \"component\":{\"id\":\"" + uuid + "\",\"state\":\"" + controllerState + "\"" + config  + "}}";
        }
    }

    private void execute(OkHttpClient okHttpClient, String url, String token, String uuid, String payload) throws IOException {
        Request.Builder requestBuilder = new Request.Builder();
        requestBuilder = requestBuilder.url(url + uuid);

        if(token != null) {
            requestBuilder = requestBuilder.header("Authorization", "Bearer " + token);
        }

        requestBuilder = requestBuilder.header("Content-Type", "application/json");
        requestBuilder = requestBuilder.put(RequestBody.create(MediaType.parse("application/json"), payload));
        Response responseHttp = okHttpClient.newCall(requestBuilder.build()).execute();

        if (responseHttp.code() != 200) {
            throw new IOException(responseHttp.code() + " - " + responseHttp.message() + " - " + responseHttp.body().string());
        }
    }

    private String getToken(OkHttpClient okHttpClient, String url, ProcessContext context) throws IOException {
        Request.Builder requestBuilder = new Request.Builder();
        requestBuilder = requestBuilder.url(url + "/access/token");

        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();
        final String payload = "username=" + username + "&password=" + password;

        requestBuilder = requestBuilder.header("Content-Type", "application/x-www-form-urlencoded");
        requestBuilder = requestBuilder.post(RequestBody.create(MediaType.parse("application/x-www-form-urlencoded"), payload));
        Response responseHttp = okHttpClient.newCall(requestBuilder.build()).execute();

        int statusCode = responseHttp.code();

        if (statusCode != 201 && statusCode != 200) {
            throw new IOException(statusCode + " - " + responseHttp.message() + " - " + responseHttp.body().string());
        }

        return responseHttp.body().string();
    }
}
