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
package org.apache.nifi.processors.oozie;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.oozie.util.SslAuthOozieClient;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.oozie.client.AuthOozieClient.AuthType;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

@Tags({"oozie", "job", "workflow"})
@CapabilityDescription("This processor is used to submit and start an Oozie workflow")
@WritesAttributes({@WritesAttribute(attribute="oozie.workflow.job.id", description="Job ID for the submitted workflow")})
@InputRequirement(Requirement.INPUT_REQUIRED)
public class SubmitOozieWorkflow extends AbstractProcessor {

    private final static String OOZIE_WORKFLOW_JOB_ID = "oozie.workflow.job.id";

    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("oozie-url")
            .displayName("Oozie server URL")
            .description("The URL of the Oozie server. Example: http://oozie.example.com:11000/oozie")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor APP_PATH = new PropertyDescriptor.Builder()
            .name("oozie-app-path")
            .displayName("HDFS Application Path")
            .description("HDFS Path to the directory containing the workflow.xml file")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_ATTRIBUTES_TO_SEND = new PropertyDescriptor.Builder()
            .name("oozie-prop-regex")
            .displayName("Attributes to use as properties")
            .description("Regular expression that defines which attributes to send as properties of the workflow.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("If specified, this service will be used to create an SSL Context that will be used "
                    + "to secure communications; if not specified, communications will not be secure")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("When the job is successfully submitted, the flow file is routed to this relationship.")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If an error occurred to submit a workflow, the flow file is routed to this relationship.")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private volatile Pattern regexAttributesToSend = null;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(URL);
        descriptors.add(APP_PATH);
        descriptors.add(PROP_ATTRIBUTES_TO_SEND);
        descriptors.add(KERBEROS_CREDENTIALS_SERVICE);
        descriptors.add(SSL_CONTEXT_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String att = context.getProperty(PROP_ATTRIBUTES_TO_SEND).getValue();
        if (att == null || att.isEmpty()) {
            regexAttributesToSend = null;
        } else {
            final String trimmedValue = StringUtils.trimToEmpty(att);
            regexAttributesToSend = Pattern.compile(trimmedValue);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        if(credentialsService != null) {
            try {
                String principal = credentialsService.getPrincipal();
                String keyTab = credentialsService.getKeytab();
                Configuration conf = new Configuration();
                conf.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation ugi = SecurityUtil.loginKerberos(conf, principal, keyTab);
                ugi.doAs((PrivilegedAction<Object>)() -> {
                    submitWorkflow(context, session, flowFile, AuthType.KERBEROS.name());
                    return null;
                });
            } catch (IOException e) {
                getLogger().error("Failed to authenticate using Kerberos, routing flow file to failure.", e);
                session.transfer(flowFile, FAILURE);
            }
        } else {
            submitWorkflow(context, session, flowFile, AuthType.SIMPLE.name());
        }
    }

    private void submitWorkflow(ProcessContext context, ProcessSession session, FlowFile flowFile, String authMethod) {
        try {
            SSLContext sslContext = context.getProperty(SSL_CONTEXT_SERVICE).isSet()
                    ? context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(RestrictedSSLContextService.class).createSSLContext(ClientAuth.NONE) : null;

            // initialize Oozie client
            SslAuthOozieClient client = new SslAuthOozieClient(context.getProperty(URL).evaluateAttributeExpressions(flowFile).getValue(), authMethod, sslContext);

            // set the workflow application path
            Properties conf = client.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, context.getProperty(APP_PATH).evaluateAttributeExpressions(flowFile).getValue());

            // set the parameters (from processor)
            for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                PropertyDescriptor property = entry.getKey();
                if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                    conf.setProperty(property.getName(), context.getProperty(property).evaluateAttributeExpressions().getValue());
                }
            }

            // set the parameters (from flow files)
            if (regexAttributesToSend != null) {
                Map<String, String> attributes = flowFile.getAttributes();
                Matcher m = regexAttributesToSend.matcher("");
                for (Map.Entry<String, String> entry : attributes.entrySet()) {
                    String key = trimToEmpty(entry.getKey());
                    m.reset(key);
                    if (m.matches()) {
                        conf.setProperty(key, trimToEmpty(entry.getValue()));
                    }
                }
            }

            // submit and start the workflow job
            String jobId = client.run(conf);
            session.putAttribute(flowFile, OOZIE_WORKFLOW_JOB_ID, jobId);
            session.transfer(flowFile, SUCCESS);

        } catch (OozieClientException e) {
            getLogger().error("Failed to submit and start the Oozie workflow, routing flow file to failure.", e);
            session.transfer(flowFile, FAILURE);
        }
    }
}
