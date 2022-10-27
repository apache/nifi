package org.apache.nifi.processors.aws.ml;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

public abstract class AwsMlProcessor<SERVICE extends AmazonWebServiceClient, REQUEST extends AmazonWebServiceRequest, RESPONSE extends AmazonWebServiceResult> extends AbstractAWSCredentialsProviderProcessor<SERVICE> {
    protected static final String AWS_TASK_ID_PROPERTY = "awsTaskId";
    public static final PropertyDescriptor JSON_PAYLOAD = new PropertyDescriptor.Builder()
            .name("json-payload")
            .displayName("JSON Payload")
            .description("JSON Payload that represent an AWS ML Request. See more details in AWS API documentation.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(AWS_CREDENTIALS_PROVIDER_SERVICE)
                    .required(true)
                    .build();
    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            JSON_PAYLOAD,
            MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE,
            TIMEOUT,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            PROXY_CONFIGURATION_SERVICE,
            PROXY_HOST,
            PROXY_HOST_PORT,
            PROXY_USERNAME,
            PROXY_PASSWORD));
    private final ObjectMapper mapper = JsonMapper.builder()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        RESPONSE response;
        try {
            response = sendRequest(buildRequest(session, context, flowFile), context);
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Exception was thrown during sending AWS ML request.", e);
            return;
        }

        try {
            writeToFlowFile(session, flowFile, response);
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Exception was thrown during writing aws response to flow file.", e);
            return;
        }

        try {
            postProcessFlowFile(context, session, flowFile, response);
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Exception was thrown during AWS ML post processing.", e);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    protected void postProcessFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile, RESPONSE response) {
        session.putAttribute(flowFile, AWS_TASK_ID_PROPERTY, getAwsTaskId(context, response));
        getLogger().debug("AWS ML task has been started with task id: {}", getAwsTaskId(context, response));
    }

    protected REQUEST buildRequest(ProcessSession session, ProcessContext context, FlowFile flowFile) throws JsonProcessingException {
        REQUEST request;
        try {
            request = mapper.readValue(getPayload(session, context, flowFile),
                    getAwsRequestClass(context));
        } catch (JsonProcessingException e) {
            getLogger().error("Exception was thrown during AWS ML request creation.", e);
            throw e;
        }
        return request;
    }

    private String getPayload(ProcessSession session, ProcessContext context, FlowFile flowFile) {
        String payloadPropertyValue = context.getProperty(JSON_PAYLOAD).evaluateAttributeExpressions(flowFile).getValue();
        if (payloadPropertyValue == null) {
            payloadPropertyValue = readFlowFile(session, flowFile);
        }
        return payloadPropertyValue;
    }

    @Override
    protected SERVICE createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
        throw new UnsupportedOperationException("Tried to create client in a deprecated way.");
    }

    abstract protected RESPONSE sendRequest(REQUEST request, ProcessContext context) throws JsonProcessingException;

    abstract protected Class<? extends REQUEST> getAwsRequestClass(ProcessContext context);

    abstract protected String getAwsTaskId(ProcessContext context, RESPONSE response);

    protected void writeToFlowFile(ProcessSession session, FlowFile flowFile, RESPONSE response) {
        session.write(flowFile, out -> {
            try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
                bufferedWriter.write(mapper.writeValueAsString(response));
                bufferedWriter.newLine();
                bufferedWriter.flush();
            }
        });
    }

    protected String readFlowFile(final ProcessSession session, final FlowFile flowFile) {
        try (InputStream inputStream = session.read(flowFile)) {
            return new String(IOUtils.toByteArray(inputStream));
        } catch (final IOException e) {
            throw new ProcessException("Read FlowFile Failed", e);
        }
    }
}
