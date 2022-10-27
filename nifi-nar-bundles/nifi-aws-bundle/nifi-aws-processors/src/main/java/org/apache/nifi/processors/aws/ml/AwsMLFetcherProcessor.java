package org.apache.nifi.processors.aws.ml;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.SdkHttpMetadata;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

abstract public class AwsMLFetcherProcessor<SERVICE extends AmazonWebServiceClient>
        extends AbstractAWSCredentialsProviderProcessor<SERVICE>  {
    public static final String AWS_TASK_ID_PROPERTY = "awsTaskId";
    public static final String AWS_TASK_OUTPUT_LOCATION = "outputLocation";
    public static final PropertyDescriptor MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(AWS_CREDENTIALS_PROVIDER_SERVICE)
                    .required(true)
                    .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Upon successful completion, the original FlowFile will be routed to this relationship.")
            .autoTerminateDefault(true)
            .build();
    public static final Relationship REL_IN_PROGRESS = new Relationship.Builder()
            .name("in progress")
            .description("The job is currently still being processed")
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Job successfully finished. FlowFile will be routed to this relation.")
            .build();
    public final Relationship REL_PARTIAL_SUCCESS = new Relationship.Builder()
            .name("partial success")
            .description("Retrieving results failed for some reason, but the issue is likely to resolve on its own, such as Provisioned Throughput Exceeded or a Throttling failure. " +
                    "It is generally expected to retry this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The job failed, the original FlowFile will be routed to this relationship.")
            .autoTerminateDefault(true)
            .build();
    protected final ObjectMapper mapper = JsonMapper.builder()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .build();
    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE,
            TIMEOUT,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            PROXY_CONFIGURATION_SERVICE,
            PROXY_HOST,
            PROXY_HOST_PORT,
            PROXY_USERNAME,
            PROXY_PASSWORD));

    public static final String FAILURE_REASON_ATTRIBUTE = "failure.reason";

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_ORIGINAL,
            REL_SUCCESS,
            REL_IN_PROGRESS,
            REL_FAILURE
    )));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected SERVICE createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
        throw new UnsupportedOperationException("Tried to create client in a deprecated way.");
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ResponseMetadata.class, new AwsResponseMetadataDeserializer());
        SimpleModule module2 = new SimpleModule();
        module.addDeserializer(SdkHttpMetadata.class, new SdkHttpMetadataDeserializer());
        mapper.registerModule(module);
        mapper.registerModule(module2);
    }


    protected void writeToFlowFile(ProcessSession session, FlowFile flowFile, Object response) {
        session.write(flowFile, out -> {
            try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
                bufferedWriter.write(mapper.writeValueAsString(response));
                bufferedWriter.newLine();
                bufferedWriter.flush();
            }
        });
    }
}
