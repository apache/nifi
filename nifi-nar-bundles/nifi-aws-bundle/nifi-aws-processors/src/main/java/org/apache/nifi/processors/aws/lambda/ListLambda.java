package org.apache.nifi.processors.aws.lambda;

import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.FunctionConfiguration;
import com.amazonaws.services.lambda.model.FunctionVersion;
import com.amazonaws.services.lambda.model.ListFunctionsRequest;
import com.amazonaws.services.lambda.model.ListFunctionsResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;


public class ListLambda extends AbstractAWSLambdaProcessor {
    public static final PropertyDescriptor USE_VERSIONS = new PropertyDescriptor.Builder()
            .name("use-versions")
            .displayName("Use Versions")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to use function versions, if applicable.  If false, only the latest version of each function will be returned.")
            .build();

    public static final Set<Relationship> relationships = Collections.singleton(REL_SUCCESS);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        //return USE_VERSIONS as the only supported property
        return Collections.unmodifiableList(Arrays.asList(
                REGION,
                ACCESS_KEY,
                SECRET_KEY,
                CREDENTIALS_FILE,
                AWS_CREDENTIALS_PROVIDER_SERVICE,
                TIMEOUT,
                SSL_CONTEXT_SERVICE,
                ENDPOINT_OVERRIDE,
                PROXY_CONFIGURATION_SERVICE,
                PROXY_HOST,
                PROXY_HOST_PORT,
                PROXY_USERNAME,
                PROXY_PASSWORD,
                USE_VERSIONS));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        AWSLambdaClient lambdaClient = getClient(context);
        String marker = null;

        do {
            ListFunctionsRequest request = new ListFunctionsRequest().withMarker(marker);
            if(context.getProperty(USE_VERSIONS).asBoolean()) {
                request.withFunctionVersion(FunctionVersion.ALL);
            }

            ListFunctionsResult result = lambdaClient.listFunctions(request);
            List<FunctionConfiguration> functions = result.getFunctions();

            // Process the page of results and write to flowfile
            String json = toJson(functions);
            FlowFile flowFile = session.create();
            session.write(flowFile, (outputStream) -> {
                outputStream.write(json.getBytes());
            });
            // Transfer the flow file to the success relationship
            session.transfer(flowFile, REL_SUCCESS);
            marker = result.getNextMarker();
        } while (marker != null);
    }

    /**
     * Convert a list of FunctionConfiguration objects to JSON
     */
    private String toJson(List<FunctionConfiguration> functions) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(functions);
    }
}
