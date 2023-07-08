package org.apache.nifi.processors.aws.lambda;

import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.FunctionConfiguration;
import com.amazonaws.services.lambda.model.ListFunctionsResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.s3.ListS3;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestListLambda {
    private TestRunner runner = null;
    private ListLambda mockListLambda = null;

    private AWSLambdaClient mockLambdaClient = null;

    @BeforeEach
    public void setUp() {
        mockLambdaClient = Mockito.mock(AWSLambdaClient.class);
        mockListLambda = new ListLambda() {
            @Override
            protected AWSLambdaClient getClient(ProcessContext context) {
                return mockLambdaClient;
            }
        };
        runner = TestRunners.newTestRunner(mockListLambda);
    }

    @Test
    public void testSinlgePage() {
        runner.setProperty(ListLambda.USE_VERSIONS, "true");

        /**
         * Create 10 FunctionConfiguration objects and mock the listFunctions() method to return them.
         */
        Collection<FunctionConfiguration> functions = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            FunctionConfiguration functionConfiguration = new FunctionConfiguration().
                    withFunctionName("functionName" + i).
                    withFunctionArn("arn:aws:lambda:us-west-2:123456789012:function:helloworld" + i).
                    withRuntime("java8").
                    withRole("arn:aws:iam::123456789012:role/lambda-role" + i).
                    withHandler("helloworld" + i).
                    withCodeSize(123456789L).
                    withDescription("Hello World function" + i).
                    withTimeout(3).
                    withMemorySize(128).
                    withLastModified("2016-11-21T19:49:13.123+0000").
                    withCodeSha256("1234567890123456789012345678901234567890123456789012345678901234" + i).
                    withVersion("$LATEST").
                    withVpcConfig(null).
                    withDeadLetterConfig(null).
                    withEnvironment(null).
                    withKMSKeyArn(null).
                    withTracingConfig(null).
                    withMasterArn(null).
                    withRevisionId("12345678-1234-1234-1234-123456789012" + i);
            functions.add(functionConfiguration);
        }
        ListFunctionsResult listFunctionsResult = new ListFunctionsResult().withFunctions(functions);
        Mockito.when(mockLambdaClient.listFunctions(Mockito.any())).thenReturn(listFunctionsResult);
        runner.run();
        runner.assertAllFlowFilesTransferred(ListS3.REL_SUCCESS, 1);
        // parse the flowfile content to verify the output
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS).get(0);
        String flowFileContent = new String(runner.getContentAsByteArray(flowFile));
        JsonElement jsonElement = gson.fromJson(flowFileContent, JsonElement.class);
        JsonArray jsonArray = jsonElement.getAsJsonArray();
        assertEquals(10, jsonArray.size());
    }
}
