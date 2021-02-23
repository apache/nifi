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
package org.apache.nifi.processors.aws.kinesis.stream;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class TestGetKinesisStream {
    private final TestRunner runner = TestRunners.newTestRunner(GetKinesisStream.class);

    @Before
    public void setUp() {
        runner.setProperty(GetKinesisStream.KINESIS_STREAM_NAME, "test-stream");
        runner.setProperty(GetKinesisStream.APPLICATION_NAME, "test-application");
        runner.assertValid();
    }

    @Test
    public void testValidWithCredentialsProperties() {
        runner.setProperty(GetKinesisStream.ACCESS_KEY, "access-key");
        runner.setProperty(GetKinesisStream.SECRET_KEY, "secret-key");
        runner.assertValid();

        ((GetKinesisStream) runner.getProcessor()).onScheduled(runner.getProcessContext());
        assertThat(runner.getLogger().getInfoMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Creating client using aws credentials")), is(true));

        // "raw" credentials are put into a static credentials provider for creating the client
        assertThat(runner.getLogger().getInfoMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Creating client using aws credentials provider")), is(true));
    }

    @Test
    public void testValidWithCredentialsProvider() throws InitializationException {
        final ControllerService credentialsProvider = new AWSCredentialsProviderControllerService();
        runner.addControllerService("credentials-provider", credentialsProvider);
        runner.setProperty(credentialsProvider, CredentialPropertyDescriptors.ACCESS_KEY, "access-key");
        runner.setProperty(credentialsProvider, CredentialPropertyDescriptors.SECRET_KEY, "secret-key");
        runner.assertValid(credentialsProvider);
        runner.enableControllerService(credentialsProvider);
        runner.setProperty(GetKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials-provider");
        runner.assertValid();

        ((GetKinesisStream) runner.getProcessor()).onScheduled(runner.getProcessContext());
        assertThat(runner.getLogger().getInfoMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Creating client using aws credentials provider")), is(true));

        // "raw" credentials aren't used
        assertThat(runner.getLogger().getInfoMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Creating client using aws credentials")), is(false));
    }

    @Test
    public void testMissingMandatoryProperties() {
        runner.removeProperty(GetKinesisStream.KINESIS_STREAM_NAME);
        runner.removeProperty(GetKinesisStream.APPLICATION_NAME);
        runner.removeProperty(GetKinesisStream.ACCESS_KEY);
        runner.removeProperty(GetKinesisStream.SECRET_KEY);
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 2 validation failures:\n" +
                        "'%s' is invalid because %s is required\n" +
                        "'%s' is invalid because %s is required\n",
                GetKinesisStream.KINESIS_STREAM_NAME.getDisplayName(), GetKinesisStream.KINESIS_STREAM_NAME.getDisplayName(),
                GetKinesisStream.APPLICATION_NAME.getDisplayName(), GetKinesisStream.APPLICATION_NAME.getDisplayName()
        )));
    }

    @Test
    public void testInvalidProperties() {
        runner.setProperty(GetKinesisStream.APPLICATION_NAME, " ");
        runner.setProperty(GetKinesisStream.TIMESTAMP_FORMAT, "not-valid-format");
        runner.setProperty(GetKinesisStream.RETRY_WAIT_MILLIS, "not-a-long");
        runner.setProperty(GetKinesisStream.NUM_RETRIES, "not-an-int");
        runner.setProperty(GetKinesisStream.CHECKPOINT_INTERVAL_MILLIS, "not-a-long");
        runner.setProperty(GetKinesisStream.REPORT_CLOUDWATCH_METRICS, "not-a-boolean");
        runner.setProperty(GetKinesisStream.DYNAMODB_ENDPOINT_OVERRIDE, "not-a-url");
        runner.setProperty(GetKinesisStream.INITIAL_STREAM_POSITION, "not-an-enum-match");
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 8 validation failures:\n" +
                        "'%s' validated against ' ' is invalid because %s must contain at least one character that is not white space\n" +
                        "'%s' validated against 'not-a-url' is invalid because Not a valid URL\n" +
                        "'%s' validated against 'not-an-enum-match' is invalid because Given value not found in allowed set '%s, %s, %s'\n" +
                        "'%s' validated against 'not-valid-format' is invalid because Must be a valid java.time.DateTimeFormatter pattern, e.g. %s\n" +
                        "'%s' validated against 'not-a-long' is invalid because not a valid Long\n" +
                        "'%s' validated against 'not-an-int' is invalid because not a valid integer\n" +
                        "'%s' validated against 'not-a-long' is invalid because not a valid Long\n" +
                        "'%s' validated against 'not-a-boolean' is invalid because Value must be 'true' or 'false'\n",
                GetKinesisStream.APPLICATION_NAME.getName(), GetKinesisStream.APPLICATION_NAME.getName(),
                GetKinesisStream.DYNAMODB_ENDPOINT_OVERRIDE.getName(),
                GetKinesisStream.INITIAL_STREAM_POSITION.getName(), GetKinesisStream.LATEST.getDisplayName(),
                GetKinesisStream.TRIM_HORIZON.getDisplayName(), GetKinesisStream.AT_TIMESTAMP.getDisplayName(),
                GetKinesisStream.TIMESTAMP_FORMAT.getName(), RecordFieldType.TIMESTAMP.getDefaultFormat(),
                GetKinesisStream.CHECKPOINT_INTERVAL_MILLIS.getName(),
                GetKinesisStream.NUM_RETRIES.getName(),
                GetKinesisStream.RETRY_WAIT_MILLIS.getName(),
                GetKinesisStream.REPORT_CLOUDWATCH_METRICS.getName()
        )));
    }

    @Test
    public void testMissingStreamPositionTimestamp() {
        runner.setProperty(GetKinesisStream.INITIAL_STREAM_POSITION, InitialPositionInStream.AT_TIMESTAMP.toString());
        runner.removeProperty(GetKinesisStream.STREAM_POSITION_TIMESTAMP);
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 1 validation failures:\n" +
                        "'%s' is invalid because %s must be provided when %s is %s\n",
                GetKinesisStream.STREAM_POSITION_TIMESTAMP.getName(), GetKinesisStream.STREAM_POSITION_TIMESTAMP.getDisplayName(),
                GetKinesisStream.INITIAL_STREAM_POSITION.getDisplayName(), InitialPositionInStream.AT_TIMESTAMP.toString()
        )));
    }

    @Test
    public void testInvalidStreamPositionTimestamp() {
        runner.setProperty(GetKinesisStream.INITIAL_STREAM_POSITION, InitialPositionInStream.AT_TIMESTAMP.toString());
        runner.setProperty(GetKinesisStream.TIMESTAMP_FORMAT, "yyyy-MM-dd");
        runner.setProperty(GetKinesisStream.STREAM_POSITION_TIMESTAMP, "12:00:00");
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 1 validation failures:\n" +
                        "'%s' is invalid because %s must be parsable by %s\n",
                GetKinesisStream.STREAM_POSITION_TIMESTAMP.getName(),
                GetKinesisStream.STREAM_POSITION_TIMESTAMP.getDisplayName(),
                GetKinesisStream.TIMESTAMP_FORMAT.getDisplayName()
        )));
    }

    /*
     * Trigger a run of the GetKinesisStream processor, but expect the KCL Worker to fail (it needs connections to AWS resources)
     * Assert that our code is being called by checking log output. The ITGetKinesisStream integration tests prove actual AWS connectivity
     */
    @Test
    public void testRunWorker() throws UnknownHostException {
        final TestRunner mockGetKinesisStreamRuner = TestRunners.newTestRunner(MockGetKinesisStream.class);

        mockGetKinesisStreamRuner.setProperty(GetKinesisStream.KINESIS_STREAM_NAME, "test-stream");
        mockGetKinesisStreamRuner.setProperty(GetKinesisStream.APPLICATION_NAME, "test-application");
        mockGetKinesisStreamRuner.setProperty(GetKinesisStream.ACCESS_KEY, "test-access");
        mockGetKinesisStreamRuner.setProperty(GetKinesisStream.SECRET_KEY, "test-secret");
        mockGetKinesisStreamRuner.setProperty(GetKinesisStream.REGION, Regions.EU_WEST_2.getName());

        // speed up init process for the unit test (And show use of dynamic properties to configure KCL)
        mockGetKinesisStreamRuner.setProperty("maxInitializationAttempts", "1");
        mockGetKinesisStreamRuner.setProperty("parentShardPollIntervalMillis", "1");

        mockGetKinesisStreamRuner.assertValid();

        mockGetKinesisStreamRuner.run();

        final String hostname = InetAddress.getLocalHost().getCanonicalHostName();

        final MockGetKinesisStream processor = ((MockGetKinesisStream) mockGetKinesisStreamRuner.getProcessor());
        assertKinesisClientLibConfiguration(processor.kinesisClientLibConfiguration, hostname);
        assertThat(processor.workerBuilder.build().getApplicationName(), equalTo("test-application"));

        // confirm the Kinesis Worker initialisation was attempted
        assertThat(mockGetKinesisStreamRuner.getLogger().getInfoMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().contains(String.format(
                        "Kinesis Worker prepared for application %s to process stream %s as worker ID %s:",
                        "test-application", "test-stream", hostname
                ))), is(true));

        // confirm the processor worked through the onTrigger and stopConsuming methods
        assertThat(mockGetKinesisStreamRuner.getLogger().getDebugMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Starting Kinesis Worker")), is(true));
        assertThat(mockGetKinesisStreamRuner.getLogger().getDebugMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Kinesis Worker finished")), is(true));
        assertThat(mockGetKinesisStreamRuner.getLogger().getDebugMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Requesting Kinesis Worker shutdown")), is(true));
        assertThat(mockGetKinesisStreamRuner.getLogger().getDebugMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Kinesis Worker shutdown")), is(true));

        assertThat(mockGetKinesisStreamRuner.getLogger().getWarnMessages().isEmpty(), is(true));
        assertThat(mockGetKinesisStreamRuner.getLogger().getErrorMessages().isEmpty(), is(true));
    }

    @Test
    public void testInvalidDynamicKCLProperties() {
        // blank properties
        runner.setProperty("", "empty");
        runner.setProperty(" ", "blank");

        // invalid property names
        runner.setProperty("withPrefixNotAllowed", "a-value");
        runner.setProperty("UpperCaseLeadingCharacterNotAllowed", "another-value");
        runner.setProperty("unknownProperty", "a-third-value");
        runner.setProperty("toString", "cannot-call");

        // invalid property names (cannot use nested/indexed/mapped properties via BeanUtils)
        runner.setProperty("no.allowed", "no-.");
        runner.setProperty("no[allowed", "no-[");
        runner.setProperty("no]allowed", "no-]");
        runner.setProperty("no(allowed", "no-(");
        runner.setProperty("no)allowed", "no-)");

        // can't override static properties
        runner.setProperty("regionName", Regions.AF_SOUTH_1.getName());
        runner.setProperty("timestampAtInitialPositionInStream", "2021-01-01 00:00:00");
        runner.setProperty("initialPositionInStream", "AT_TIMESTAMP");
        runner.setProperty("dynamoDBEndpoint", "http://localhost:4566/dynamodb");
        runner.setProperty("kinesisEndpoint", "http://localhost:4566/kinesis");

        // invalid parameter conversions
        runner.setProperty("dynamoDBClientConfig", "too-complex");
        runner.setProperty("shutdownGraceMillis", "not-long");

        final AssertionError ae = assertThrows(AssertionError.class, runner::assertValid);
        assertThat(ae.getMessage(), startsWith("Processor has 18 validation failures:\n"));

        // blank properties
        assertThat(ae.getMessage(), containsString("'Property Name' validated against '' is invalid because Invalid attribute key: <Empty String>\n"));
        assertThat(ae.getMessage(), containsString("'Property Name' validated against ' ' is invalid because Invalid attribute key: <Empty String>\n"));

        // invalid property names
        assertThat(ae.getMessage(), containsString(
                "'withPrefixNotAllowed' validated against 'a-value' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a lowercase letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'UpperCaseLeadingCharacterNotAllowed' validated against 'another-value' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a lowercase letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'unknownProperty' validated against 'a-third-value' is invalid because Kinesis Client Library Configuration property with name " +
                "withUnknownProperty does not exist or is not writable\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'toString' validated against 'cannot-call' is invalid because Kinesis Client Library Configuration property with name " +
                "withToString does not exist or is not writable\n"
        ));

        // invalid property names (cannot use nested/indexed/mapped properties via BeanUtils)
        assertThat(ae.getMessage(), containsString(
                "'no.allowed' validated against 'no-.' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a lowercase letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'no[allowed' validated against 'no-[' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a lowercase letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'no]allowed' validated against 'no-]' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a lowercase letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'no(allowed' validated against 'no-(' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a lowercase letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'no)allowed' validated against 'no-)' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a lowercase letter and contain only letters, numbers or underscores\n"
        ));

        // can't override static properties
        assertThat(ae.getMessage(), containsString("'regionName' validated against 'af-south-1' is invalid because Use \"Region\" instead of a dynamic property\n"));
        assertThat(ae.getMessage(), containsString(
                "'timestampAtInitialPositionInStream' validated against '2021-01-01 00:00:00' is invalid because Use \"Stream Position Timestamp\" instead of a dynamic property\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'initialPositionInStream' validated against 'AT_TIMESTAMP' is invalid because Use \"Initial Stream Position\" instead of a dynamic property\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'dynamoDBEndpoint' validated against 'http://localhost:4566/dynamodb' is invalid because Use \"DynamoDB Override\" instead of a dynamic property\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'kinesisEndpoint' validated against 'http://localhost:4566/kinesis' is invalid because Use \"Amazon Kinesis Stream Name\" instead of a dynamic property\n"
        ));

        // invalid parameter conversions
        assertThat(ae.getMessage(), containsString(
                "'dynamoDBClientConfig' validated against 'too-complex' is invalid because Kinesis Client Library Configuration property " +
                "with name withDynamoDBClientConfig cannot be used with value \"too-complex\" : " +
                "Cannot invoke com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration.withDynamoDBClientConfig on bean class " +
                "'class com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration' - argument type mismatch - " +
                "had objects of type \"java.lang.String\" but expected signature \"com.amazonaws.ClientConfiguration\"\n"
        ));
        assertThat(ae.getMessage(), containsString("'shutdownGraceMillis' validated against 'not-long' is invalid because " +
                "Kinesis Client Library Configuration property with name withShutdownGraceMillis " +
                "cannot be used with value \"not-long\" : Value of ShutdownGraceMillis should be positive, but current value is 0\n"));
    }

    @Test
    public void testValidDynamicKCLProperties() {
        runner.setProperty("billingMode", "PROVISIONED"); // enum
        runner.setProperty("leaseCleanupIntervalMillis", "1000"); // long
        runner.setProperty("cleanupLeasesUponShardCompletion", "true"); // boolean
        runner.setProperty("maxInitializationAttempts", "1"); // int
        runner.setProperty("dataFetchingStrategy", "DEFAULT"); // String

        runner.assertValid();
    }

    private void assertKinesisClientLibConfiguration(final KinesisClientLibConfiguration kinesisClientLibConfiguration, final String hostname) {
        assertThat(kinesisClientLibConfiguration.getWorkerIdentifier(), startsWith(hostname));
        assertThat(kinesisClientLibConfiguration.getApplicationName(), equalTo("test-application"));
        assertThat(kinesisClientLibConfiguration.getStreamName(), equalTo("test-stream"));

        assertThat(kinesisClientLibConfiguration.getKinesisCredentialsProvider().getCredentials().getAWSAccessKeyId(), equalTo("test-access"));
        assertThat(kinesisClientLibConfiguration.getKinesisCredentialsProvider().getCredentials().getAWSSecretKey(), equalTo("test-secret"));
        assertThat(kinesisClientLibConfiguration.getDynamoDBCredentialsProvider().getCredentials().getAWSAccessKeyId(), equalTo("test-access"));
        assertThat(kinesisClientLibConfiguration.getDynamoDBCredentialsProvider().getCredentials().getAWSSecretKey(), equalTo("test-secret"));
        assertThat(kinesisClientLibConfiguration.getCloudWatchCredentialsProvider().getCredentials().getAWSAccessKeyId(), equalTo("test-access"));
        assertThat(kinesisClientLibConfiguration.getCloudWatchCredentialsProvider().getCredentials().getAWSSecretKey(), equalTo("test-secret"));

        assertThat(kinesisClientLibConfiguration.getRegionName(), equalTo(Regions.EU_WEST_2.getName()));
        assertThat(kinesisClientLibConfiguration.getInitialPositionInStream(), equalTo(InitialPositionInStream.LATEST));
        assertThat(kinesisClientLibConfiguration.getDynamoDBEndpoint(), nullValue());
        assertThat(kinesisClientLibConfiguration.getKinesisEndpoint(), nullValue());

        assertThat(kinesisClientLibConfiguration.getKinesisClientConfiguration(), instanceOf(ClientConfiguration.class));
        assertThat(kinesisClientLibConfiguration.getDynamoDBClientConfiguration(), instanceOf(ClientConfiguration.class));
        assertThat(kinesisClientLibConfiguration.getCloudWatchClientConfiguration(), instanceOf(ClientConfiguration.class));

        assertThat(kinesisClientLibConfiguration.getMaxInitializationAttempts(), equalTo(1));
        assertThat(kinesisClientLibConfiguration.getParentShardPollIntervalMillis(), equalTo(1L));
    }

    public static class MockGetKinesisStream extends GetKinesisStream {
        KinesisClientLibConfiguration kinesisClientLibConfiguration;
        Worker.Builder workerBuilder;

        @Override
        Worker.Builder prepareWorkerBuilder(final KinesisClientLibConfiguration kinesisClientLibConfiguration,
                                            final IRecordProcessorFactory factory, final ProcessContext context) {
            workerBuilder = super.prepareWorkerBuilder(kinesisClientLibConfiguration, factory, context);
            return workerBuilder;
        }

        @Override
        KinesisClientLibConfiguration prepareKinesisClientLibConfiguration(final ProcessContext context, final String appName,
                                                                           final String streamName, final String workerId) {
            kinesisClientLibConfiguration =
                    super.prepareKinesisClientLibConfiguration(context, appName, streamName, workerId);

            return kinesisClientLibConfiguration;
        }
    }
}
