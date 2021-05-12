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
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.WorkerStateChangeListener;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestConsumeKinesisStream {
    private final TestRunner runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);

    @Before
    public void setUp() throws InitializationException {
        runner.setProperty(ConsumeKinesisStream.KINESIS_STREAM_NAME, "test-stream");
        runner.setProperty(ConsumeKinesisStream.APPLICATION_NAME, "test-application");

        // use anonymous credentials by default
        final ControllerService credentialsProvider = new AWSCredentialsProviderControllerService();
        runner.addControllerService("credentials-provider", credentialsProvider);
        runner.setProperty(credentialsProvider, CredentialPropertyDescriptors.USE_ANONYMOUS_CREDENTIALS, "true");
        runner.assertValid(credentialsProvider);
        runner.enableControllerService(credentialsProvider);
        runner.setProperty(ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials-provider");

        runner.assertValid();
    }

    @Test
    public void testValidWithCredentials() throws InitializationException {
        final ControllerService credentialsProvider = new AWSCredentialsProviderControllerService();
        runner.addControllerService("credentials-provider", credentialsProvider);
        runner.setProperty(credentialsProvider, CredentialPropertyDescriptors.ACCESS_KEY, "access-key");
        runner.setProperty(credentialsProvider, CredentialPropertyDescriptors.SECRET_KEY, "secret-key");
        runner.assertValid(credentialsProvider);
        runner.enableControllerService(credentialsProvider);
        runner.setProperty(ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials-provider");
        runner.assertValid();

        ((ConsumeKinesisStream) runner.getProcessor()).onScheduled(runner.getProcessContext());
        assertThat(runner.getLogger().getInfoMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Creating client using aws credentials provider")), is(true));

        // "raw" credentials aren't used
        assertThat(runner.getLogger().getInfoMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().endsWith("Creating client using aws credentials")), is(false));
    }

    @Test
    public void testMissingMandatoryProperties() {
        runner.removeProperty(ConsumeKinesisStream.KINESIS_STREAM_NAME);
        runner.removeProperty(ConsumeKinesisStream.APPLICATION_NAME);
        runner.removeProperty(ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE);
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 3 validation failures:\n" +
                        "'%s' is invalid because %s is required\n" +
                        "'%s' is invalid because %s is required\n" +
                        "'%s' is invalid because %s is required\n",
                ConsumeKinesisStream.KINESIS_STREAM_NAME.getDisplayName(), ConsumeKinesisStream.KINESIS_STREAM_NAME.getDisplayName(),
                ConsumeKinesisStream.APPLICATION_NAME.getDisplayName(), ConsumeKinesisStream.APPLICATION_NAME.getDisplayName(),
                ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE.getDisplayName(), ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE.getDisplayName()
        )));
    }

    @Test
    public void testInvalidProperties() {
        runner.setProperty(ConsumeKinesisStream.APPLICATION_NAME, " ");
        runner.setProperty(ConsumeKinesisStream.TIMESTAMP_FORMAT, "not-valid-format");
        runner.setProperty(ConsumeKinesisStream.RETRY_WAIT, "not-a-long");
        runner.setProperty(ConsumeKinesisStream.NUM_RETRIES, "not-an-int");
        runner.setProperty(ConsumeKinesisStream.FAILOVER_TIMEOUT, "not-a-period");
        runner.setProperty(ConsumeKinesisStream.GRACEFUL_SHUTDOWN_TIMEOUT, "not-a-period");
        runner.setProperty(ConsumeKinesisStream.CHECKPOINT_INTERVAL, "not-a-long");
        runner.setProperty(ConsumeKinesisStream.REPORT_CLOUDWATCH_METRICS, "not-a-boolean");
        runner.setProperty(ConsumeKinesisStream.DYNAMODB_ENDPOINT_OVERRIDE, "not-a-url");
        runner.setProperty(ConsumeKinesisStream.INITIAL_STREAM_POSITION, "not-an-enum-match");
        runner.setProperty(ConsumeKinesisStream.RECORD_READER, "not-a-reader");
        runner.setProperty(ConsumeKinesisStream.RECORD_WRITER, "not-a-writer");
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 14 validation failures:\n" +
                        "'%s' validated against ' ' is invalid because %s must contain at least one character that is not white space\n" +
                        "'%s' validated against 'not-a-reader' is invalid because Property references a Controller Service that does not exist\n" +
                        "'%s' validated against 'not-a-writer' is invalid because Property references a Controller Service that does not exist\n" +
                        "'%s' validated against 'not-a-url' is invalid because Not a valid URL\n" +
                        "'%s' validated against 'not-an-enum-match' is invalid because Given value not found in allowed set '%s, %s, %s'\n" +
                        "'%s' validated against 'not-valid-format' is invalid because Must be a valid java.time.DateTimeFormatter pattern, e.g. %s\n" +
                        "'%s' validated against 'not-a-period' is invalid because Must be of format <duration> <TimeUnit> where <duration> is a non-negative integer and " +
                        "TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days\n" +
                        "'%s' validated against 'not-a-period' is invalid because Must be of format <duration> <TimeUnit> where <duration> is a non-negative integer and " +
                        "TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days\n" +
                        "'%s' validated against 'not-a-long' is invalid because Must be of format <duration> <TimeUnit> where <duration> is a non-negative integer and " +
                        "TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days\n" +
                        "'%s' validated against 'not-an-int' is invalid because not a valid integer\n" +
                        "'%s' validated against 'not-a-long' is invalid because Must be of format <duration> <TimeUnit> where <duration> is a non-negative integer and " +
                        "TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days\n" +
                        "'%s' validated against 'not-a-boolean' is invalid because Given value not found in allowed set 'true, false'\n" +
                        "'%s' validated against 'not-a-reader' is invalid because Invalid Controller Service: not-a-reader is not a valid Controller Service Identifier\n" +
                        "'%s' validated against 'not-a-writer' is invalid because Invalid Controller Service: not-a-writer is not a valid Controller Service Identifier\n",
                ConsumeKinesisStream.APPLICATION_NAME.getName(), ConsumeKinesisStream.APPLICATION_NAME.getName(),
                ConsumeKinesisStream.RECORD_READER.getDisplayName(),
                ConsumeKinesisStream.RECORD_WRITER.getDisplayName(),
                ConsumeKinesisStream.DYNAMODB_ENDPOINT_OVERRIDE.getName(),
                ConsumeKinesisStream.INITIAL_STREAM_POSITION.getName(), ConsumeKinesisStream.LATEST.getDisplayName(),
                ConsumeKinesisStream.TRIM_HORIZON.getDisplayName(), ConsumeKinesisStream.AT_TIMESTAMP.getDisplayName(),
                ConsumeKinesisStream.TIMESTAMP_FORMAT.getName(), RecordFieldType.TIMESTAMP.getDefaultFormat(),
                ConsumeKinesisStream.FAILOVER_TIMEOUT.getName(),
                ConsumeKinesisStream.GRACEFUL_SHUTDOWN_TIMEOUT.getName(),
                ConsumeKinesisStream.CHECKPOINT_INTERVAL.getName(),
                ConsumeKinesisStream.NUM_RETRIES.getName(),
                ConsumeKinesisStream.RETRY_WAIT.getName(),
                ConsumeKinesisStream.REPORT_CLOUDWATCH_METRICS.getName(),
                ConsumeKinesisStream.RECORD_READER.getDisplayName(),
                ConsumeKinesisStream.RECORD_WRITER.getDisplayName()
        )));
    }

    @Test
    public void testMissingStreamPositionTimestamp() {
        runner.setProperty(ConsumeKinesisStream.INITIAL_STREAM_POSITION, InitialPositionInStream.AT_TIMESTAMP.toString());
        runner.removeProperty(ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP);
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 1 validation failures:\n" +
                        "'%s' is invalid because %s must be provided when %s is %s\n",
                ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP.getName(), ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP.getDisplayName(),
                ConsumeKinesisStream.INITIAL_STREAM_POSITION.getDisplayName(), InitialPositionInStream.AT_TIMESTAMP
        )));
    }

    @Test
    public void testInvalidStreamPositionTimestamp() {
        runner.setProperty(ConsumeKinesisStream.INITIAL_STREAM_POSITION, InitialPositionInStream.AT_TIMESTAMP.toString());
        runner.setProperty(ConsumeKinesisStream.TIMESTAMP_FORMAT, "yyyy-MM-dd");
        runner.setProperty(ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP, "12:00:00");
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 1 validation failures:\n" +
                        "'%s' is invalid because %s must be parsable by %s\n",
                ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP.getName(),
                ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP.getDisplayName(),
                ConsumeKinesisStream.TIMESTAMP_FORMAT.getDisplayName()
        )));
    }

    @Test
    public void testInvalidRecordReaderWithoutRecordWriter() throws InitializationException {
        final ControllerService service = new JsonTreeReader();
        runner.addControllerService("record-reader", service);
        runner.enableControllerService(service);
        runner.setProperty(ConsumeKinesisStream.RECORD_READER, "record-reader");
        runner.removeProperty(ConsumeKinesisStream.RECORD_WRITER);
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::assertValid);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 1 validation failures:\n" +
                        "'%s' is invalid because %s must be set if %s is set in order to write FlowFiles as Records.\n",
                ConsumeKinesisStream.RECORD_WRITER.getName(),
                ConsumeKinesisStream.RECORD_WRITER.getDisplayName(),
                ConsumeKinesisStream.RECORD_READER.getDisplayName()
        )));
    }

    @Test
    public void testInvalidRecordWriterWithoutRecordReader() throws InitializationException {
        final ControllerService service = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", service);
        runner.enableControllerService(service);
        runner.setProperty(ConsumeKinesisStream.RECORD_WRITER, "record-writer");
        runner.removeProperty(ConsumeKinesisStream.RECORD_READER);
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::assertValid);
        assertThat(assertionError.getMessage(), equalTo(String.format("Processor has 1 validation failures:\n" +
                        "'%s' is invalid because %s must be set if %s is set in order to write FlowFiles as Records.\n",
                ConsumeKinesisStream.RECORD_READER.getName(),
                ConsumeKinesisStream.RECORD_READER.getDisplayName(),
                ConsumeKinesisStream.RECORD_WRITER.getDisplayName()
        )));
    }

    @Test
    public void testRunWorkerWithCredentials() throws UnknownHostException, InitializationException, InterruptedException {
        runWorker(true, false);
    }

    @Test
    public void testRunWorkerUnexpectedShutdown() throws UnknownHostException, InitializationException, InterruptedException {
        runWorker(true, true);
    }

    @Test
    public void testRunWorkerWithoutCredentials() throws UnknownHostException, InitializationException, InterruptedException {
        runWorker(false, false);
    }

    @Test
    public void testInvalidDynamicKCLProperties() {
        // blank properties
        runner.setProperty("", "empty");
        runner.setProperty(" ", "blank");

        // invalid property names
        runner.setProperty("withPrefixNotAllowed", "a-value");
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
        assertThat(ae.getMessage(), startsWith("Processor has 17 validation failures:\n"));

        // blank properties
        assertThat(ae.getMessage(), containsString("'Property Name' validated against '' is invalid because Invalid attribute key: <Empty String>\n"));
        assertThat(ae.getMessage(), containsString("'Property Name' validated against ' ' is invalid because Invalid attribute key: <Empty String>\n"));

        // invalid property names
        assertThat(ae.getMessage(), containsString(
                "'withPrefixNotAllowed' validated against 'a-value' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'unknownProperty' validated against 'a-third-value' is invalid because Kinesis Client Library Configuration property with name " +
                "UnknownProperty does not exist or is not writable\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'toString' validated against 'cannot-call' is invalid because Kinesis Client Library Configuration property with name " +
                "ToString does not exist or is not writable\n"
        ));

        // invalid property names (cannot use nested/indexed/mapped properties via BeanUtils)
        assertThat(ae.getMessage(), containsString(
                "'no.allowed' validated against 'no-.' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'no[allowed' validated against 'no-[' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'no]allowed' validated against 'no-]' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'no(allowed' validated against 'no-(' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a letter and contain only letters, numbers or underscores\n"
        ));
        assertThat(ae.getMessage(), containsString(
                "'no)allowed' validated against 'no-)' is invalid because Property name must not have a prefix of \"with\", " +
                "must start with a letter and contain only letters, numbers or underscores\n"
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
                "'kinesisEndpoint' validated against 'http://localhost:4566/kinesis' is invalid because Use \"Endpoint Override URL\" instead of a dynamic property\n"
        ));

        // invalid parameter conversions
        assertThat(ae.getMessage(), containsString(
                "'dynamoDBClientConfig' validated against 'too-complex' is invalid because Kinesis Client Library Configuration property " +
                "with name DynamoDBClientConfig cannot be used with value \"too-complex\" : " +
                "Cannot invoke com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration.withDynamoDBClientConfig on bean class " +
                "'class com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration' - argument type mismatch - " +
                "had objects of type \"java.lang.String\" but expected signature \"com.amazonaws.ClientConfiguration\"\n"
        ));
        assertThat(ae.getMessage(), containsString("'shutdownGraceMillis' validated against 'not-long' is invalid because " +
                "Kinesis Client Library Configuration property with name ShutdownGraceMillis " +
                "cannot be used with value \"not-long\" : Value of ShutdownGraceMillis should be positive, but current value is 0\n"));
    }

    @Test
    public void testValidDynamicKCLProperties() {
        runner.setProperty("billingMode", "PROVISIONED"); // enum
        runner.setProperty("idleMillisBetweenCalls", "1000"); // long
        runner.setProperty("cleanupLeasesUponShardCompletion", "true"); // boolean
        runner.setProperty("initialLeaseTableReadCapacity", "1"); // int
        runner.setProperty("DataFetchingStrategy", "DEFAULT"); // String with uppercase leading character in property name

        runner.assertValid();
    }

    /*
     * Trigger a run of the ConsumeKinesisStream processor, but expect the KCL Worker to fail (it needs connections to AWS resources)
     * Assert that our code is being called by checking log output. The ITConsumeKinesisStream integration tests prove actual AWS connectivity
     */
    private void runWorker(final boolean withCredentials, final boolean waitForFailure) throws UnknownHostException, InitializationException, InterruptedException {
        final TestRunner mockConsumeKinesisStreamRunner = TestRunners.newTestRunner(MockConsumeKinesisStream.class);

        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.KINESIS_STREAM_NAME, "test-stream");
        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.APPLICATION_NAME, "test-application");
        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.REGION, Regions.EU_WEST_2.getName());

        final AWSCredentialsProviderService awsCredentialsProviderService = new AWSCredentialsProviderControllerService();
        mockConsumeKinesisStreamRunner.addControllerService("aws-credentials", awsCredentialsProviderService);
        if (withCredentials) {
            mockConsumeKinesisStreamRunner.setProperty(awsCredentialsProviderService, CredentialPropertyDescriptors.ACCESS_KEY, "test-access");
            mockConsumeKinesisStreamRunner.setProperty(awsCredentialsProviderService, CredentialPropertyDescriptors.SECRET_KEY, "test-secret");
        } else {
            mockConsumeKinesisStreamRunner.setProperty(awsCredentialsProviderService, CredentialPropertyDescriptors.USE_ANONYMOUS_CREDENTIALS, "true");
        }
        mockConsumeKinesisStreamRunner.assertValid(awsCredentialsProviderService);
        mockConsumeKinesisStreamRunner.enableControllerService(awsCredentialsProviderService);
        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE, "aws-credentials");

        // speed up init process for the unit test (and show use of dynamic properties to configure KCL)
        mockConsumeKinesisStreamRunner.setProperty("parentShardPollIntervalMillis", "1");

        mockConsumeKinesisStreamRunner.assertValid();

        // start the processor (but don't auto-shutdown to give Worker initialisation a chance to progress)
        mockConsumeKinesisStreamRunner.run(1, false);
        final MockConsumeKinesisStream processor = ((MockConsumeKinesisStream) mockConsumeKinesisStreamRunner.getProcessor());

        // WorkerState should get to INITIALIZING pretty quickly, but there's a change it will still be at CREATED by the time we get here
        assertThat(processor.workerState.get(), anyOf(equalTo(WorkerStateChangeListener.WorkerState.INITIALIZING), equalTo(WorkerStateChangeListener.WorkerState.CREATED)));

        final String hostname = InetAddress.getLocalHost().getCanonicalHostName();

        assertKinesisClientLibConfiguration(processor.kinesisClientLibConfiguration, withCredentials, hostname);
        assertThat(processor.workerBuilder.build().getApplicationName(), equalTo("test-application"));

        // confirm the Kinesis Worker initialisation was attempted
        assertThat(mockConsumeKinesisStreamRunner.getLogger().getInfoMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().contains(String.format(
                        "Kinesis Worker prepared for application %s to process stream %s as worker ID %s:",
                        "test-application", "test-stream", hostname
                ))), is(true));

        // confirm the processor worked through the onTrigger method (and no execution of stopConsuming method)
        assertThat(mockConsumeKinesisStreamRunner.getLogger().getInfoMessages().stream()
                .anyMatch(logMessage -> logMessage.getMsg().contains(String.format("Starting Kinesis Worker %s", hostname))), is(true));
        assertThat(mockConsumeKinesisStreamRunner.getLogger().getInfoMessages().stream()
                .noneMatch(logMessage -> logMessage.getMsg().endsWith("Requesting Kinesis Worker shutdown")), is(true));
        assertThat(mockConsumeKinesisStreamRunner.getLogger().getInfoMessages().stream()
                .noneMatch(logMessage -> logMessage.getMsg().endsWith("Kinesis Worker shutdown")), is(true));
        assertThat(mockConsumeKinesisStreamRunner.getLogger().getWarnMessages().isEmpty(), is(true));
        assertThat(mockConsumeKinesisStreamRunner.getLogger().getErrorMessages().isEmpty(), is(true));

        if (!waitForFailure) {
            // re-trigger the processor to ensure the Worker isn't re-initialised when already running
            mockConsumeKinesisStreamRunner.run(1, false, false);
            assertTrue(((MockProcessContext) mockConsumeKinesisStreamRunner.getProcessContext()).isYieldCalled());

            // "Starting" log count remains at 1 from the initial startup above (the Logger doesn't get reset between processor calls)
            assertThat(mockConsumeKinesisStreamRunner.getLogger().getInfoMessages().stream()
                    .filter(logMessage -> logMessage.getMsg().contains(String.format("Starting Kinesis Worker %s", hostname))).count(), is(1L));
            assertThat(mockConsumeKinesisStreamRunner.getLogger().getWarnMessages().isEmpty(), is(true));
            assertThat(mockConsumeKinesisStreamRunner.getLogger().getErrorMessages().isEmpty(), is(true));

            // stop the processor
            mockConsumeKinesisStreamRunner.stop();

            // confirm the processor worked through the stopConsuming method
            assertThat(mockConsumeKinesisStreamRunner.getLogger().getInfoMessages().stream()
                    .anyMatch(logMessage -> logMessage.getMsg().endsWith("Requesting Kinesis Worker shutdown")), is(true));
            assertThat(mockConsumeKinesisStreamRunner.getLogger().getInfoMessages().stream()
                    .anyMatch(logMessage -> logMessage.getMsg().endsWith("Kinesis Worker shutdown")), is(true));

            // LeaseCoordinator doesn't startup properly (can't create DynamoDB table during unit test) and therefore has a problem during shutdown
            assertThat(mockConsumeKinesisStreamRunner.getLogger().getWarnMessages().size(), is(2));
            assertThat(mockConsumeKinesisStreamRunner.getLogger().getWarnMessages().stream()
                    .anyMatch(logMessage -> logMessage.getMsg().endsWith(
                            "Problem while shutting down Kinesis Worker: java.lang.NullPointerException: java.util.concurrent.ExecutionException: java.lang.NullPointerException"
                    )), is(true));
            assertThat(mockConsumeKinesisStreamRunner.getLogger().getWarnMessages().stream()
                    .anyMatch(logMessage -> logMessage.getMsg().endsWith("One or more problems while shutting down Kinesis Worker, see logs for details")), is(true));
            assertThat(mockConsumeKinesisStreamRunner.getLogger().getErrorMessages().isEmpty(), is(true));
        } else {
            for (int runs = 0; runs < 10; runs++) {
                try {
                    mockConsumeKinesisStreamRunner.run(1, false, false);
                    Thread.sleep(1_000);
                } catch (AssertionError e) {
                    assertThat(e.getCause(), instanceOf(ProcessException.class));
                    assertThat(e.getCause().getMessage(), equalTo("Worker has shutdown unexpectedly, possibly due to a configuration issue; check logs for details"));
                    assertTrue(((MockProcessContext) mockConsumeKinesisStreamRunner.getProcessContext()).isYieldCalled());
                    break;
                }
            }
        }
    }

    private void assertKinesisClientLibConfiguration(final KinesisClientLibConfiguration kinesisClientLibConfiguration,
                                                     final boolean withCredentials, final String hostname) {
        assertThat(kinesisClientLibConfiguration.getWorkerIdentifier(), startsWith(hostname));
        assertThat(kinesisClientLibConfiguration.getApplicationName(), equalTo("test-application"));
        assertThat(kinesisClientLibConfiguration.getStreamName(), equalTo("test-stream"));

        if (withCredentials) {
            assertThat(kinesisClientLibConfiguration.getKinesisCredentialsProvider().getCredentials().getAWSAccessKeyId(), equalTo("test-access"));
            assertThat(kinesisClientLibConfiguration.getKinesisCredentialsProvider().getCredentials().getAWSSecretKey(), equalTo("test-secret"));
            assertThat(kinesisClientLibConfiguration.getDynamoDBCredentialsProvider().getCredentials().getAWSAccessKeyId(), equalTo("test-access"));
            assertThat(kinesisClientLibConfiguration.getDynamoDBCredentialsProvider().getCredentials().getAWSSecretKey(), equalTo("test-secret"));
            assertThat(kinesisClientLibConfiguration.getCloudWatchCredentialsProvider().getCredentials().getAWSAccessKeyId(), equalTo("test-access"));
            assertThat(kinesisClientLibConfiguration.getCloudWatchCredentialsProvider().getCredentials().getAWSSecretKey(), equalTo("test-secret"));
        } else {
            assertThat(kinesisClientLibConfiguration.getKinesisCredentialsProvider().getCredentials().getAWSAccessKeyId(), nullValue());
            assertThat(kinesisClientLibConfiguration.getKinesisCredentialsProvider().getCredentials().getAWSSecretKey(), nullValue());
            assertThat(kinesisClientLibConfiguration.getDynamoDBCredentialsProvider().getCredentials().getAWSAccessKeyId(), nullValue());
            assertThat(kinesisClientLibConfiguration.getDynamoDBCredentialsProvider().getCredentials().getAWSSecretKey(), nullValue());
            assertThat(kinesisClientLibConfiguration.getCloudWatchCredentialsProvider().getCredentials().getAWSAccessKeyId(), nullValue());
            assertThat(kinesisClientLibConfiguration.getCloudWatchCredentialsProvider().getCredentials().getAWSSecretKey(), nullValue());
        }

        assertThat(kinesisClientLibConfiguration.getRegionName(), equalTo(Regions.EU_WEST_2.getName()));
        assertThat(kinesisClientLibConfiguration.getInitialPositionInStream(), equalTo(InitialPositionInStream.LATEST));
        assertThat(kinesisClientLibConfiguration.getDynamoDBEndpoint(), nullValue());
        assertThat(kinesisClientLibConfiguration.getKinesisEndpoint(), nullValue());

        assertThat(kinesisClientLibConfiguration.getKinesisClientConfiguration(), instanceOf(ClientConfiguration.class));
        assertThat(kinesisClientLibConfiguration.getDynamoDBClientConfiguration(), instanceOf(ClientConfiguration.class));
        assertThat(kinesisClientLibConfiguration.getCloudWatchClientConfiguration(), instanceOf(ClientConfiguration.class));

        assertThat(kinesisClientLibConfiguration.getParentShardPollIntervalMillis(), equalTo(1L));
    }

    // public so TestRunners is able to see and instantiate the class for the tests
    public static class MockConsumeKinesisStream extends ConsumeKinesisStream {
        // capture the WorkerBuilder and KinesisClientLibConfiguration for unit test assertions
        KinesisClientLibConfiguration kinesisClientLibConfiguration;
        Worker.Builder workerBuilder;

        @Override
        Worker.Builder prepareWorkerBuilder(final ProcessContext context, final KinesisClientLibConfiguration kinesisClientLibConfiguration,
                                            final IRecordProcessorFactory factory) {
            workerBuilder = super.prepareWorkerBuilder(context, kinesisClientLibConfiguration, factory);
            return workerBuilder;
        }

        @Override
        KinesisClientLibConfiguration prepareKinesisClientLibConfiguration(final ProcessContext context, final String workerId) {
            kinesisClientLibConfiguration = super.prepareKinesisClientLibConfiguration(context, workerId);
            return kinesisClientLibConfiguration;
        }
    }
}
