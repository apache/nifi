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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.AbstractAwsProcessor;
import org.apache.nifi.processors.aws.ObsoleteAbstractAwsProcessorProperties;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.coordinator.Scheduler;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConsumeKinesisStream {
    private final TestRunner runner = TestRunners.newTestRunner(ConsumeKinesisStream.class);

    @BeforeEach
    public void setUp() throws InitializationException {
        runner.setProperty(ConsumeKinesisStream.KINESIS_STREAM_NAME, "test-stream");
        runner.setProperty(ConsumeKinesisStream.APPLICATION_NAME, "test-application");

        // use anonymous credentials by default
        final ControllerService credentialsProvider = new AWSCredentialsProviderControllerService();
        runner.addControllerService("credentials-provider", credentialsProvider);
        runner.setProperty(credentialsProvider, AWSCredentialsProviderControllerService.USE_ANONYMOUS_CREDENTIALS, "true");
        runner.assertValid(credentialsProvider);
        runner.enableControllerService(credentialsProvider);
        runner.setProperty(ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials-provider");

        runner.assertValid();
    }


    @Test
    public void testMissingMandatoryProperties() {
        runner.removeProperty(ConsumeKinesisStream.KINESIS_STREAM_NAME);
        runner.removeProperty(ConsumeKinesisStream.APPLICATION_NAME);
        runner.removeProperty(ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE);
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertEquals(assertionError.getMessage(), String.format("""
                        Processor has 3 validation failures:
                        '%s' is invalid because %s is required
                        '%s' is invalid because %s is required
                        '%s' is invalid because %s is required
                        """,
                ConsumeKinesisStream.KINESIS_STREAM_NAME.getDisplayName(), ConsumeKinesisStream.KINESIS_STREAM_NAME.getDisplayName(),
                ConsumeKinesisStream.APPLICATION_NAME.getDisplayName(), ConsumeKinesisStream.APPLICATION_NAME.getDisplayName(),
                ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE.getDisplayName(), ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE.getDisplayName()
        ));
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
        assertEquals(assertionError.getMessage(), String.format("""
                        Processor has 14 validation failures:
                        '%s' validated against ' ' is invalid because %s must contain at least one character that is not white space
                        '%s' validated against 'not-a-reader' is invalid because Property references a Controller Service that does not exist
                        '%s' validated against 'not-a-writer' is invalid because Property references a Controller Service that does not exist
                        '%s' validated against 'not-a-url' is invalid because Not a valid URL
                        '%s' validated against 'not-an-enum-match' is invalid because Given value not found in allowed set '%s, %s, %s'
                        '%s' validated against 'not-valid-format' is invalid because Must be a valid java.time.DateTimeFormatter pattern, e.g. %s
                        '%s' validated against 'not-a-period' is invalid because Must be of format <duration> <TimeUnit> where <duration> is a non-negative integer and \
                        TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days
                        '%s' validated against 'not-a-period' is invalid because Must be of format <duration> <TimeUnit> where <duration> is a non-negative integer and \
                        TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days
                        '%s' validated against 'not-a-long' is invalid because Must be of format <duration> <TimeUnit> where <duration> is a non-negative integer and \
                        TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days
                        '%s' validated against 'not-an-int' is invalid because not a valid integer
                        '%s' validated against 'not-a-long' is invalid because Must be of format <duration> <TimeUnit> where <duration> is a non-negative integer and \
                        TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days
                        '%s' validated against 'not-a-boolean' is invalid because Given value not found in allowed set 'true, false'
                        '%s' validated against 'not-a-reader' is invalid because Invalid Controller Service: not-a-reader is not a valid Controller Service Identifier
                        '%s' validated against 'not-a-writer' is invalid because Invalid Controller Service: not-a-writer is not a valid Controller Service Identifier
                        """,
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
        ));
    }

    @Test
    public void testMissingStreamPositionTimestamp() {
        runner.setProperty(ConsumeKinesisStream.INITIAL_STREAM_POSITION, InitialPositionInStream.AT_TIMESTAMP.toString());
        runner.removeProperty(ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP);
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertEquals(assertionError.getMessage(), String.format("""
                        Processor has 1 validation failures:
                        '%s' is invalid because %s must be provided when %s is %s
                        """,
                ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP.getName(), ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP.getDisplayName(),
                ConsumeKinesisStream.INITIAL_STREAM_POSITION.getDisplayName(), InitialPositionInStream.AT_TIMESTAMP
        ));
    }

    @Test
    public void testInvalidStreamPositionTimestamp() {
        runner.setProperty(ConsumeKinesisStream.INITIAL_STREAM_POSITION, InitialPositionInStream.AT_TIMESTAMP.toString());
        runner.setProperty(ConsumeKinesisStream.TIMESTAMP_FORMAT, "yyyy-MM-dd");
        runner.setProperty(ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP, "12:00:00");
        runner.assertNotValid();

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertEquals(assertionError.getMessage(), String.format("""
                        Processor has 1 validation failures:
                        '%s' is invalid because %s must be parsable by %s
                        """,
                ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP.getName(),
                ConsumeKinesisStream.STREAM_POSITION_TIMESTAMP.getDisplayName(),
                ConsumeKinesisStream.TIMESTAMP_FORMAT.getDisplayName()
        ));
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
        assertEquals(assertionError.getMessage(), String.format("""
                        Processor has 1 validation failures:
                        '%s' is invalid because %s must be set if %s is set in order to write FlowFiles as Records.
                        """,
                ConsumeKinesisStream.RECORD_WRITER.getName(),
                ConsumeKinesisStream.RECORD_WRITER.getDisplayName(),
                ConsumeKinesisStream.RECORD_READER.getDisplayName()
        ));
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
        assertEquals(assertionError.getMessage(), String.format("""
                        Processor has 1 validation failures:
                        '%s' is invalid because %s must be set if %s is set in order to write FlowFiles as Records.
                        """,
                ConsumeKinesisStream.RECORD_READER.getName(),
                ConsumeKinesisStream.RECORD_READER.getDisplayName(),
                ConsumeKinesisStream.RECORD_WRITER.getDisplayName()
        ));
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
        runner.setProperty("leaseManagementConfig.failoverTimeMillis", "1000");
        runner.setProperty("leaseManagementConfig.initialPositionInStream", "AT_TIMESTAMP");

        // invalid parameter conversions
        runner.setProperty("checkpointConfig.checkpointFactory", "too-complex");
        runner.setProperty("coordinatorConfig.schedulerInitializationBackoffTimeMillis", "not-long");

        // valid dynamic parameters
        runner.setProperty("namespace", "value");

        assertThrows(AssertionError.class, runner::assertValid);
    }

    @Test
    public void testValidDynamicKCLProperties() {
        runner.setProperty("leaseManagementConfig.billingMode", "PROVISIONED"); // enum
        runner.setProperty("leaseManagementConfig.leasesRecoveryAuditorExecutionFrequencyMillis", "1000"); // long
        runner.setProperty("leaseManagementConfig.cleanupLeasesUponShardCompletion", "true"); // boolean
        runner.setProperty("leaseManagementConfig.initialLeaseTableReadCapacity", "1"); // int
        runner.setProperty("leaseManagementConfig.MaxCacheMissesBeforeReload", "2"); // String with uppercase leading character in property name

        runner.assertValid();
    }

    @Test
    void testMigration() {
        final Map<String, String> expected = Map.ofEntries(
                Map.entry("aws-region", REGION.getName()),
                Map.entry(AbstractAwsProcessor.OBSOLETE_AWS_CREDENTIALS_PROVIDER_SERVICE_PROPERTY_NAME, AbstractAwsProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, AbstractAwsProcessor.PROXY_CONFIGURATION_SERVICE.getName()),
                Map.entry("kinesis-stream-name", "Amazon Kinesis Stream Name"),
                Map.entry("amazon-kinesis-stream-application-name", "Application Name"),
                Map.entry("amazon-kinesis-stream-initial-position", "Initial Stream Position"),
                Map.entry("amazon-kinesis-stream-position-timestamp", "Stream Position Timestamp"),
                Map.entry("amazon-kinesis-stream-timestamp-format", "Timestamp Format"),
                Map.entry("amazon-kinesis-stream-failover-timeout", "Failover Timeout"),
                Map.entry("amazon-kinesis-stream-graceful-shutdown-timeout", "Graceful Shutdown Timeout"),
                Map.entry("amazon-kinesis-stream-checkpoint-interval", "Checkpoint Interval"),
                Map.entry("amazon-kinesis-stream-retry-count", "Retry Count"),
                Map.entry("amazon-kinesis-stream-retry-wait", "Retry Wait"),
                Map.entry("amazon-kinesis-stream-dynamodb-override", "DynamoDB Override"),
                Map.entry("amazon-kinesis-stream-cloudwatch-flag", "Report Metrics to CloudWatch"),
                Map.entry("amazon-kinesis-stream-record-reader", "Record Reader"),
                Map.entry("amazon-kinesis-stream-record-writer", "Record Writer")
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());

        final Set<String> expectedRemoved = Set.of(
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_ACCESS_KEY.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_SECRET_KEY.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_CREDENTIALS_FILE.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_HOST.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_PORT.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_USERNAME.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_PASSWORD.getValue()
        );

        assertEquals(expectedRemoved,
                propertyMigrationResult.getPropertiesRemoved());
    }

    /*
     * Trigger a run of the ConsumeKinesisStream processor, but expect the KCL Worker to fail (it needs connections to AWS resources)
     * Assert that our code is being called by checking log output. The ITConsumeKinesisStream integration tests prove actual AWS connectivity
     */
    private void runWorker(final boolean withCredentials, final boolean waitForFailure) throws UnknownHostException, InitializationException, InterruptedException {
        final TestRunner mockConsumeKinesisStreamRunner = TestRunners.newTestRunner(MockConsumeKinesisStream.class);

        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.GRACEFUL_SHUTDOWN_TIMEOUT, "50 millis");
        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.KINESIS_STREAM_NAME, "test-stream");
        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.APPLICATION_NAME, "test-application");
        mockConsumeKinesisStreamRunner.setProperty(RegionUtil.REGION, Region.EU_WEST_2.id());
        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.TIMEOUT, "5 secs");
        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.INITIAL_STREAM_POSITION, "TRIM_HORIZON");

        final AwsCredentialsProviderService awsCredentialsProviderService = new AWSCredentialsProviderControllerService();
        mockConsumeKinesisStreamRunner.addControllerService("aws-credentials", awsCredentialsProviderService);
        if (withCredentials) {
            mockConsumeKinesisStreamRunner.setProperty(awsCredentialsProviderService, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, "test-access");
            mockConsumeKinesisStreamRunner.setProperty(awsCredentialsProviderService, AWSCredentialsProviderControllerService.SECRET_KEY, "test-secret");
        } else {
            mockConsumeKinesisStreamRunner.setProperty(awsCredentialsProviderService, AWSCredentialsProviderControllerService.USE_ANONYMOUS_CREDENTIALS, "true");
        }
        mockConsumeKinesisStreamRunner.assertValid(awsCredentialsProviderService);
        mockConsumeKinesisStreamRunner.enableControllerService(awsCredentialsProviderService);
        mockConsumeKinesisStreamRunner.setProperty(ConsumeKinesisStream.AWS_CREDENTIALS_PROVIDER_SERVICE, "aws-credentials");

        // speed up init process for the unit test (and show use of dynamic properties to configure KCL)
        mockConsumeKinesisStreamRunner.setProperty("coordinatorConfig.parentShardPollIntervalMillis", "1");

        mockConsumeKinesisStreamRunner.assertValid();

        // start the processor (but don't auto-shutdown to give Worker initialisation a chance to progress)
        mockConsumeKinesisStreamRunner.run(1, false);
        final MockConsumeKinesisStream processor = ((MockConsumeKinesisStream) mockConsumeKinesisStreamRunner.getProcessor());

        Thread.sleep(50);

        final String hostname = InetAddress.getLocalHost().getCanonicalHostName();

        assertSchedulerConfigs(processor.scheduler, hostname);
        assertConfigsBuilder(processor.configsBuilder);
        assertEquals(processor.scheduler.applicationName(), "test-application");

        if (!waitForFailure) {
            // re-trigger the processor to ensure the Worker isn't re-initialised when already running
            mockConsumeKinesisStreamRunner.run(1, true, false);
            assertTrue(((MockProcessContext) mockConsumeKinesisStreamRunner.getProcessContext()).isYieldCalled());
        } else {
            for (int runs = 0; runs < 10; runs++) {
                try {
                    mockConsumeKinesisStreamRunner.run(1, false, false);
                } catch (AssertionError e) {
                    assertInstanceOf(ProcessException.class, e.getCause());
                    assertTrue(((MockProcessContext) mockConsumeKinesisStreamRunner.getProcessContext()).isYieldCalled());
                    break;
                }
            }
        }
    }

    private void assertConfigsBuilder(final ConfigsBuilder configsBuilder) {
        try (KinesisAsyncClient kinesisAsyncClient = configsBuilder.kinesisClient();
             DynamoDbAsyncClient dynamoDbAsyncClient = configsBuilder.dynamoDBClient()) {
            assertEquals(kinesisAsyncClient.serviceClientConfiguration().region().id(), Region.EU_WEST_2.id());
            assertTrue(dynamoDbAsyncClient.serviceClientConfiguration().endpointOverride().isEmpty());
            assertTrue(kinesisAsyncClient.serviceClientConfiguration().endpointOverride().isEmpty());
        }
    }

    private void assertSchedulerConfigs(final Scheduler scheduler, final String hostname) {
        assertTrue(scheduler.leaseManagementConfig().workerIdentifier().startsWith(hostname));
        assertEquals(scheduler.coordinatorConfig().applicationName(), "test-application");
        assertEquals(scheduler.leaseManagementConfig().streamName(), "test-stream");
        assertEquals(scheduler.retrievalConfig().streamTracker().streamConfigList().getFirst().initialPositionInStreamExtended().getInitialPositionInStream(),
                InitialPositionInStream.TRIM_HORIZON);
        assertEquals(scheduler.coordinatorConfig().parentShardPollIntervalMillis(), 1);
    }

    // public so TestRunners is able to see and instantiate the class for the tests
    public static class MockConsumeKinesisStream extends ConsumeKinesisStream {
        // capture the Scheduler and ConfigsBuilder for unit test assertions
        ConfigsBuilder configsBuilder;
        Scheduler scheduler;

        @Override
        synchronized Scheduler prepareScheduler(final ProcessContext context, final ProcessSessionFactory sessionFactory, final String schedulerId) {
            scheduler = super.prepareScheduler(context, sessionFactory, schedulerId);
            return scheduler;
        }

        @Override
        ConfigsBuilder prepareConfigsBuilder(final ProcessContext context, final String workerId, final ProcessSessionFactory sessionFactory) {
            configsBuilder = super.prepareConfigsBuilder(context, workerId, sessionFactory);
            return configsBuilder;
        }
    }
}
