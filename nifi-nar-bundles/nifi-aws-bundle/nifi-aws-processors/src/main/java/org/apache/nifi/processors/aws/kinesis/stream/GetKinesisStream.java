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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Tags({"amazon", "aws", "kinesis", "get", "stream"})
@CapabilityDescription("Reads data from the specified AWS Kinesis stream. " +
        "Uses DynamoDB for check pointing and CloudWatch (optional) for metrics. " +
        "Ensure that the credentials provided have access to DynamoDB and CloudWatch (if used) along with Kinesis.")
@SeeAlso(PutKinesisStream.class)
public class GetKinesisStream extends AbstractKinesisStreamProcessor {

    public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Application Name")
            .name("amazon-kinesis-stream-application-name")
            .description("The stream reader application name.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true).build();

    public static final PropertyDescriptor INITIAL_STREAM_POSITION = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Initial Stream Position")
            .name("amazon-kinesis-stream-initial-position")
            .description("Initial position to read streams.")
            .allowableValues(InitialPositionInStream.LATEST.toString(),
                    InitialPositionInStream.TRIM_HORIZON.toString())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(InitialPositionInStream.LATEST.toString())
            .required(true).build();

    public static final PropertyDescriptor DYNAMODB_ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis DynamoDB Override")
            .name("amazon-kinesis-stream-dynamodb-override")
            .description("DynamoDB override to use non AWS deployments")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false).build();

    public static final PropertyDescriptor DISABLE_CLOUDWATCH = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Cloudwatch Flag")
            .name("amazon-kinesis-stream-cloudwatch-flag")
            .description("Disable kinesis logging to cloud watch.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .required(false).build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KINESIS_STREAM_NAME, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE,
                    AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, ENDPOINT_OVERRIDE,
                    PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD,
                    APPLICATION_NAME, INITIAL_STREAM_POSITION, DYNAMODB_ENDPOINT_OVERRIDE, DISABLE_CLOUDWATCH));

    private Worker worker;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            if (worker == null) {
                AWSCredentials awsCredentials = getCredentials(context);
                ClientConfiguration config = createConfiguration(context);
                this.worker = initializeWorker(awsCredentials, config, getLogger(), context, session);
                this.worker.run();
            }
        } catch (Throwable t) {
            getLogger().error("Error while processing data.", t);
        }
    }

    @OnShutdown
    public void onShutDown() {
        try {
            Future<Boolean> shutdownCallback = worker.startGracefulShutdown();

            waitForShutdownToComplete(shutdownCallback);

            if (shutdownCallback.get()) {
                getLogger().info("Successfully shutdown KCL");
            } else {
                getLogger().error("Some error occurred during shutdown of KCL. Logs may have more details.");
            }
        } catch (InterruptedException e) {
            getLogger().error("Shutdown interrupted", e);
        } catch (ExecutionException e) {
            getLogger().error("Error during KCL processing", e);
        }
    }

    @Override
    protected AmazonKinesisClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider,
                                               ClientConfiguration config) {
        // The Amazon Kinesis Client is not suitable for reading from kinesis.
        // We will be using Amazon Kinesis Client library. Therefore returning null.
        return null;
    }

    private void waitForShutdownToComplete(Future<Boolean> shutdownCallback) throws InterruptedException {
        while (!shutdownCallback.isDone()) {
            Thread.sleep(3_000);
        }
    }

    private static Worker initializeWorker(AWSCredentials awsCredentials, ClientConfiguration config,
                                           ComponentLog log, ProcessContext context, ProcessSession session) {
        String appName = getApplicationName(context);
        String streamName = getStreamName(context);
        String workerId = generateWorkerId();

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(appName, streamName, credentialsProvider, workerId)
                        .withCommonClientConfig(config);

        kinesisClientLibConfiguration.withInitialPositionInStream(getInitialPositionInStream(context));
        getDynamoDBOverride(context).ifPresent(kinesisClientLibConfiguration::withDynamoDBEndpoint);
        getKinesisOverride(context).ifPresent(kinesisClientLibConfiguration::withKinesisEndpoint);

        RecordProcessorFactory factory = new RecordProcessorFactory(session, log);

        Worker.Builder workerBuilder = new Worker.Builder()
                .config(kinesisClientLibConfiguration)
                .recordProcessorFactory(factory);

        if (isCloudWatchDisabled(context)) {
            workerBuilder.metricsFactory(new NullMetricsFactory());
        }

        log.info(String.format("Running %s to process stream %s as worker %s...\n", appName, streamName, workerId));

        return workerBuilder.build();
    }

    private static String generateWorkerId() {
        String workerId;
        try {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            throw new ProcessException(e);
        }
        return workerId;
    }

    private static String getApplicationName(ProcessContext context) {
        return StringUtils.trimToEmpty(context.getProperty(APPLICATION_NAME).getValue());
    }

    private static String getStreamName(ProcessContext context) {
        return StringUtils.trimToEmpty(context.getProperty(KINESIS_STREAM_NAME)
                .evaluateAttributeExpressions().getValue());
    }

    private static InitialPositionInStream getInitialPositionInStream(ProcessContext context) {
        String initialPositionStr =
                StringUtils.trimToEmpty(context.getProperty(INITIAL_STREAM_POSITION).getValue());

        return InitialPositionInStream.valueOf(initialPositionStr);
    }

    private static boolean isCloudWatchDisabled(ProcessContext context) {
        return context.getProperty(DISABLE_CLOUDWATCH).asBoolean();
    }

    private static Optional<String> getKinesisOverride(ProcessContext context) {
        return context.getProperty(ENDPOINT_OVERRIDE).isSet()
                ? Optional.of(StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE)
                .evaluateAttributeExpressions().getValue())) : Optional.empty();
    }

    private static Optional<String> getDynamoDBOverride(ProcessContext context) {
        return context.getProperty(DYNAMODB_ENDPOINT_OVERRIDE).isSet()
                ? Optional.of(StringUtils.trimToEmpty(context.getProperty(DYNAMODB_ENDPOINT_OVERRIDE)
                .evaluateAttributeExpressions().getValue())) : Optional.empty();
    }
}
