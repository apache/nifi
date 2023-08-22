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
package org.apache.nifi.processors.gcp.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.UnavailableException;
import com.google.cloud.pubsub.v1.Publisher;
import io.grpc.Status;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.pubsub.publish.MessageDerivationStrategy;
import org.apache.nifi.processors.gcp.pubsub.publish.TrackedApiFutureCallback;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PublishGCPubSubTest {

    private static final String TOPIC = "my-topic";
    private static final String PROJECT = "my-project";

    private Throwable throwable;
    private Publisher publisherMock;
    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        throwable = null;
        publisherMock = mock(Publisher.class);
        runner = TestRunners.newTestRunner(new PublishGCPubSub() {
            @Override
            @OnScheduled
            public void onScheduled(ProcessContext context) {
                publisher = publisherMock;
            }

            @Override
            protected void addCallback(ApiFuture<String> apiFuture, ApiFutureCallback<? super String> callback, Executor executor) {
                if (callback instanceof TrackedApiFutureCallback) {
                    final TrackedApiFutureCallback apiFutureCallback = (TrackedApiFutureCallback) callback;
                    if (throwable == null) {
                        apiFutureCallback.onSuccess(Long.toString(System.currentTimeMillis()));
                    } else {
                        apiFutureCallback.onFailure(throwable);
                    }
                }
            }
        });
    }

    @Test
    void testPropertyDescriptors() throws InitializationException {
        runner.assertNotValid();

        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, getCredentialsServiceId(runner));
        runner.assertNotValid();

        runner.setProperty(PublishGCPubSub.TOPIC_NAME, TOPIC);
        runner.assertNotValid();

        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT);
        runner.assertValid();

        runner.setProperty(PublishGCPubSub.API_ENDPOINT, "localhost");
        runner.assertNotValid();
        runner.setProperty(PublishGCPubSub.API_ENDPOINT, "localhost:443");
        runner.assertValid();

        runner.setProperty(PublishGCPubSub.BATCH_SIZE_THRESHOLD, "-1");
        runner.assertNotValid();
        runner.setProperty(PublishGCPubSub.BATCH_SIZE_THRESHOLD, "15");
        runner.assertValid();

        runner.setProperty(PublishGCPubSub.BATCH_BYTES_THRESHOLD, "3");
        runner.assertNotValid();
        runner.setProperty(PublishGCPubSub.BATCH_BYTES_THRESHOLD, "3 MB");
        runner.assertValid();

        runner.setProperty(PublishGCPubSub.BATCH_DELAY_THRESHOLD, "100");
        runner.assertNotValid();
        runner.setProperty(PublishGCPubSub.BATCH_DELAY_THRESHOLD, "100 millis");
        runner.assertValid();
    }

    @Test
    void testSendOneSuccessFlowFileStrategy() throws InitializationException {
        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, getCredentialsServiceId(runner));
        runner.setProperty(PublishGCPubSub.TOPIC_NAME, TOPIC);
        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT);

        runner.enqueue("text");
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PublishGCPubSub.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PublishGCPubSub.REL_SUCCESS).iterator().next();
        assertNotNull(flowFile.getAttribute(PubSubAttributes.MESSAGE_ID_ATTRIBUTE));
    }

    @Test
    void testSendOneRetryFlowFileStrategy() throws InitializationException {
        throwable = new UnavailableException(null, GrpcStatusCode.of(Status.Code.UNAVAILABLE), true);

        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, getCredentialsServiceId(runner));
        runner.setProperty(PublishGCPubSub.TOPIC_NAME, TOPIC);
        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT);

        runner.enqueue("text");
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PublishGCPubSub.REL_RETRY, 1);
    }


    @Test
    void testSendOneFailureFlowFileStrategy() throws InitializationException {
        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, getCredentialsServiceId(runner));
        runner.setProperty(PublishGCPubSub.TOPIC_NAME, TOPIC);
        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT);
        runner.setProperty(PublishGCPubSub.MAX_MESSAGE_SIZE, "16 B");
        runner.enqueue("some really long text");

        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PublishGCPubSub.REL_FAILURE, 1);
    }

    @Test
    void testSendOneSuccessRecordStrategyAvroReader() throws InitializationException, IOException {
        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, getCredentialsServiceId(runner));
        runner.setProperty(PublishGCPubSub.TOPIC_NAME, TOPIC);
        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT);
        runner.setProperty(PublishGCPubSub.RECORD_READER, getReaderServiceId(runner, new AvroReader()));
        runner.setProperty(PublishGCPubSub.RECORD_WRITER, getWriterServiceId(runner));
        runner.setProperty(PublishGCPubSub.MESSAGE_DERIVATION_STRATEGY, MessageDerivationStrategy.RECORD_ORIENTED.getValue());

        runner.enqueue(IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource("pubsub/records.avro"))));
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PublishGCPubSub.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PublishGCPubSub.REL_SUCCESS).iterator().next();
        assertEquals("3", flowFile.getAttribute(PubSubAttributes.RECORDS_ATTRIBUTE));
    }

    @Test
    void testSendOneSuccessRecordStrategy() throws InitializationException, IOException {
        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, getCredentialsServiceId(runner));
        runner.setProperty(PublishGCPubSub.TOPIC_NAME, TOPIC);
        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT);
        runner.setProperty(PublishGCPubSub.RECORD_READER, getReaderServiceId(runner, new JsonTreeReader()));
        runner.setProperty(PublishGCPubSub.RECORD_WRITER, getWriterServiceId(runner));
        runner.setProperty(PublishGCPubSub.MESSAGE_DERIVATION_STRATEGY, MessageDerivationStrategy.RECORD_ORIENTED.getValue());

        runner.enqueue(IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource("pubsub/records.json"))));
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PublishGCPubSub.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PublishGCPubSub.REL_SUCCESS).iterator().next();
        assertEquals("3", flowFile.getAttribute(PubSubAttributes.RECORDS_ATTRIBUTE));
    }

    @Test
    void testSendOneRetryRecordStrategy() throws InitializationException, IOException {
        throwable = new UnavailableException(null, GrpcStatusCode.of(Status.Code.UNAVAILABLE), true);

        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, getCredentialsServiceId(runner));
        runner.setProperty(PublishGCPubSub.TOPIC_NAME, TOPIC);
        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT);
        runner.setProperty(PublishGCPubSub.RECORD_READER, getReaderServiceId(runner, new JsonTreeReader()));
        runner.setProperty(PublishGCPubSub.RECORD_WRITER, getWriterServiceId(runner));
        runner.setProperty(PublishGCPubSub.MESSAGE_DERIVATION_STRATEGY, MessageDerivationStrategy.RECORD_ORIENTED.getValue());

        runner.enqueue(IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource("pubsub/records.json"))));
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PublishGCPubSub.REL_RETRY, 1);
    }

    @Test
    void testSendOneFailureRecordStrategy() throws InitializationException, IOException {
        throwable = new IllegalStateException("testSendOne_Failure_RecordStrategy");

        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, getCredentialsServiceId(runner));
        runner.setProperty(PublishGCPubSub.TOPIC_NAME, TOPIC);
        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT);
        runner.setProperty(PublishGCPubSub.RECORD_READER, getReaderServiceId(runner, new JsonTreeReader()));
        runner.setProperty(PublishGCPubSub.RECORD_WRITER, getWriterServiceId(runner));
        runner.setProperty(PublishGCPubSub.MESSAGE_DERIVATION_STRATEGY, MessageDerivationStrategy.RECORD_ORIENTED.getValue());

        runner.enqueue(IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource("pubsub/records.json"))));
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PublishGCPubSub.REL_FAILURE, 1);
    }

    private static String getCredentialsServiceId(final TestRunner runner) throws InitializationException {
        final ControllerService controllerService = mock(GCPCredentialsControllerService.class);
        final String controllerServiceId = GCPCredentialsControllerService.class.getSimpleName();
        when(controllerService.getIdentifier()).thenReturn(controllerServiceId);
        runner.addControllerService(controllerServiceId, controllerService);
        runner.enableControllerService(controllerService);
        return controllerServiceId;
    }

    private static String getReaderServiceId(
            final TestRunner runner, final RecordReaderFactory recordReaderFactory) throws InitializationException {
        final String readerServiceId = recordReaderFactory.getClass().getName();
        runner.addControllerService(readerServiceId, recordReaderFactory);
        runner.enableControllerService(recordReaderFactory);
        return readerServiceId;
    }

    private static String getWriterServiceId(TestRunner runner) throws InitializationException {
        final ControllerService writerService = new JsonRecordSetWriter();
        final String writerServiceId = writerService.getClass().getName();
        runner.addControllerService(writerServiceId, writerService);
        runner.enableControllerService(writerService);
        return writerServiceId;
    }
}
