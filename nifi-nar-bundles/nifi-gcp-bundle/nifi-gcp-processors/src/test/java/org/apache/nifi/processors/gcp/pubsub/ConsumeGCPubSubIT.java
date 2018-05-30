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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.ACK_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_ATTRIBUTES_COUNT_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_PUBLISH_TIME_ATTRIBUTE;

public class ConsumeGCPubSubIT extends AbstractGCPubSubIT{

    @BeforeClass
    public static void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeGCPubSub.class);
    }

    @Test
    public void testSimpleConsume() throws InitializationException {
        final String subscription = "my-sub";
        runner.clearTransferState();

        runner = setCredentialsCS(runner);

        runner.setProperty(ConsumeGCPubSub.PROJECT_ID, PROJECT_ID);
        runner.setProperty(ConsumeGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(ConsumeGCPubSub.SUBSCRIPTION, subscription);
        runner.setProperty(ConsumeGCPubSub.BATCH_SIZE, "10");

        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ConsumeGCPubSub.REL_SUCCESS, 10);
        runner.assertAllFlowFilesContainAttribute(ACK_ID_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(MSG_ATTRIBUTES_COUNT_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(MSG_PUBLISH_TIME_ATTRIBUTE);
    }

    @Test
    public void testConsumeWithBatchSize() throws InitializationException {
        final String subscription = "my-sub";
        runner.clearTransferState();

        runner = setCredentialsCS(runner);

        runner.setProperty(ConsumeGCPubSub.PROJECT_ID, PROJECT_ID);
        runner.setProperty(ConsumeGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(ConsumeGCPubSub.SUBSCRIPTION, subscription);
        runner.setProperty(ConsumeGCPubSub.BATCH_SIZE, "2");

        runner.assertValid();

        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumeGCPubSub.REL_SUCCESS, 2);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumeGCPubSub.REL_SUCCESS, 4);

        runner.assertAllFlowFilesContainAttribute(ACK_ID_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(MSG_ATTRIBUTES_COUNT_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(MSG_PUBLISH_TIME_ATTRIBUTE);
    }

    @Test
    public void testConsumeWithFormattedSubscriptionName() throws InitializationException {
        final String subscription = "projects/my-gcm-client/subscriptions/my-sub";
        runner.clearTransferState();

        runner = setCredentialsCS(runner);

        runner.setProperty(ConsumeGCPubSub.PROJECT_ID, PROJECT_ID);
        runner.setProperty(ConsumeGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(ConsumeGCPubSub.SUBSCRIPTION, subscription);
        runner.setProperty(ConsumeGCPubSub.BATCH_SIZE, "2");

        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ConsumeGCPubSub.REL_SUCCESS, 2);
        runner.assertAllFlowFilesContainAttribute(ACK_ID_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(MSG_ATTRIBUTES_COUNT_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(MSG_PUBLISH_TIME_ATTRIBUTE);
    }
}
