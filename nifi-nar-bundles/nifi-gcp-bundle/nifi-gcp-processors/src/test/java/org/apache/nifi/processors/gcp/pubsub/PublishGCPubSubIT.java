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

import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.TOPIC_NAME_ATTRIBUTE;

public class PublishGCPubSubIT extends AbstractGCPubSubIT{

    @BeforeClass
    public static void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(PublishGCPubSub.class);
    }

    @Test
    public void testSimplePublish() throws InitializationException {
        final String topic = "my-topic";

        runner = setCredentialsCS(runner);

        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT_ID);
        runner.setProperty(PublishGCPubSub.TOPIC_NAME, topic);
        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(PublishGCPubSub.BATCH_SIZE, "1");

        runner.assertValid();

        runner.enqueue("Testing simple publish");
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishGCPubSub.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(MESSAGE_ID_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(TOPIC_NAME_ATTRIBUTE);
    }

    @Test
    public void testPublishWithFormattedTopicName() throws InitializationException {
        final String topic = "projects/my-gcm-client/topics/my-topic";

        runner = setCredentialsCS(runner);

        runner.setProperty(PublishGCPubSub.PROJECT_ID, PROJECT_ID);
        runner.setProperty(PublishGCPubSub.TOPIC_NAME, topic);
        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(PublishGCPubSub.BATCH_SIZE, "1");

        runner.assertValid();

        runner.enqueue("Testing publish with formatted topic name");
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishGCPubSub.REL_SUCCESS, 2);
        runner.assertAllFlowFilesContainAttribute(MESSAGE_ID_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(TOPIC_NAME_ATTRIBUTE);
    }
}
