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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PublishGCPubSubTest {

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(PublishGCPubSub.class);
    }

    @Test
    void testPropertyDescriptors() throws InitializationException {
        runner.assertNotValid();

        final ControllerService controllerService = new GCPCredentialsControllerService();
        final String controllerServiceId = GCPCredentialsControllerService.class.getSimpleName();
        runner.addControllerService(controllerServiceId, controllerService);
        runner.enableControllerService(controllerService);
        //runner.setProperty(GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE, controllerServiceId);
        runner.setProperty(PublishGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, controllerServiceId);
        runner.assertNotValid();

        runner.setProperty(PublishGCPubSub.TOPIC_NAME, "my-topic");
        runner.assertNotValid();

        runner.setProperty(PublishGCPubSub.PROJECT_ID, "my-project");
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
}
