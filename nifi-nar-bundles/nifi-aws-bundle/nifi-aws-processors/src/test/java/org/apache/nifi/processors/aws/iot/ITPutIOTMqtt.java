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
package org.apache.nifi.processors.aws.iot;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public class ITPutIOTMqtt {
    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Ignore
    @Test
    public void testSimplePutUsingCredentialsProviderService() throws Throwable {
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        final TestRunner runner = TestRunners.newTestRunner(new PublishAWSIoTMqtt());
        final String clientId = PublishAWSIoTMqtt.class.getSimpleName();
        final String endpoint = "A1B71MLXKNXXXX";
        final String topic = "$aws/things/nifiConsumer/shadow/update";
        final String qos = "0";
        final byte[] message = "{\"state\":{\"desired\":{\"key\":\"value\"}}}".getBytes();
        final Region region = Regions.getCurrentRegion();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.setProperty(PublishAWSIoTMqtt.PROP_CLIENT, clientId);
        runner.setProperty(PublishAWSIoTMqtt.PROP_ENDPOINT, endpoint);
        runner.setProperty(PublishAWSIoTMqtt.PROP_TOPIC, topic);
        runner.setProperty(PublishAWSIoTMqtt.PROP_QOS, qos);
        runner.setProperty(PublishAWSIoTMqtt.REGION, region.getName());
        runner.setProperty(PublishAWSIoTMqtt.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");

        runner.setProperty(serviceImpl, AbstractAWSCredentialsProviderProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);

        // ensure that the Controller Service is configured accordingly
        runner.assertValid(serviceImpl);

        // If the Controller Service is not valid, this method will throw an IllegalStateException. Otherwise, the service is now ready to use.
        runner.enableControllerService(serviceImpl);

        runner.enqueue(message);

        runner.run(1);

        // validate that the FlowFiles went where they were expected to go
        runner.assertAllFlowFilesTransferred(PublishAWSIoTMqtt.REL_SUCCESS, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PublishAWSIoTMqtt.REL_SUCCESS);
        for (final MockFlowFile mff : flowFiles) {
            mff.assertContentEquals(message);
        }
    }
}
