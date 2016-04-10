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

import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.List;

public class TestGetIOTMqtt {
    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @Test
    public void testSimpleGetUsingCredentailsProviderService() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(new GetIOTMqtt());

        runner.setProperty(GetIOTMqtt.PROP_NAME_CLIENT, "RandomClientId");
        runner.setProperty(GetIOTMqtt.PROP_NAME_ENDPOINT, "A1B71MLXKNXXXX");
        runner.setProperty(GetIOTMqtt.PROP_NAME_TOPIC, "$aws/things/nifiConsumer/shadow/update");
        runner.setProperty(GetIOTMqtt.PROP_QOS, "0");

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);

        runner.setProperty(serviceImpl, AbstractAWSCredentialsProviderProcessor.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);

        runner.setProperty(GetIOTMqtt.AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
        runner.run(1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetIOTMqtt.REL_SUCCESS);
        for (final MockFlowFile mff : flowFiles) {
            System.out.println(mff.getAttributes());
            System.out.println(new String(mff.toByteArray()));
        }
    }
}
