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
package org.apache.nifi.processors.aws.credentials.provider.service;

import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AWSProcessorProxyTest {

    private TestRunner runner;

    @BeforeEach
    public void testSetup() {
        runner = TestRunners.newTestRunner(FetchS3Object.class);
        runner.setProperty(FetchS3Object.BUCKET_WITHOUT_DEFAULT_VALUE, "bucket");
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
        runner.assertValid();
    }

    @AfterEach
    public void testTearDown() {
        runner = null;
    }

    @Test
    public void testProxyHostOnlyInvalid() {
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_HOST, "proxyHost");
        runner.assertNotValid();
    }

    @Test
    public void testProxyHostPortOnlyInvalid() {
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_HOST_PORT, "1");
        runner.assertNotValid();
    }

    @Test
    public void testProxyHostPortNonNumberInvalid() {
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_HOST_PORT, "a");
        runner.assertNotValid();
    }

    @Test
    public void testProxyHostAndPortValid() {
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_HOST_PORT, "1");
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_HOST, "proxyHost");
        runner.assertValid();
    }

    @Test
    public void testProxyUserNoPasswordInValid() {
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_USERNAME, "foo");
        runner.assertNotValid();
    }

    @Test
    public void testProxyNoUserPasswordInValid() {
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_PASSWORD, "foo");
        runner.assertNotValid();
    }

    @Test
    public void testProxyUserPasswordNoHostInValid() {
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_USERNAME, "foo");
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_PASSWORD, "foo");
        runner.assertNotValid();
    }

    @Test
    public void testProxyUserPasswordHostValid() {
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_HOST_PORT, "1");
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_HOST, "proxyHost");
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_USERNAME, "foo");
        runner.setProperty(AbstractAWSCredentialsProviderProcessor.PROXY_PASSWORD, "foo");
        runner.assertValid();
    }

}