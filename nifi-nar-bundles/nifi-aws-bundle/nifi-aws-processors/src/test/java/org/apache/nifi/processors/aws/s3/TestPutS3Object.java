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
package org.apache.nifi.processors.aws.s3;

import java.util.List;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;

import org.junit.Assert;
import org.junit.Test;


/**
 * Unit tests for {@link PutS3Object}, without interaction with S3.
 */
public class TestPutS3Object {

    @Test
    public void testSignerOverrideOptions() {
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        final ClientConfiguration config = new ClientConfiguration();
        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final List<AllowableValue> allowableSignerValues = PutS3Object.SIGNER_OVERRIDE.getAllowableValues();
        final String defaultSignerValue = PutS3Object.SIGNER_OVERRIDE.getDefaultValue();

        for (AllowableValue allowableSignerValue : allowableSignerValues) {
            String signerType = allowableSignerValue.getValue();
            if (!signerType.equals(defaultSignerValue)) {
                runner.setProperty(PutS3Object.SIGNER_OVERRIDE, signerType);
                ProcessContext context = runner.getProcessContext();
                try {
                    AmazonS3Client s3Client = processor.createClient(context, credentialsProvider, config);
                } catch (IllegalArgumentException argEx) {
                    Assert.fail(argEx.getMessage());
                }
            }
        }
    }

}
