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
package org.apache.nifi.processors.aws.credentials.provider.factory;

import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleWithWebIdentitySessionCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Tests of the validation and credentials provider capabilities of CredentialsProviderFactory.
 */
public class TestCredentialsProviderFactoryWebIdentityOnly {
    
    @Test
    public void testAssumeRoleWithWebIdentity() throws Throwable {
        final TestRunner runner = TestRunners.newTestRunner(MockAWSProcessor.class);
        
        // The credentials don't matter since we're not connecting, just use the credentials file as a mock
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_ARN, "BogusArn");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_NAME, "BogusSession");
        runner.setProperty(CredentialPropertyDescriptors.ASSUME_ROLE_WITH_WEB_IDENTITY_TOKEN_FILENAME, "BobTheFile.txt");
        runner.assertValid();

        Map<PropertyDescriptor, String> properties = runner.getProcessContext().getProperties();
        
        final CredentialsProviderFactory factory = new CredentialsProviderFactory();
        final AWSCredentialsProvider credentialsProvider = factory.getCredentialsProvider(properties);
        Assert.assertNotNull(credentialsProvider);
        assertEquals("credentials provider should be equal", STSAssumeRoleWithWebIdentitySessionCredentialsProvider.class,
                credentialsProvider.getClass());
    }
}
