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

package org.apache.nifi.processors.aws.testutil;

import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;

public class AuthUtils {
    public static void enableCredentialsFile(final TestRunner runner, final String credentialsFile) {
        final AWSCredentialsProviderControllerService credentialsService = new AWSCredentialsProviderControllerService();
        try {
            runner.addControllerService("creds", credentialsService);
        } catch (final InitializationException e) {
            throw new AssertionError("Failed to enable AWSCredentialsProviderControllerService", e);
        }

        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.CREDENTIALS_FILE, credentialsFile);
        runner.enableControllerService(credentialsService);

        runner.setProperty(AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE, "creds");
    }

    public static void enableAccessKey(final TestRunner runner, final String accessKeyId, final String secretKey) {
        final AWSCredentialsProviderControllerService credentialsService = new AWSCredentialsProviderControllerService();
        try {
            runner.addControllerService("creds", credentialsService);
        } catch (final InitializationException e) {
            throw new AssertionError("Failed to enable AWSCredentialsProviderControllerService", e);
        }

        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, accessKeyId);
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.SECRET_KEY, secretKey);
        runner.enableControllerService(credentialsService);

        runner.setProperty(AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE, "creds");
    }
}
