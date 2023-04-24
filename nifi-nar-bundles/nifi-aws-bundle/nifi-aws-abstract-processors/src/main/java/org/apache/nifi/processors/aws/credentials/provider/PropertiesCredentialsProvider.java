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
package org.apache.nifi.processors.aws.credentials.provider;


import org.apache.nifi.processor.exception.ProcessException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class PropertiesCredentialsProvider implements AwsCredentialsProvider {

    private final String accessKey;
    private final String secretAccessKey;

    public PropertiesCredentialsProvider(final File credentialsProperties) {
        try {
            if (!credentialsProperties.exists()) {
                throw new FileNotFoundException("File doesn't exist: " + credentialsProperties.getAbsolutePath());
            }

            try (final FileInputStream stream = new FileInputStream(credentialsProperties)) {
                final Properties accountProperties = new Properties();
                accountProperties.load(stream);

                if (accountProperties.getProperty("accessKey") == null || accountProperties.getProperty("secretKey") == null) {
                    throw new IllegalArgumentException(String.format("The specified file (%s) doesn't contain the expected properties " +
                            "'accessKey' and 'secretKey'.", credentialsProperties.getAbsolutePath()));
                }

                accessKey = accountProperties.getProperty("accessKey");
                secretAccessKey = accountProperties.getProperty("secretKey");
            }
        } catch (final IOException e) {
            throw new ProcessException("Failed to load AWS credentials properties " + credentialsProperties, e);
        }
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return AwsBasicCredentials.create(accessKey, secretAccessKey);
    }
}