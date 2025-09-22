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
package org.apache.nifi.kafka.shared.login;

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.AwsRoleSource;
import org.apache.nifi.util.StringUtils;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;

/**
 * SASL AWS MSK IAM Login Module implementation of configuration provider
 */
public class AwsMskIamLoginConfigProvider implements LoginConfigProvider {

    private static final String MODULE_CLASS = "software.amazon.msk.auth.iam.IAMLoginModule";

    private static final String AWS_PROFILE_NAME_KEY = "awsProfileName";
    private static final String ROLE_ARN_KEY = "awsRoleArn";
    private static final String ROLE_SESSION_NAME_KEY = "awsRoleSessionName";

    @Override
    public String getConfiguration(PropertyContext context) {
        final AwsRoleSource roleSource = context.getProperty(KafkaClientComponent.AWS_ROLE_SOURCE).asAllowableValue(AwsRoleSource.class);
        final LoginConfigBuilder builder = new LoginConfigBuilder(MODULE_CLASS, REQUIRED);

        if (roleSource == AwsRoleSource.SPECIFIED_PROFILE) {
            final String awsProfileName = context.getProperty(KafkaClientComponent.AWS_PROFILE_NAME).getValue();
            if (!StringUtils.isBlank(awsProfileName)) {
                builder.append(AWS_PROFILE_NAME_KEY, awsProfileName);
            }
        }

        if (roleSource == AwsRoleSource.SPECIFIED_ROLE) {
            final String assumeRoleArn = context.getProperty(KafkaClientComponent.AWS_ASSUME_ROLE_ARN).getValue();
            final String assumeRoleSessionName = context.getProperty(KafkaClientComponent.AWS_ASSUME_ROLE_SESSION_NAME).getValue();

            builder.append(ROLE_ARN_KEY, assumeRoleArn);
            builder.append(ROLE_SESSION_NAME_KEY, assumeRoleSessionName);
        }

        return builder.build();
    }
}
