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

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

public class S3Step {
    public static final String S3_STEP_NAME = "S3 Configuration";
    public static final String ACCESS_KEY_ID_SECRET_KEY = "Access Key ID and Secret Key";
    public static final String DEFAULT_CREDENTIALS = "Default AWS Credentials";


    public static final ConnectorPropertyDescriptor S3_BUCKET = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Bucket")
        .description("The name of the S3 bucket to write data to.")
        .required(true)
        .build();

    public static final ConnectorPropertyDescriptor S3_REGION = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Region")
        .description("The AWS region where the S3 bucket is located.")
        .allowableValuesFetchable(true)
        .required(true)
        .build();

    public static final ConnectorPropertyDescriptor S3_DATA_FORMAT = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Data Format")
        .description("The format to use when writing data to S3.")
        .required(true)
        .defaultValue("JSON")
        .allowableValues("Avro", "JSON")
        .build();

    public static final ConnectorPropertyDescriptor S3_PREFIX = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Prefix")
        .description("An optional prefix to prepend to all object keys written to the S3 bucket.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final ConnectorPropertyDescriptor S3_ENDPOINT_OVERRIDE_URL = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Endpoint Override URL")
        .description("An optional endpoint URL to use instead of the default AWS S3 endpoint. " +
                     "This can be used to connect to S3-compatible storage systems but should be left unset for connecting to S3.")
        .required(false)
        .addValidator(StandardValidators.URL_VALIDATOR)
        .build();

    public static final ConnectorPropertyDescriptor TARGET_OBJECT_SIZE = new ConnectorPropertyDescriptor.Builder()
        .name("Target Object Size")
        .description("The target size for each object written to S3. The connector will attempt to " +
                     "combine messages until this size is reached before writing an object to S3. Note that this size is approximate " +
                     "and may vary from object to object.")
        .required(true)
        .defaultValue("256 MB")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();

    public static final ConnectorPropertyDescriptor MERGE_LATENCY = new ConnectorPropertyDescriptor.Builder()
        .name("Merge Latency")
        .description("The maximum amount of time to wait while merging messages before writing an object to S3. " +
                     "If this time is reached before the target object size is met, the current set of merged messages " +
                     "will be written to S3.")
        .required(true)
        .defaultValue("5 min")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();

    public static final ConnectorPropertyDescriptor S3_AUTHENTICATION_STRATEGY = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Authentication Strategy")
        .description("The authentication strategy to use when connecting to S3.")
        .required(true)
        .defaultValue(ACCESS_KEY_ID_SECRET_KEY)
        .allowableValues(ACCESS_KEY_ID_SECRET_KEY, DEFAULT_CREDENTIALS)
        .build();

    public static final ConnectorPropertyDescriptor S3_ACCESS_KEY_ID = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Access Key ID")
        .description("The AWS Access Key ID used to authenticate to S3.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .dependsOn(S3_AUTHENTICATION_STRATEGY, ACCESS_KEY_ID_SECRET_KEY)
        .build();

    public static final ConnectorPropertyDescriptor S3_SECRET_ACCESS_KEY = new ConnectorPropertyDescriptor.Builder()
        .name("S3 Secret Access Key")
        .description("The AWS Secret Access Key used to authenticate to S3.")
        .required(true)
        .type(PropertyType.SECRET)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .dependsOn(S3_AUTHENTICATION_STRATEGY, ACCESS_KEY_ID_SECRET_KEY)
        .build();


    public static final ConnectorPropertyGroup S3_DESTINATION_GROUP = new ConnectorPropertyGroup.Builder()
        .name("S3 Destination Configuration")
        .description("Properties required to connect to S3 and specify the target bucket.")
        .properties(List.of(
            S3_BUCKET,
            S3_PREFIX,
            S3_REGION,
            S3_DATA_FORMAT,
            S3_ENDPOINT_OVERRIDE_URL
        ))
        .build();

    public static final ConnectorPropertyGroup S3_CREDENTIALS_GROUP = new ConnectorPropertyGroup.Builder()
        .name("S3 Credentials")
        .description("Properties required to authenticate to S3.")
        .properties(List.of(
            S3_AUTHENTICATION_STRATEGY,
            S3_ACCESS_KEY_ID,
            S3_SECRET_ACCESS_KEY
        ))
        .build();

    public static final ConnectorPropertyGroup MERGE_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Merge Configuration")
        .description("Configuration for how data should be merged together before being persisted to S3.")
        .properties(List.of(
            TARGET_OBJECT_SIZE,
            MERGE_LATENCY
        ))
        .build();

    public static final ConfigurationStep S3_STEP = new ConfigurationStep.Builder()
        .name(S3_STEP_NAME)
        .description("Configure connection to S3 and target bucket details.")
        .propertyGroups(List.of(
            S3_DESTINATION_GROUP,
            MERGE_GROUP,
            S3_CREDENTIALS_GROUP
        ))
        .build();
}
