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
package org.apache.nifi.processors.aws.dynamodb;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AbstractDynamoDBIT {
    protected static final String PARTITION_KEY_ONLY_TABLE = "partitionKeyTable";
    protected static final String PARTITION_AND_SORT_KEY_TABLE = "partitionKeySortKeyTable";
    protected static final String PARTITION_KEY = "partitionKey";
    protected static final String SORT_KEY = "sortKey";
    protected static final String PARTITION_KEY_VALUE_PREFIX = "partition.value.";

    private static DynamoDbClient client;

    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:latest");

    private static final LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(LocalStackContainer.Service.DYNAMODB);

    @BeforeAll
    public static void oneTimeSetup() {
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService");
        localstack.start();

        client = DynamoDbClient.builder()
                .endpointOverride(localstack.getEndpoint())
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        final AttributeDefinition partitionKeyDefinition = AttributeDefinition.builder().attributeName(PARTITION_KEY).attributeType(ScalarAttributeType.S).build();
        final KeySchemaElement partitionKeySchema = KeySchemaElement.builder().attributeName(PARTITION_KEY).keyType(KeyType.HASH).build();
        final AttributeDefinition sortKeyDefinition = AttributeDefinition.builder().attributeName(SORT_KEY).attributeType(ScalarAttributeType.N).build();
        final KeySchemaElement sortKeySchema = KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build();

        final ProvisionedThroughput provisionedThroughput = ProvisionedThroughput.builder()
                .readCapacityUnits(1000L)
                .writeCapacityUnits(1000L).build();
        final CreateTableRequest request1 = CreateTableRequest.builder()
                .tableName(PARTITION_KEY_ONLY_TABLE)
                .attributeDefinitions(partitionKeyDefinition)
                .keySchema(partitionKeySchema)
                .provisionedThroughput(provisionedThroughput)
                .build();
        final CreateTableRequest request2 = CreateTableRequest.builder()
                .tableName(PARTITION_AND_SORT_KEY_TABLE)
                .attributeDefinitions(partitionKeyDefinition, sortKeyDefinition)
                .provisionedThroughput(provisionedThroughput)
                .keySchema(partitionKeySchema, sortKeySchema)
                .build();
        client.createTable(request1);
        client.createTable(request2);
    }

    @AfterAll
    public static void oneTimeTeardown() {
        client.close();
        localstack.stop();
    }

    protected DynamoDbClient getClient() {
        return client;
    }

    protected TestRunner initRunner(final Class<? extends Processor> processorClass) {
        TestRunner runner = TestRunners.newTestRunner(processorClass);
        AuthUtils.enableAccessKey(runner, localstack.getAccessKey(), localstack.getSecretKey());

        runner.setProperty(AbstractDynamoDBProcessor.REGION, localstack.getRegion());
        runner.setProperty(AbstractDynamoDBProcessor.ENDPOINT_OVERRIDE, localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString());
        return runner;
    }

    protected BatchGetItemResponse getBatchGetItems(int count, String table, boolean includeSortKey) {
        final Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        final Collection<Map<String, AttributeValue>> keys = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final Map<String, AttributeValue> partitionKey = new HashMap<>();
            partitionKey.put(PARTITION_KEY, AttributeValue.builder().s(PARTITION_KEY_VALUE_PREFIX + i).build());
            if (includeSortKey) {
                partitionKey.put(SORT_KEY, AttributeValue.builder().n(String.valueOf(i)).build());
            }
            keys.add(partitionKey);
        }
        final KeysAndAttributes keysAndAttributes = KeysAndAttributes.builder().keys(keys).build();
        requestItems.put(table, keysAndAttributes);
        final BatchGetItemRequest request = BatchGetItemRequest.builder().requestItems(requestItems).build();

        final BatchGetItemResponse response = getClient().batchGetItem(request);
        return response;
    }
}
