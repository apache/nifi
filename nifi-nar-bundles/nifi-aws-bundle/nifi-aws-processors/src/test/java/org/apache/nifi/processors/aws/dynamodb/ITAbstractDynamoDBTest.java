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

import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.util.ArrayList;

import org.apache.nifi.flowfile.FlowFile;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class ITAbstractDynamoDBTest {

    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    protected static DynamoDB dynamoDB;
    protected static AmazonDynamoDBClient amazonDynamoDBClient;
    protected static String stringHashStringRangeTableName = "StringHashStringRangeTable";
    protected static String numberHashNumberRangeTableName = "NumberHashNumberRangeTable";
    protected static String numberHashOnlyTableName = "NumberHashOnlyTable";
    protected final static String REGION = "us-west-2";

    @BeforeClass
    public static void beforeClass() throws Exception {
        FileInputStream fis = new FileInputStream(CREDENTIALS_FILE);
        final PropertiesCredentials credentials = new PropertiesCredentials(fis);
        amazonDynamoDBClient = new AmazonDynamoDBClient(credentials);
        dynamoDB = new DynamoDB(amazonDynamoDBClient);
        amazonDynamoDBClient.setRegion(Region.getRegion(Regions.US_WEST_2));

        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions
            .add(new AttributeDefinition().withAttributeName("hashS").withAttributeType("S"));
        attributeDefinitions
            .add(new AttributeDefinition().withAttributeName("rangeS").withAttributeType("S"));

        ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("hashS").withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement().withAttributeName("rangeS").withKeyType(KeyType.RANGE));

        CreateTableRequest request = new CreateTableRequest()
            .withTableName(stringHashStringRangeTableName)
            .withKeySchema(keySchema)
            .withAttributeDefinitions(attributeDefinitions)
            .withProvisionedThroughput(new ProvisionedThroughput()
                .withReadCapacityUnits(5L)
                .withWriteCapacityUnits(6L));
        Table stringHashStringRangeTable = dynamoDB.createTable(request);
        stringHashStringRangeTable.waitForActive();

        attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions
            .add(new AttributeDefinition().withAttributeName("hashN").withAttributeType("N"));
        attributeDefinitions
            .add(new AttributeDefinition().withAttributeName("rangeN").withAttributeType("N"));

        keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("hashN").withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement().withAttributeName("rangeN").withKeyType(KeyType.RANGE));

        request = new CreateTableRequest()
            .withTableName(numberHashNumberRangeTableName)
            .withKeySchema(keySchema)
            .withAttributeDefinitions(attributeDefinitions)
            .withProvisionedThroughput(new ProvisionedThroughput()
                .withReadCapacityUnits(5L)
                .withWriteCapacityUnits(6L));
        Table numberHashNumberRangeTable = dynamoDB.createTable(request);
        numberHashNumberRangeTable.waitForActive();

        attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions
            .add(new AttributeDefinition().withAttributeName("hashN").withAttributeType("N"));

        keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("hashN").withKeyType(KeyType.HASH));

        request = new CreateTableRequest()
            .withTableName(numberHashOnlyTableName)
            .withKeySchema(keySchema)
            .withAttributeDefinitions(attributeDefinitions)
            .withProvisionedThroughput(new ProvisionedThroughput()
                .withReadCapacityUnits(5L)
                .withWriteCapacityUnits(6L));
        Table numberHashOnlyTable = dynamoDB.createTable(request);
        numberHashOnlyTable.waitForActive();

    }

    protected static void validateServiceExceptionAttribute(FlowFile flowFile) {
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_CODE));
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_MESSAGE));
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_TYPE));
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_SERVICE));
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_RETRYABLE));
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_REQUEST_ID));
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_STATUS_CODE));
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE));
        assertNotNull(flowFile.getAttribute(AbstractDynamoDBProcessor.DYNAMODB_ERROR_RETRYABLE));
    }

    @AfterClass
    public static void afterClass() {
        DeleteTableResult result = amazonDynamoDBClient.deleteTable(stringHashStringRangeTableName);
        result = amazonDynamoDBClient.deleteTable(numberHashNumberRangeTableName);
        result = amazonDynamoDBClient.deleteTable(numberHashOnlyTableName);
    }


}
