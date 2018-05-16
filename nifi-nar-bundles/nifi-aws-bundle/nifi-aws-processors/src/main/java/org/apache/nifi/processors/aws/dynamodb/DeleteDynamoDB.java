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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

@SupportsBatching
@SeeAlso({GetDynamoDB.class, PutDynamoDB.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "DynamoDB", "AWS", "Delete", "Remove"})
@CapabilityDescription("Deletes a document from DynamoDB based on hash and range key. The key can be string or number."
        + " The request requires all the primary keys for the operation (hash or hash and range key)")
@WritesAttributes({
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_UNPROCESSED, description = "Dynamo db unprocessed keys"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR, description = "Dynamod db range key error"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND, description = "Dynamo db key not found"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE, description = "Dynamo db exception message"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_CODE, description = "Dynamo db error code"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_MESSAGE, description = "Dynamo db error message"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_TYPE, description = "Dynamo db error type"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_SERVICE, description = "Dynamo db error service"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_RETRYABLE, description = "Dynamo db error is retryable"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_REQUEST_ID, description = "Dynamo db error request id"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_STATUS_CODE, description = "Dynamo db status code")
    })
@ReadsAttributes({
    @ReadsAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ITEM_HASH_KEY_VALUE, description = "Items hash key value" ),
    @ReadsAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ITEM_RANGE_KEY_VALUE, description = "Items range key value" ),
    })
public class DeleteDynamoDB extends AbstractWriteDynamoDBProcessor {

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(TABLE, HASH_KEY_NAME, RANGE_KEY_NAME, HASH_KEY_VALUE, RANGE_KEY_VALUE,
                HASH_KEY_VALUE_TYPE, RANGE_KEY_VALUE_TYPE, BATCH_SIZE, REGION, ACCESS_KEY, SECRET_KEY,
                CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, SSL_CONTEXT_SERVICE));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger());
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        Map<ItemKeys,FlowFile> keysToFlowFileMap = new HashMap<>();

        final String table = context.getProperty(TABLE).evaluateAttributeExpressions().getValue();

        final String hashKeyName = context.getProperty(HASH_KEY_NAME).evaluateAttributeExpressions().getValue();
        final String hashKeyValueType = context.getProperty(HASH_KEY_VALUE_TYPE).getValue();
        final String rangeKeyName = context.getProperty(RANGE_KEY_NAME).evaluateAttributeExpressions().getValue();
        final String rangeKeyValueType = context.getProperty(RANGE_KEY_VALUE_TYPE).getValue();

        TableWriteItems tableWriteItems = new TableWriteItems(table);

        for (FlowFile flowFile : flowFiles) {
            final Object hashKeyValue = getValue(context, HASH_KEY_VALUE_TYPE, HASH_KEY_VALUE, flowFile);
            final Object rangeKeyValue = getValue(context, RANGE_KEY_VALUE_TYPE, RANGE_KEY_VALUE, flowFile);

            if ( ! isHashKeyValueConsistent(hashKeyName, hashKeyValue, session, flowFile)) {
                continue;
            }

            if ( ! isRangeKeyValueConsistent(rangeKeyName, rangeKeyValue, session, flowFile) ) {
                continue;
            }

            if ( rangeKeyValue == null || StringUtils.isBlank(rangeKeyValue.toString()) ) {
                tableWriteItems.addHashOnlyPrimaryKeysToDelete(hashKeyName, hashKeyValue);
            } else {
                tableWriteItems.addHashAndRangePrimaryKeyToDelete(hashKeyName,
                        hashKeyValue, rangeKeyName, rangeKeyValue);
            }
            keysToFlowFileMap.put(new ItemKeys(hashKeyValue, rangeKeyValue), flowFile);
        }

        if ( keysToFlowFileMap.isEmpty() ) {
            return;
        }

        final DynamoDB dynamoDB = getDynamoDB();

        try {
            BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(tableWriteItems);

            handleUnprocessedItems(session, keysToFlowFileMap, table, hashKeyName, hashKeyValueType, rangeKeyName,
               rangeKeyValueType, outcome);

            // All non unprocessed items are successful
            for (FlowFile flowFile : keysToFlowFileMap.values()) {
                getLogger().debug("Successfully deleted item from dynamodb : " + table);
                session.transfer(flowFile,REL_SUCCESS);
            }
        } catch(AmazonServiceException exception) {
            getLogger().error("Could not process flowFiles due to service exception : " + exception.getMessage());
            List<FlowFile> failedFlowFiles = processServiceException(session, flowFiles, exception);
            session.transfer(failedFlowFiles, REL_FAILURE);
        } catch(AmazonClientException exception) {
            getLogger().error("Could not process flowFiles due to client exception : " + exception.getMessage());
            List<FlowFile> failedFlowFiles = processClientException(session, flowFiles, exception);
            session.transfer(failedFlowFiles, REL_FAILURE);
        } catch(Exception exception) {
            getLogger().error("Could not process flowFiles due to exception : " + exception.getMessage());
            List<FlowFile> failedFlowFiles = processException(session, flowFiles, exception);
            session.transfer(failedFlowFiles, REL_FAILURE);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected Map<String, AttributeValue> getRequestItem(WriteRequest writeRequest) {
        return writeRequest.getDeleteRequest().getKey();
    }
}
