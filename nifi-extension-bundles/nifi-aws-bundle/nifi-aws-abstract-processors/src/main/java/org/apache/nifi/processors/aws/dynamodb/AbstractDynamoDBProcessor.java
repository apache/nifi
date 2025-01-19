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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.v2.AbstractAwsSyncProcessor;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for NiFi dynamo db related processors
 */
public abstract class AbstractDynamoDBProcessor extends AbstractAwsSyncProcessor<DynamoDbClient, DynamoDbClientBuilder> {

    public static final Relationship REL_UNPROCESSED = new Relationship.Builder().name("unprocessed")
            .description("FlowFiles are routed to unprocessed relationship when DynamoDB is not able to process "
               + "all the items in the request. Typical reasons are insufficient table throughput capacity and exceeding the maximum bytes per request. "
               + "Unprocessed FlowFiles can be retried with a new request.").build();

    public static final AllowableValue ALLOWABLE_VALUE_STRING = new AllowableValue("string");
    public static final AllowableValue ALLOWABLE_VALUE_NUMBER = new AllowableValue("number");

    public static final String DYNAMODB_KEY_ERROR_UNPROCESSED = "dynamodb.key.error.unprocessed";
    public static final String DYNAMODB_RANGE_KEY_VALUE_ERROR = "dynmodb.range.key.value.error";
    public static final String DYNAMODB_HASH_KEY_VALUE_ERROR = "dynmodb.hash.key.value.error";
    public static final String DYNAMODB_KEY_ERROR_NOT_FOUND = "dynamodb.key.error.not.found";
    public static final String DYNAMODB_ERROR_EXCEPTION_MESSAGE = "dynamodb.error.exception.message";
    public static final String DYNAMODB_ERROR_CODE = "dynamodb.error.code";
    public static final String DYNAMODB_ERROR_MESSAGE = "dynamodb.error.message";
    public static final String DYNAMODB_ERROR_SERVICE = "dynamodb.error.service";
    public static final String DYNAMODB_ERROR_RETRYABLE = "dynamodb.error.retryable";
    public static final String DYNAMODB_ERROR_REQUEST_ID = "dynamodb.error.request.id";
    public static final String DYNAMODB_ERROR_STATUS_CODE = "dynamodb.error.status.code";
    public static final String DYNAMODB_ITEM_HASH_KEY_VALUE = "  dynamodb.item.hash.key.value";
    public static final String DYNAMODB_ITEM_RANGE_KEY_VALUE = "  dynamodb.item.range.key.value";
    public static final String DYNAMODB_ITEM_IO_ERROR = "dynamodb.item.io.error";
    public static final String AWS_DYNAMO_DB_ITEM_SIZE_ERROR = "dynamodb.item.size.error";

    protected static final String DYNAMODB_KEY_ERROR_NOT_FOUND_MESSAGE = "DynamoDB key not found : ";

    public static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table Name")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The DynamoDB table name")
            .build();

    public static final PropertyDescriptor HASH_KEY_VALUE = new PropertyDescriptor.Builder()
            .name("Hash Key Value")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The hash key value of the item")
            .defaultValue("${dynamodb.item.hash.key.value}")
            .build();

    public static final PropertyDescriptor RANGE_KEY_VALUE = new PropertyDescriptor.Builder()
            .name("Range Key Value")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${dynamodb.item.range.key.value}")
            .build();

    public static final PropertyDescriptor HASH_KEY_VALUE_TYPE = new PropertyDescriptor.Builder()
            .name("Hash Key Value Type")
            .required(true)
            .description("The hash key value type of the item")
            .defaultValue(ALLOWABLE_VALUE_STRING.getValue())
            .allowableValues(ALLOWABLE_VALUE_STRING, ALLOWABLE_VALUE_NUMBER)
            .build();

    public static final PropertyDescriptor RANGE_KEY_VALUE_TYPE = new PropertyDescriptor.Builder()
            .name("Range Key Value Type")
            .required(true)
            .description("The range key value type of the item")
            .defaultValue(ALLOWABLE_VALUE_STRING.getValue())
            .allowableValues(ALLOWABLE_VALUE_STRING, ALLOWABLE_VALUE_NUMBER)
            .build();

    public static final PropertyDescriptor HASH_KEY_NAME = new PropertyDescriptor.Builder()
            .name("Hash Key Name")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The hash key name of the item")
            .build();

    public static final PropertyDescriptor RANGE_KEY_NAME = new PropertyDescriptor.Builder()
            .name("Range Key Name")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The range key name of the item")
            .build();

    public static final PropertyDescriptor JSON_DOCUMENT = new PropertyDescriptor.Builder()
            .name("Json Document attribute")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The Json document to be retrieved from the dynamodb item ('s' type in the schema)")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch items for each request (between 1 and 50)")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.createLongValidator(1, 50, true))
            .defaultValue("1")
            .description("The items to be retrieved in one batch")
            .build();

    public static final PropertyDescriptor DOCUMENT_CHARSET = new PropertyDescriptor.Builder()
            .name("Character set of document")
            .description("Character set of data in the document")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue(Charset.defaultCharset().name())
            .build();

    public static final Set<Relationship> COMMON_RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_UNPROCESSED
    );

    @Override
    public Set<Relationship> getRelationships() {
        return COMMON_RELATIONSHIPS;
    }

    @Override
    protected DynamoDbClientBuilder createClientBuilder(final ProcessContext context) {
        return DynamoDbClient.builder();
    }

    protected AttributeValue getAttributeValue(final ProcessContext context, final PropertyDescriptor type, final PropertyDescriptor value, final Map<String, String> attributes) {
        final AttributeValue.Builder builder = AttributeValue.builder();
        final String propertyValue = context.getProperty(value).evaluateAttributeExpressions(attributes).getValue();
        if (propertyValue == null) {
            return null;
        }

        if (context.getProperty(type).getValue().equals(ALLOWABLE_VALUE_STRING.getValue())) {
            builder.s(propertyValue);
        } else {
            builder.n(propertyValue);
        }
        return builder.build();
    }

    protected List<FlowFile> processException(final ProcessSession session, List<FlowFile> flowFiles, Exception exception) {
        List<FlowFile> failedFlowFiles = new ArrayList<>();
        for (FlowFile flowFile : flowFiles) {
            flowFile = session.putAttribute(flowFile, DYNAMODB_ERROR_EXCEPTION_MESSAGE, exception.getMessage() );
            failedFlowFiles.add(flowFile);
        }
        return failedFlowFiles;
    }

    protected List<FlowFile> processSdkException(final ProcessSession session, final List<FlowFile> flowFiles,
            final SdkException exception) {
        final List<FlowFile> failedFlowFiles = new ArrayList<>();
        for (FlowFile flowFile : flowFiles) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(DYNAMODB_ERROR_EXCEPTION_MESSAGE, exception.getMessage());
            attributes.put(DYNAMODB_ERROR_RETRYABLE, Boolean.toString(exception.retryable()));
            flowFile = session.putAllAttributes(flowFile, attributes);
            failedFlowFiles.add(flowFile);
        }
        return failedFlowFiles;
    }

    protected List<FlowFile> processServiceException(final ProcessSession session, final List<FlowFile> flowFiles,
            final AwsServiceException exception) {
        final List<FlowFile> failedFlowFiles = new ArrayList<>();
        for (FlowFile flowFile : flowFiles) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(DYNAMODB_ERROR_EXCEPTION_MESSAGE, exception.getMessage() );
            attributes.put(DYNAMODB_ERROR_CODE, exception.awsErrorDetails().errorCode() );
            attributes.put(DYNAMODB_ERROR_MESSAGE, exception.awsErrorDetails().errorMessage() );
            attributes.put(DYNAMODB_ERROR_SERVICE, exception.awsErrorDetails().serviceName() );
            attributes.put(DYNAMODB_ERROR_RETRYABLE, Boolean.toString(exception.retryable()));
            attributes.put(DYNAMODB_ERROR_REQUEST_ID, exception.requestId() );
            attributes.put(DYNAMODB_ERROR_STATUS_CODE, Integer.toString(exception.statusCode()) );
            flowFile = session.putAllAttributes(flowFile, attributes);
            failedFlowFiles.add(flowFile);
        }
        return failedFlowFiles;
    }

    /**
     * Send unhandled items to failure and remove the flow files from key to flow file map
     * @param session used for sending the flow file
     * @param keysToFlowFileMap - ItemKeys to flow file map
     * @param hashKeyValue the items hash key value
     * @param rangeKeyValue the items hash key value
     */
    protected void sendUnprocessedToUnprocessedRelationship(final ProcessSession session, final Map<ItemKeys, FlowFile> keysToFlowFileMap,
                                                            final AttributeValue hashKeyValue, final AttributeValue rangeKeyValue) {
        final ItemKeys itemKeys = new ItemKeys(hashKeyValue, rangeKeyValue);

        FlowFile flowFile = keysToFlowFileMap.get(itemKeys);
        if (flowFile == null) {
            return;
        }

        flowFile = session.putAttribute(flowFile, DYNAMODB_KEY_ERROR_UNPROCESSED, itemKeys.toString());
        session.transfer(flowFile, REL_UNPROCESSED);

        getLogger().error("Unprocessed key {} for flow file {}", itemKeys, flowFile);

        keysToFlowFileMap.remove(itemKeys);
    }

    protected boolean isRangeKeyValueConsistent(final String rangeKeyName, final AttributeValue rangeKeyValue, final ProcessSession session, FlowFile flowFile) {
        try {
            validateRangeKeyValue(rangeKeyName, rangeKeyValue);
        } catch (final IllegalArgumentException e) {
            getLogger().error("{}", flowFile, e);
            flowFile = session.putAttribute(flowFile, DYNAMODB_RANGE_KEY_VALUE_ERROR, "range key '" + rangeKeyName
                 + "'/value '" + rangeKeyValue + "' inconsistency error");
            session.transfer(flowFile, REL_FAILURE);
            return false;
        }

        return true;
    }

    protected void validateRangeKeyValue(final String rangeKeyName, final AttributeValue rangeKeyValue) {
        boolean isRangeNameBlank = StringUtils.isBlank(rangeKeyName);
        boolean isConsistent = true;
        if (!isRangeNameBlank && isBlank(rangeKeyValue)) {
            isConsistent = false;
        }
        if (isRangeNameBlank && !isBlank(rangeKeyValue)) {
            isConsistent = false;
        }
        if (!isConsistent) {
            throw new IllegalArgumentException(String.format("Range key name '%s' was not consistent with range value '%s'", rangeKeyName, rangeKeyValue));
        }
    }

    protected boolean isHashKeyValueConsistent(final String hashKeyName, final AttributeValue hashKeyValue, final ProcessSession session, FlowFile flowFile) {

        boolean isConsistent = true;

        try {
            validateHashKeyValue(hashKeyValue);
        } catch (final IllegalArgumentException e) {
            getLogger().error("{}", flowFile, e);
            flowFile = session.putAttribute(flowFile, DYNAMODB_HASH_KEY_VALUE_ERROR, "hash key " + hashKeyName + "/value '" + hashKeyValue + "' inconsistency error");
            session.transfer(flowFile, REL_FAILURE);
            isConsistent = false;
        }

        return isConsistent;

    }

    protected void validateHashKeyValue(final AttributeValue hashKeyValue) {
        if (isBlank(hashKeyValue)) {
            throw new IllegalArgumentException(String.format("Hash key value is required.  Provided value was '%s'", hashKeyValue));
        }
    }

    /**
     * @param attributeValue At attribute value
     * @return True if the AttributeValue is null or both 's' and 'n' are null or blank
     */
    protected static boolean isBlank(final AttributeValue attributeValue) {
        return attributeValue == null || (StringUtils.isBlank(attributeValue.s()) && StringUtils.isBlank(attributeValue.n()));
    }
}
