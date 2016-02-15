package org.apache.nifi.processors.aws.dynamodb;

import java.util.List;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public abstract class AbstractWriteDynamoDBProcessor extends AbstractDynamoDBProcessor {

    protected void handleUnprocessedItems(final ProcessSession session, Map<ItemKeys, FlowFile> keysToFlowFileMap, final String table, final String hashKeyName, final String hashKeyValueType,
            final String rangeKeyName, final String rangeKeyValueType, BatchWriteItemOutcome outcome) {
        BatchWriteItemResult result = outcome.getBatchWriteItemResult();

        // Handle unprocessed items
        List<WriteRequest> unprocessedItems = result.getUnprocessedItems().get(table);
        if ( unprocessedItems != null && unprocessedItems.size() > 0 ) {
            for ( WriteRequest request : unprocessedItems) {
                Map<String,AttributeValue> item = request.getPutRequest().getItem();
                Object hashKeyValue = getValue(item, hashKeyName, hashKeyValueType);
                Object rangeKeyValue = getValue(item, rangeKeyName, rangeKeyValueType);

                sendUnhandledToFailure(session, keysToFlowFileMap, hashKeyValue, rangeKeyValue);
            }
        }
    }
}
