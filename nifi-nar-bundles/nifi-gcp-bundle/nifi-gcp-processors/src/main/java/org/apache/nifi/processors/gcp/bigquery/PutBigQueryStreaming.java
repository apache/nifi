/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gcp.bigquery;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.StringUtils;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;

/**
 * A processor for streaming loading data into a Google BigQuery table. It uses the BigQuery
 * streaming insert API to insert data. This provides the lowest-latency insert path into BigQuery,
 * and therefore is the default method when the input is unbounded. BigQuery will make a strong
 * effort to ensure no duplicates when using this path, however there are some scenarios in which
 * BigQuery is unable to make this guarantee (see
 * https://cloud.google.com/bigquery/streaming-data-into-bigquery). A query can be run over the
 * output table to periodically clean these rare duplicates. Alternatively, using the Batch insert
 * method does guarantee no duplicates, though the latency for the insert into BigQuery will be much
 * higher.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "google", "google cloud", "bq", "gcp", "bigquery", "record" })
@CapabilityDescription("Load data into Google BigQuery table using the streaming API. This processor "
        + "is not intended to load large flow files as it will load the full content into memory. If "
        + "you need to insert large flow files, consider using PutBigQueryBatch instead.")
@SeeAlso({ PutBigQueryBatch.class })
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@WritesAttributes({
        @WritesAttribute(attribute = BigQueryAttributes.JOB_NB_RECORDS_ATTR, description = BigQueryAttributes.JOB_NB_RECORDS_DESC)
})
public class PutBigQueryStreaming extends AbstractBigQueryProcessor {

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.RECORD_READER_ATTR)
            .displayName("Record Reader")
            .description(BigQueryAttributes.RECORD_READER_DESC)
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor SKIP_INVALID_ROWS = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.SKIP_INVALID_ROWS_ATTR)
            .displayName("Skip Invalid Rows")
            .description(BigQueryAttributes.SKIP_INVALID_ROWS_DESC)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("false")
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor> builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(RECORD_READER)
                .add(SKIP_INVALID_ROWS)
                .build();
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String projectId = context.getProperty(PROJECT_ID).evaluateAttributeExpressions().getValue();
        final String dataset = context.getProperty(DATASET).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        final TableId tableId;
        if (StringUtils.isEmpty(projectId)) {
            tableId = TableId.of(dataset, tableName);
        } else {
            tableId = TableId.of(projectId, dataset, tableName);
        }

        try {

            InsertAllRequest.Builder request = InsertAllRequest.newBuilder(tableId);
            int nbrecord = 0;

            try (final InputStream in = session.read(flowFile)) {
                final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
                try (final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());) {
                    Record currentRecord;
                    while ((currentRecord = reader.nextRecord()) != null) {
                        request.addRow(convertMapRecord(currentRecord.toMap()));
                        nbrecord++;
                    }
                }
            }

            request.setIgnoreUnknownValues(context.getProperty(IGNORE_UNKNOWN).evaluateAttributeExpressions(flowFile).asBoolean());
            request.setSkipInvalidRows(context.getProperty(SKIP_INVALID_ROWS).evaluateAttributeExpressions(flowFile).asBoolean());

            InsertAllResponse response = getCloudService().insertAll(request.build());

            final Map<String, String> attributes = new HashMap<>();

            if (response.hasErrors()) {
                getLogger().log(LogLevel.WARN, "Failed to insert {} of {} records into BigQuery {} table.", new Object[] { response.getInsertErrors().size(), nbrecord, tableName });
                if (getLogger().isDebugEnabled()) {
                    for (long index : response.getInsertErrors().keySet()) {
                        for (BigQueryError e : response.getInsertErrors().get(index)) {
                            getLogger().log(LogLevel.DEBUG, "Failed to insert record #{}: {}", new Object[] { index, e.getMessage() });
                        }
                    }
                }

                attributes.put(BigQueryAttributes.JOB_NB_RECORDS_ATTR, Long.toString(nbrecord - response.getInsertErrors().size()));

                flowFile = session.penalize(flowFile);
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_FAILURE);
            } else {
                attributes.put(BigQueryAttributes.JOB_NB_RECORDS_ATTR, Long.toString(nbrecord));
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_SUCCESS);
            }

        } catch (Exception ex) {
            getLogger().log(LogLevel.ERROR, ex.getMessage(), ex);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private Map<String, Object> convertMapRecord(Map<String, Object> map) {
        Map<String, Object> result = new HashMap<String, Object>();
        for (String key : map.keySet()) {
            Object obj = map.get(key);
            if (obj instanceof MapRecord) {
                result.put(key, convertMapRecord(((MapRecord) obj).toMap()));
            } else if (obj instanceof Object[]
                    && ((Object[]) obj).length > 0
                    && ((Object[]) obj)[0] instanceof MapRecord) {
                List<Map<String, Object>> lmapr = new ArrayList<Map<String, Object>>();
                for (Object mapr : ((Object[]) obj)) {
                    lmapr.add(convertMapRecord(((MapRecord) mapr).toMap()));
                }
                result.put(key, lmapr);
            } else {
                result.put(key, obj);
            }
        }
        return result;
    }

}