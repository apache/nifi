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

package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.common.collect.ImmutableList;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.storage.DeleteGCSObject;
import org.apache.nifi.processors.gcp.storage.PutGCSObject;
import org.apache.nifi.util.StringUtils;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A processor for batch loading data into a Google BigQuery table
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Batch loads flow files content to a Google BigQuery table.")
@SeeAlso({PutGCSObject.class, DeleteGCSObject.class})
@WritesAttributes({
    @WritesAttribute(attribute = BigQueryAttributes.DATASET_ATTR, description = BigQueryAttributes.DATASET_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.TABLE_NAME_ATTR, description = BigQueryAttributes.TABLE_NAME_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.TABLE_SCHEMA_ATTR, description = BigQueryAttributes.TABLE_SCHEMA_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.SOURCE_TYPE_ATTR, description = BigQueryAttributes.SOURCE_TYPE_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.IGNORE_UNKNOWN_ATTR, description = BigQueryAttributes.IGNORE_UNKNOWN_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.CREATE_DISPOSITION_ATTR, description = BigQueryAttributes.CREATE_DISPOSITION_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.WRITE_DISPOSITION_ATTR, description = BigQueryAttributes.WRITE_DISPOSITION_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.MAX_BADRECORDS_ATTR, description = BigQueryAttributes.MAX_BADRECORDS_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.JOB_CREATE_TIME_ATTR, description = BigQueryAttributes.JOB_CREATE_TIME_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.JOB_END_TIME_ATTR, description = BigQueryAttributes.JOB_END_TIME_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.JOB_START_TIME_ATTR, description = BigQueryAttributes.JOB_START_TIME_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.JOB_LINK_ATTR, description = BigQueryAttributes.JOB_LINK_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.JOB_ERROR_MSG_ATTR, description = BigQueryAttributes.JOB_ERROR_MSG_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.JOB_ERROR_REASON_ATTR, description = BigQueryAttributes.JOB_ERROR_REASON_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.JOB_ERROR_LOCATION_ATTR, description = BigQueryAttributes.JOB_ERROR_LOCATION_DESC),
    @WritesAttribute(attribute = BigQueryAttributes.JOB_NB_RECORDS_ATTR, description = BigQueryAttributes.JOB_NB_RECORDS_DESC)
})
public class PutBigQueryBatch extends AbstractBigQueryProcessor {

    private static final List<String> TYPES = Arrays.asList(FormatOptions.json().getType(), FormatOptions.csv().getType(), FormatOptions.avro().getType());

    private static final Validator FORMAT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject(subject).input(input);
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return builder.valid(true).explanation("Contains Expression Language").build();
            }

            if(TYPES.contains(input.toUpperCase())) {
                builder.valid(true);
            } else {
                builder.valid(false).explanation("Load File Type must be one of the following options: " + StringUtils.join(TYPES, ", "));
            }

            return builder.build();
        }
    };

    public static final PropertyDescriptor SOURCE_TYPE = new PropertyDescriptor
            .Builder().name(BigQueryAttributes.SOURCE_TYPE_ATTR)
            .displayName("Load file type")
            .description(BigQueryAttributes.SOURCE_TYPE_DESC)
            .required(true)
            .addValidator(FORMAT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IGNORE_UNKNOWN = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.IGNORE_UNKNOWN_ATTR)
            .displayName("Ignore Unknown Values")
            .description(BigQueryAttributes.IGNORE_UNKNOWN_DESC)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor CREATE_DISPOSITION = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CREATE_DISPOSITION_ATTR)
            .displayName("Create Disposition")
            .description(BigQueryAttributes.CREATE_DISPOSITION_DESC)
            .required(true)
            .allowableValues(BigQueryAttributes.CREATE_IF_NEEDED, BigQueryAttributes.CREATE_NEVER)
            .defaultValue(BigQueryAttributes.CREATE_IF_NEEDED.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WRITE_DISPOSITION = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.WRITE_DISPOSITION_ATTR)
            .displayName("Write Disposition")
            .description(BigQueryAttributes.WRITE_DISPOSITION_DESC)
            .required(true)
            .allowableValues(BigQueryAttributes.WRITE_EMPTY, BigQueryAttributes.WRITE_APPEND, BigQueryAttributes.WRITE_TRUNCATE)
            .defaultValue(BigQueryAttributes.WRITE_EMPTY.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAXBAD_RECORDS = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.MAX_BADRECORDS_ATTR)
            .displayName("Max Bad Records")
            .description(BigQueryAttributes.MAX_BADRECORDS_DESC)
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CSV_ALLOW_JAGGED_ROWS = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_ALLOW_JAGGED_ROWS_ATTR)
            .displayName("CSV Input - Allow Jagged Rows")
            .description(BigQueryAttributes.CSV_ALLOW_JAGGED_ROWS_DESC)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor CSV_ALLOW_QUOTED_NEW_LINES = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_ALLOW_QUOTED_NEW_LINES_ATTR)
            .displayName("CSV Input - Allow Quoted New Lines")
            .description(BigQueryAttributes.CSV_ALLOW_QUOTED_NEW_LINES_DESC)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor CSV_CHARSET = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_CHARSET_ATTR)
            .displayName("CSV Input - Character Set")
            .description(BigQueryAttributes.CSV_CHARSET_DESC)
            .required(true)
            .allowableValues("UTF-8", "ISO-8859-1")
            .defaultValue("UTF-8")
            .build();

    public static final PropertyDescriptor CSV_FIELD_DELIMITER = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_FIELD_DELIMITER_ATTR)
            .displayName("CSV Input - Field Delimiter")
            .description(BigQueryAttributes.CSV_FIELD_DELIMITER_DESC)
            .required(true)
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor CSV_QUOTE = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_QUOTE_ATTR)
            .displayName("CSV Input - Quote")
            .description(BigQueryAttributes.CSV_QUOTE_DESC)
            .required(true)
            .defaultValue("\"")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor CSV_SKIP_LEADING_ROWS = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_SKIP_LEADING_ROWS_ATTR)
            .displayName("CSV Input - Skip Leading Rows")
            .description(BigQueryAttributes.CSV_SKIP_LEADING_ROWS_DESC)
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(SOURCE_TYPE)
                .add(CREATE_DISPOSITION)
                .add(WRITE_DISPOSITION)
                .add(MAXBAD_RECORDS)
                .add(IGNORE_UNKNOWN)
                .add(CSV_ALLOW_JAGGED_ROWS)
                .add(CSV_ALLOW_QUOTED_NEW_LINES)
                .add(CSV_CHARSET)
                .add(CSV_FIELD_DELIMITER)
                .add(CSV_QUOTE)
                .add(CSV_SKIP_LEADING_ROWS)
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
        final String type = context.getProperty(SOURCE_TYPE).evaluateAttributeExpressions(flowFile).getValue();

        final TableId tableId;
        if (StringUtils.isEmpty(projectId)) {
            tableId = TableId.of(dataset, tableName);
        } else {
            tableId = TableId.of(projectId, dataset, tableName);
        }

        try {

            FormatOptions formatOption;

            if(type.equals(FormatOptions.csv().getType())) {
                formatOption = FormatOptions.csv().toBuilder()
                        .setAllowJaggedRows(context.getProperty(CSV_ALLOW_JAGGED_ROWS).asBoolean())
                        .setAllowQuotedNewLines(context.getProperty(CSV_ALLOW_QUOTED_NEW_LINES).asBoolean())
                        .setEncoding(context.getProperty(CSV_CHARSET).getValue())
                        .setFieldDelimiter(context.getProperty(CSV_FIELD_DELIMITER).evaluateAttributeExpressions(flowFile).getValue())
                        .setQuote(context.getProperty(CSV_QUOTE).evaluateAttributeExpressions(flowFile).getValue())
                        .setSkipLeadingRows(context.getProperty(CSV_SKIP_LEADING_ROWS).evaluateAttributeExpressions(flowFile).asInteger())
                        .build();
            } else {
                formatOption = FormatOptions.of(type);
            }

            final Schema schema = BigQueryUtils.schemaFromString(context.getProperty(TABLE_SCHEMA).evaluateAttributeExpressions(flowFile).getValue());
            final WriteChannelConfiguration writeChannelConfiguration =
                    WriteChannelConfiguration.newBuilder(tableId)
                    .setCreateDisposition(JobInfo.CreateDisposition.valueOf(context.getProperty(CREATE_DISPOSITION).getValue()))
                    .setWriteDisposition(JobInfo.WriteDisposition.valueOf(context.getProperty(WRITE_DISPOSITION).getValue()))
                    .setIgnoreUnknownValues(context.getProperty(IGNORE_UNKNOWN).asBoolean())
                    .setMaxBadRecords(context.getProperty(MAXBAD_RECORDS).asInteger())
                    .setSchema(schema)
                    .setFormatOptions(formatOption)
                    .build();

            try ( TableDataWriteChannel writer = getCloudService().writer(writeChannelConfiguration) ) {

                session.read(flowFile, rawIn -> {
                    ReadableByteChannel readableByteChannel = Channels.newChannel(rawIn);
                    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
                    while (readableByteChannel.read(byteBuffer) >= 0) {
                        byteBuffer.flip();
                        writer.write(byteBuffer);
                        byteBuffer.clear();
                    }
                });

                // writer must be closed to get the job
                writer.close();

                Job job = writer.getJob();
                Long timePeriod = context.getProperty(READ_TIMEOUT).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.SECONDS);
                Duration waitFor = Duration.of(timePeriod, ChronoUnit.SECONDS);
                job = job.waitFor(RetryOption.totalTimeout(waitFor));

                if (job != null) {
                    final Map<String, String> attributes = new HashMap<>();

                    attributes.put(BigQueryAttributes.JOB_CREATE_TIME_ATTR, Long.toString(job.getStatistics().getCreationTime()));
                    attributes.put(BigQueryAttributes.JOB_END_TIME_ATTR, Long.toString(job.getStatistics().getEndTime()));
                    attributes.put(BigQueryAttributes.JOB_START_TIME_ATTR, Long.toString(job.getStatistics().getStartTime()));
                    attributes.put(BigQueryAttributes.JOB_LINK_ATTR, job.getSelfLink());

                    boolean jobError = (job.getStatus().getError() != null);

                    if (jobError) {
                        attributes.put(BigQueryAttributes.JOB_ERROR_MSG_ATTR, job.getStatus().getError().getMessage());
                        attributes.put(BigQueryAttributes.JOB_ERROR_REASON_ATTR, job.getStatus().getError().getReason());
                        attributes.put(BigQueryAttributes.JOB_ERROR_LOCATION_ATTR, job.getStatus().getError().getLocation());
                    } else {
                        // in case it got looped back from error
                        flowFile = session.removeAttribute(flowFile, BigQueryAttributes.JOB_ERROR_MSG_ATTR);
                        flowFile = session.removeAttribute(flowFile, BigQueryAttributes.JOB_ERROR_REASON_ATTR);
                        flowFile = session.removeAttribute(flowFile, BigQueryAttributes.JOB_ERROR_LOCATION_ATTR);

                        // add the number of records successfully added
                        if(job.getStatistics() instanceof LoadStatistics) {
                            final LoadStatistics stats = (LoadStatistics) job.getStatistics();
                            attributes.put(BigQueryAttributes.JOB_NB_RECORDS_ATTR, Long.toString(stats.getOutputRows()));
                        }
                    }

                    if (!attributes.isEmpty()) {
                        flowFile = session.putAllAttributes(flowFile, attributes);
                    }

                    if (jobError) {
                        getLogger().log(LogLevel.WARN, job.getStatus().getError().getMessage());
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                    } else {
                        session.getProvenanceReporter().send(flowFile, job.getSelfLink(), job.getStatistics().getEndTime() - job.getStatistics().getStartTime());
                        session.transfer(flowFile, REL_SUCCESS);
                    }
                }
            }

        } catch (Exception ex) {
            getLogger().log(LogLevel.ERROR, ex.getMessage(), ex);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}