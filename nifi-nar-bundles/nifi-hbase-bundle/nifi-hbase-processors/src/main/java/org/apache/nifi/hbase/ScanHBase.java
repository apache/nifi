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
package org.apache.nifi.hbase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.hbase.io.JsonFullRowSerializer;
import org.apache.nifi.hbase.io.JsonQualifierAndValueRowSerializer;
import org.apache.nifi.hbase.io.RowSerializer;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hbase", "scan", "fetch", "get"})
@CapabilityDescription("Scans and fetches rows from an HBase table. This processor may be used to fetch rows from hbase table by specifying a range of rowkey values (start and/or end ),"
        + "by time range, by filter expression, or any combination of them. "
        + "Order of records can be controlled by a property Reversed"
        + "Number of rows retrieved by the processor can be limited.")
@WritesAttributes({
        @WritesAttribute(attribute = "hbase.table", description = "The name of the HBase table that the row was fetched from"),
        @WritesAttribute(attribute = "mime.type", description = "Set to application/json when using a Destination of flowfile-content, not set or modified otherwise"),
        @WritesAttribute(attribute = "hbase.rows.count", description = "Number of rows in the content of given flow file"),
        @WritesAttribute(attribute = "scanhbase.results.found", description = "Indicates whether at least one row has been found in given hbase table with provided conditions. "
                + "Could be null (not present) if transfered to FAILURE")
})

public class ScanHBase extends AbstractProcessor implements VisibilityFetchSupport {
    //enhanced regex for columns to allow "-" in column qualifier names
    static final Pattern COLUMNS_PATTERN = Pattern.compile("\\w+(:(\\w|-)+)?(?:,\\w+(:(\\w|-)+)?)*");
    static final String nl = System.lineSeparator();

    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .displayName("HBase Client Service")
            .name("scanhbase-client-service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .displayName("Table Name")
            .name("scanhbase-table-name")
            .description("The name of the HBase Table to fetch from.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor START_ROW = new PropertyDescriptor.Builder()
            .displayName("Start rowkey")
            .name("scanhbase-start-rowkey")
            .description("The rowkey to start scan from.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor END_ROW = new PropertyDescriptor.Builder()
            .displayName("End rowkey")
            .name("scanhbase-end-rowkey")
            .description("The row key to end scan by.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TIME_RANGE_MIN = new PropertyDescriptor.Builder()
            .displayName("Time range min")
            .name("scanhbase-time-range-min")
            .description("Time range min value. Both min and max values for time range should be either blank or provided.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    static final PropertyDescriptor TIME_RANGE_MAX = new PropertyDescriptor.Builder()
            .displayName("Time range max")
            .name("scanhbase-time-range-max")
            .description("Time range max value. Both min and max values for time range should be either blank or provided.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    static final PropertyDescriptor LIMIT_ROWS = new PropertyDescriptor.Builder()
            .displayName("Limit rows")
            .name("scanhbase-limit")
            .description("Limit number of rows retrieved by scan.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor BULK_SIZE = new PropertyDescriptor.Builder()
            .displayName("Max rows per flow file")
            .name("scanhbase-bulk-size")
            .description("Limits number of rows in single flow file content. Set to 0 to avoid multiple flow files.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor REVERSED_SCAN = new PropertyDescriptor.Builder()
            .displayName("Reversed order")
            .name("scanhbase-reversed-order")
            .description("Set whether this scan is a reversed one. This is false by default which means forward(normal) scan.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_EXPRESSION = new PropertyDescriptor.Builder()
            .displayName("Filter expression")
            .name("scanhbase-filter-expression")
            .description("An HBase filter expression that will be applied to the scan. This property can not be used when also using the Columns property. "
                    + "Example: \"ValueFilter( =, 'binaryprefix:commit' )\"")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor COLUMNS = new PropertyDescriptor.Builder()
            .displayName("Columns")
            .name("scanhbase-columns")
            .description("An optional comma-separated list of \"<colFamily>:<colQualifier>\" pairs to fetch. To return all columns " +
                    "for a given family, leave off the qualifier such as \"<colFamily1>,<colFamily2>\".")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createRegexMatchingValidator(COLUMNS_PATTERN))
            .build();

    static final AllowableValue JSON_FORMAT_FULL_ROW = new AllowableValue("full-row", "full-row",
            "Creates a JSON document with the format: {\"row\":<row-id>, \"cells\":[{\"fam\":<col-fam>, \"qual\":<col-val>, \"val\":<value>, \"ts\":<timestamp>}]}.");
    static final AllowableValue JSON_FORMAT_QUALIFIER_AND_VALUE = new AllowableValue("col-qual-and-val", "col-qual-and-val",
            "Creates a JSON document with the format: {\"<col-qual>\":\"<value>\", \"<col-qual>\":\"<value>\".");

    static final PropertyDescriptor JSON_FORMAT = new PropertyDescriptor.Builder()
            .displayName("JSON Format")
            .name("scanhbase-json-format")
            .description("Specifies how to represent the HBase row as a JSON document.")
            .required(true)
            .allowableValues(JSON_FORMAT_FULL_ROW, JSON_FORMAT_QUALIFIER_AND_VALUE)
            .defaultValue(JSON_FORMAT_FULL_ROW.getValue())
            .build();

    static final PropertyDescriptor DECODE_CHARSET = new PropertyDescriptor.Builder()
            .displayName("Decode Character Set")
            .name("scanhbase-decode-charset")
            .description("The character set used to decode data from HBase.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final PropertyDescriptor ENCODE_CHARSET = new PropertyDescriptor.Builder()
            .displayName("Encode Character Set")
            .name("scanhbase-encode-charset")
            .description("The character set used to encode the JSON representation of the row.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input file will be routed to this destination, even if no rows are retrieved based on provided conditions.")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successful fetches are routed to this relationship.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All failed fetches are routed to this relationship.")
            .build();

    static final String HBASE_TABLE_ATTR = "hbase.table";
    static final String HBASE_ROWS_COUNT_ATTR = "hbase.rows.count";

    static final List<PropertyDescriptor> properties;
    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HBASE_CLIENT_SERVICE);
        props.add(TABLE_NAME);
        props.add(AUTHORIZATIONS);
        props.add(START_ROW);
        props.add(END_ROW);
        props.add(TIME_RANGE_MIN);
        props.add(TIME_RANGE_MAX);
        props.add(LIMIT_ROWS);
        props.add(REVERSED_SCAN);
        props.add(BULK_SIZE);
        props.add(FILTER_EXPRESSION);
        props.add(COLUMNS);
        props.add(JSON_FORMAT);
        props.add(ENCODE_CHARSET);
        props.add(DECODE_CHARSET);
        properties = Collections.unmodifiableList(props);
    }

    static final Set<Relationship> relationships;
    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_ORIGINAL);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    private volatile Charset decodeCharset;
    private volatile Charset encodeCharset;
    private RowSerializer serializer = null;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.decodeCharset = Charset.forName(context.getProperty(DECODE_CHARSET).getValue());
        this.encodeCharset = Charset.forName(context.getProperty(ENCODE_CHARSET).getValue());

        final String jsonFormat = context.getProperty(JSON_FORMAT).getValue();
        if (jsonFormat.equals(JSON_FORMAT_FULL_ROW.getValue())) {
            this.serializer = new JsonFullRowSerializer(decodeCharset, encodeCharset);
        } else {
            this.serializer = new JsonQualifierAndValueRowSerializer(decodeCharset, encodeCharset);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {

        final List<ValidationResult> problems = new ArrayList<>();

        final String columns = validationContext.getProperty(COLUMNS).getValue();
        final String filter = validationContext.getProperty(FILTER_EXPRESSION).getValue();

        if (!StringUtils.isBlank(columns) && !StringUtils.isBlank(filter)) {
            problems.add(new ValidationResult.Builder()
                    .subject(FILTER_EXPRESSION.getDisplayName())
                    .input(filter).valid(false)
                    .explanation("A filter expression can not be used in conjunction with the Columns property")
                    .build());
        }

        String minTS = validationContext.getProperty(TIME_RANGE_MIN).getValue();
        String maxTS = validationContext.getProperty(TIME_RANGE_MAX).getValue();
        if ( (!StringUtils.isBlank(minTS) && StringUtils.isBlank(maxTS)) || (StringUtils.isBlank(minTS) && !StringUtils.isBlank(maxTS))){
            problems.add(new ValidationResult.Builder()
                    .subject(TIME_RANGE_MAX.getDisplayName())
                    .input(maxTS).valid(false)
                    .explanation(String.format("%s and %s both should be either empty or provided", TIME_RANGE_MIN, TIME_RANGE_MAX))
                    .build());
        }

        return problems;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try{
            final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
            if (StringUtils.isBlank(tableName)) {
                getLogger().error("Table Name is blank or null for {}, transferring to failure", new Object[] {flowFile});
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }

            final List<String> authorizations = getAuthorizations(context, flowFile);

            final String startRow = context.getProperty(START_ROW).evaluateAttributeExpressions(flowFile).getValue();
            final String endRow = context.getProperty(END_ROW).evaluateAttributeExpressions(flowFile).getValue();

            final String filterExpression = context.getProperty(FILTER_EXPRESSION).evaluateAttributeExpressions(flowFile).getValue();

            //evaluate and validate time range min and max values. They both should be either empty or provided.
            Long timerangeMin = null;
            Long timerangeMax = null;

            try{
                timerangeMin = context.getProperty(TIME_RANGE_MIN).evaluateAttributeExpressions(flowFile).asLong();
            }catch(Exception e){
                getLogger().error("Time range min value is not a number ({}) for {}, transferring to failure",
                        new Object[] {context.getProperty(TIME_RANGE_MIN).evaluateAttributeExpressions(flowFile).getValue(), flowFile});
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }
            try{
                timerangeMax = context.getProperty(TIME_RANGE_MAX).evaluateAttributeExpressions(flowFile).asLong();
            }catch(Exception e){
                getLogger().error("Time range max value is not a number ({}) for {}, transferring to failure",
                        new Object[] {context.getProperty(TIME_RANGE_MAX).evaluateAttributeExpressions(flowFile).getValue(), flowFile});
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }
            if (timerangeMin == null && timerangeMax != null) {
                getLogger().error("Time range min value cannot be blank when max value provided for {}, transferring to failure", new Object[] {flowFile});
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }else if (timerangeMin != null && timerangeMax == null) {
                getLogger().error("Time range max value cannot be blank when min value provided for {}, transferring to failure", new Object[] {flowFile});
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }

            final Integer limitRows = context.getProperty(LIMIT_ROWS).evaluateAttributeExpressions(flowFile).asInteger();

            final Boolean isReversed = context.getProperty(REVERSED_SCAN).asBoolean();

            final Integer bulkSize = context.getProperty(BULK_SIZE).evaluateAttributeExpressions(flowFile).asInteger();

            final List<Column> columns = getColumns(context.getProperty(COLUMNS).evaluateAttributeExpressions(flowFile).getValue());
            final HBaseClientService hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);

            final AtomicReference<Long> rowsPulledHolder = new AtomicReference<>(0L);
            final AtomicReference<Long> ffCountHolder = new AtomicReference<>(0L);
            ScanHBaseResultHandler handler = new ScanHBaseResultHandler(context, session, flowFile, rowsPulledHolder, ffCountHolder, hBaseClientService, tableName, bulkSize);
            try {
                hBaseClientService.scan(tableName,
                                        startRow, endRow,
                                        filterExpression,
                                        timerangeMin, timerangeMax,
                                        limitRows,
                                        isReversed,
                                        columns,
                                        authorizations,
                                        handler);
            } catch (Exception e) {
                if (handler.getFlowFile() != null){
                    session.remove(handler.getFlowFile());
                }
                getLogger().error("Unable to fetch rows from HBase table {} due to {}", new Object[] {tableName, e});
                flowFile = session.putAttribute(flowFile, "scanhbase.results.found", Boolean.toString(handler.isHandledAny()));
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            flowFile = session.putAttribute(flowFile, "scanhbase.results.found", Boolean.toString(handler.isHandledAny()));

            FlowFile openedFF = handler.getFlowFile();
            if (openedFF != null) {
                finalizeFlowFile(session, hBaseClientService, openedFF, tableName, handler.getRecordsCount(), null);
            }

            session.transfer(flowFile, REL_ORIGINAL);
            session.commit();

        }catch (final Exception e) {
            getLogger().error("Failed to receive data from HBase due to {}", e);
            session.rollback();
            // if we failed, we want to yield so that we don't hammer hbase.
            context.yield();
        }
    }

    /*
     * Initiates FF content, adds relevant attributes, and starts content with JSON array "["
     */
    private FlowFile initNewFlowFile(final ProcessSession session, final FlowFile origFF, final String tableName) throws IOException{

        FlowFile flowFile = session.create(origFF);
        flowFile = session.putAttribute(flowFile, HBASE_TABLE_ATTR, tableName);
        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");

        final AtomicReference<IOException> ioe = new AtomicReference<>(null);
        flowFile = session.write(flowFile, (out) -> {
            try{
                out.write("[".getBytes());
            }catch(IOException e){
                ioe.set(e);
            }
        });

        if (ioe.get() != null){
            throw ioe.get();
        }

        return flowFile;
    }

    private void finalizeFlowFile(final ProcessSession session, final HBaseClientService hBaseClientService,
            FlowFile flowFile, final String tableName, Long rowsPulled, Exception e) {
        Relationship rel = REL_SUCCESS;
        flowFile = session.putAttribute(flowFile, HBASE_ROWS_COUNT_ATTR, rowsPulled.toString());

        final AtomicReference<IOException> ioe = new AtomicReference<>(null);
        flowFile = session.append(flowFile, (out) -> {
            try{
                out.write("]".getBytes());
            }catch(IOException ei){
                ioe.set(ei);
            }
        });
        if (e != null || ioe.get() != null) {
            flowFile = session.putAttribute(flowFile, "scanhbase.error", (e==null?e:ioe.get()).toString());
            rel = REL_FAILURE;
        } else {
            session.getProvenanceReporter().receive(flowFile, hBaseClientService.toTransitUri(tableName, "{ids}"));
        }
        session.transfer(flowFile, rel);
    }

    /**
     * @param columnsValue a String in the form colFam:colQual,colFam:colQual
     * @return a list of Columns based on parsing the given String
     */
    private List<Column> getColumns(final String columnsValue) {
        final String[] columns = (columnsValue == null || columnsValue.isEmpty() ? new String[0] : columnsValue.split(","));

        List<Column> columnsList = new ArrayList<>(columns.length);

        for (final String column : columns) {
            if (column.contains(":"))  {
                final String[] parts = column.split(":");
                final byte[] cf = parts[0].getBytes(StandardCharsets.UTF_8);
                final byte[] cq = parts[1].getBytes(StandardCharsets.UTF_8);
                columnsList.add(new Column(cf, cq));
            } else {
                final byte[] cf = column.getBytes(StandardCharsets.UTF_8);
                columnsList.add(new Column(cf, null));
            }
        }

        return columnsList;
    }

    /**
     * @return number of rows to be committed to session.
     */
    protected int getBatchSize(){
        return 500;
    }

    /**
     * Result Handler for Scan operation
     */
    private class ScanHBaseResultHandler implements ResultHandler {

        final private ProcessSession session;
        final private FlowFile origFF;
        final private AtomicReference<Long> rowsPulledHolder;
        final private AtomicReference<Long> ffCountHolder;
        final private HBaseClientService hBaseClientService;
        final private String tableName;
        final private Integer bulkSize;
        private FlowFile flowFile = null;
        private byte[] JSON_ARRAY_DELIM = ",\n".getBytes();

        private boolean handledAny = false;

        ScanHBaseResultHandler(final ProcessContext context, final ProcessSession session,
                final FlowFile origFF, final AtomicReference<Long> rowsPulledHolder, final AtomicReference<Long> ffCountHolder,
                final HBaseClientService hBaseClientService, final String tableName, final Integer bulkSize){
            this.session = session;
            this.rowsPulledHolder = rowsPulledHolder;
            this.ffCountHolder = ffCountHolder;
            this.hBaseClientService = hBaseClientService;
            this.tableName = tableName;
            this.bulkSize = bulkSize == null ? 0 : bulkSize;
            this.origFF = origFF;

        }

        @Override
        public void handle(final byte[] rowKey, final ResultCell[] resultCells) {

            long rowsPulled = rowsPulledHolder.get();
            long ffUncommittedCount = ffCountHolder.get();

            try{
                if (flowFile == null){
                        flowFile = initNewFlowFile(session, origFF, tableName);
                        ffUncommittedCount++;
                }

                flowFile = session.append(flowFile, (out) -> {
                    if (rowsPulledHolder.get() > 0){
                        out.write(JSON_ARRAY_DELIM);
                    }
                    serializer.serialize(rowKey, resultCells, out);
                });
                handledAny = true;

            }catch(Exception e){
                throw new RuntimeException(e);
            }

            rowsPulled++;

            // bulkSize controls number of records per flow file.
            if (bulkSize>0 && rowsPulled >= bulkSize) {

                finalizeFlowFile(session, hBaseClientService, flowFile, tableName, rowsPulled, null);
                flowFile = null;
                rowsPulledHolder.set(0L);

                // we could potentially have a huge number of rows. If we get to batchSize, go ahead and commit the
                // session so that we can avoid buffering tons of FlowFiles without ever sending any out.
                if (getBatchSize()>0 && ffUncommittedCount*bulkSize > getBatchSize()) {
                    session.commit();
                    ffCountHolder.set(0L);
                }else{
                    ffCountHolder.set(ffUncommittedCount++);
                }
            }else{
                rowsPulledHolder.set(rowsPulled);
            }
        }

        public boolean isHandledAny(){
            return handledAny;
        }

        public FlowFile getFlowFile(){
            return flowFile;
        }

        public long getRecordsCount(){
            return rowsPulledHolder.get();
        }
    }

}
