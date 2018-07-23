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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hbase", "scan", "fetch", "get", "enrich"})
@CapabilityDescription("Fetches a row from an HBase table. The Destination property controls whether the cells are added as flow file attributes, " +
        "or the row is written to the flow file content as JSON. This processor may be used to fetch a fixed row on a interval by specifying the " +
        "table and row id directly in the processor, or it may be used to dynamically fetch rows by referencing the table and row id from " +
        "incoming flow files.")
@WritesAttributes({
        @WritesAttribute(attribute = "hbase.table", description = "The name of the HBase table that the row was fetched from"),
        @WritesAttribute(attribute = "hbase.row", description = "A JSON document representing the row. This property is only written when a Destination of flowfile-attributes is selected."),
        @WritesAttribute(attribute = "mime.type", description = "Set to application/json when using a Destination of flowfile-content, not set or modified otherwise")
})
public class FetchHBaseRow extends AbstractProcessor implements VisibilityFetchSupport {

    static final Pattern COLUMNS_PATTERN = Pattern.compile("\\w+(:\\w+)?(?:,\\w+(:\\w+)?)*");

    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the HBase Table to fetch from.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ROW_ID = new PropertyDescriptor.Builder()
            .name("Row Identifier")
            .description("The identifier of the row to fetch.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor COLUMNS = new PropertyDescriptor.Builder()
            .name("Columns")
            .description("An optional comma-separated list of \"<colFamily>:<colQualifier>\" pairs to fetch. To return all columns " +
                    "for a given family, leave off the qualifier such as \"<colFamily1>,<colFamily2>\".")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createRegexMatchingValidator(COLUMNS_PATTERN))
            .build();

    static final AllowableValue DESTINATION_ATTRIBUTES = new AllowableValue("flowfile-attributes", "flowfile-attributes",
            "Adds the JSON document representing the row that was fetched as an attribute named hbase.row. " +
                    "The format of the JSON document is determined by the JSON Format property. " +
                    "NOTE: Fetching many large rows into attributes may have a negative impact on performance.");

    static final AllowableValue DESTINATION_CONTENT = new AllowableValue("flowfile-content", "flowfile-content",
            "Overwrites the FlowFile content with a JSON document representing the row that was fetched. " +
                    "The format of the JSON document is determined by the JSON Format property.");

    static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether the row fetched from HBase is written to FlowFile content or FlowFile Attributes.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTES, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_ATTRIBUTES.getValue())
            .build();

    static final AllowableValue JSON_FORMAT_FULL_ROW = new AllowableValue("full-row", "full-row",
            "Creates a JSON document with the format: {\"row\":<row-id>, \"cells\":[{\"fam\":<col-fam>, \"qual\":<col-val>, \"val\":<value>, \"ts\":<timestamp>}]}.");
    static final AllowableValue JSON_FORMAT_QUALIFIER_AND_VALUE = new AllowableValue("col-qual-and-val", "col-qual-and-val",
            "Creates a JSON document with the format: {\"<col-qual>\":\"<value>\", \"<col-qual>\":\"<value>\".");

    static final PropertyDescriptor JSON_FORMAT = new PropertyDescriptor.Builder()
            .name("JSON Format")
            .description("Specifies how to represent the HBase row as a JSON document.")
            .required(true)
            .allowableValues(JSON_FORMAT_FULL_ROW, JSON_FORMAT_QUALIFIER_AND_VALUE)
            .defaultValue(JSON_FORMAT_FULL_ROW.getValue())
            .build();

    static final AllowableValue ENCODING_NONE = new AllowableValue("none", "none", "Creates a String using the bytes of given data and the given Character Set.");
    static final AllowableValue ENCODING_BASE64 = new AllowableValue("base64", "base64", "Creates a Base64 encoded String of the given data.");

    static final PropertyDescriptor JSON_VALUE_ENCODING = new PropertyDescriptor.Builder()
            .name("JSON Value Encoding")
            .description("Specifies how to represent row ids, column families, column qualifiers, and values when stored in FlowFile attributes, or written to JSON.")
            .required(true)
            .allowableValues(ENCODING_NONE, ENCODING_BASE64)
            .defaultValue(ENCODING_NONE.getValue())
            .build();

    static final PropertyDescriptor DECODE_CHARSET = new PropertyDescriptor.Builder()
            .name("Decode Character Set")
            .description("The character set used to decode data from HBase.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final PropertyDescriptor ENCODE_CHARSET = new PropertyDescriptor.Builder()
            .name("Encode Character Set")
            .description("The character set used to encode the JSON representation of the row.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successful fetches are routed to this relationship.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All failed fetches are routed to this relationship.")
            .build();
    static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("All fetches where the row id is not found are routed to this relationship.")
            .build();

    static final String HBASE_TABLE_ATTR = "hbase.table";
    static final String HBASE_ROW_ATTR = "hbase.row";

    static final List<PropertyDescriptor> properties;
    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HBASE_CLIENT_SERVICE);
        props.add(TABLE_NAME);
        props.add(ROW_ID);
        props.add(COLUMNS);
        props.add(AUTHORIZATIONS);
        props.add(DESTINATION);
        props.add(JSON_FORMAT);
        props.add(JSON_VALUE_ENCODING);
        props.add(ENCODE_CHARSET);
        props.add(DECODE_CHARSET);
        properties = Collections.unmodifiableList(props);
    }

    static final Set<Relationship> relationships;
    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        rels.add(REL_NOT_FOUND);
        relationships = Collections.unmodifiableSet(rels);
    }

    private volatile Charset decodeCharset;
    private volatile Charset encodeCharset;
    private volatile RowSerializer regularRowSerializer;
    private volatile RowSerializer base64RowSerializer;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.decodeCharset = Charset.forName(context.getProperty(DECODE_CHARSET).getValue());
        this.encodeCharset = Charset.forName(context.getProperty(ENCODE_CHARSET).getValue());

        final String jsonFormat = context.getProperty(JSON_FORMAT).getValue();
        if (jsonFormat.equals(JSON_FORMAT_FULL_ROW.getValue())) {
            this.regularRowSerializer = new JsonFullRowSerializer(decodeCharset, encodeCharset);
            this.base64RowSerializer = new JsonFullRowSerializer(decodeCharset, encodeCharset, true);
        } else {
            this.regularRowSerializer = new JsonQualifierAndValueRowSerializer(decodeCharset, encodeCharset);
            this.base64RowSerializer = new JsonQualifierAndValueRowSerializer(decodeCharset, encodeCharset, true);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(tableName)) {
            getLogger().error("Table Name is blank or null for {}, transferring to failure", new Object[] {flowFile});
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        final String rowId = context.getProperty(ROW_ID).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(rowId)) {
            getLogger().error("Row Identifier is blank or null for {}, transferring to failure", new Object[] {flowFile});
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        final List<Column> columns = getColumns(context.getProperty(COLUMNS).evaluateAttributeExpressions(flowFile).getValue());
        final HBaseClientService hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
        final String destination = context.getProperty(DESTINATION).getValue();
        final boolean base64Encode = context.getProperty(JSON_VALUE_ENCODING).getValue().equals(ENCODING_BASE64.getValue());

        List<String> authorizations = getAuthorizations(context, flowFile);

        final RowSerializer rowSerializer = base64Encode ? base64RowSerializer : regularRowSerializer;

        final FetchHBaseRowHandler handler = destination.equals(DESTINATION_CONTENT.getValue())
                ? new FlowFileContentHandler(flowFile, session, rowSerializer) : new FlowFileAttributeHandler(flowFile, session, rowSerializer);

        final byte[] rowIdBytes = rowId.getBytes(StandardCharsets.UTF_8);

        try {
            hBaseClientService.scan(tableName, rowIdBytes, rowIdBytes, columns, authorizations, handler);
        } catch (Exception e) {
            getLogger().error("Unable to fetch row {} from  {} due to {}", new Object[] {rowId, tableName, e});
            session.transfer(handler.getFlowFile(), REL_FAILURE);
            return;
        }

        FlowFile handlerFlowFile = handler.getFlowFile();
        if (!handler.handledRow()) {
            getLogger().debug("Row {} not found in {}, transferring to not found", new Object[] {rowId, tableName});
            session.transfer(handlerFlowFile, REL_NOT_FOUND);
            return;
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Fetched {} from {} with row id {}", new Object[]{handlerFlowFile, tableName, rowId});
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(HBASE_TABLE_ATTR, tableName);
        if (destination.equals(DESTINATION_CONTENT.getValue())) {
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        }

        handlerFlowFile = session.putAllAttributes(handlerFlowFile, attributes);

        final String transitUri = hBaseClientService.toTransitUri(tableName, rowId);
        // Regardless to where the result is written to, emit a fetch event.
        session.getProvenanceReporter().fetch(handlerFlowFile, transitUri);
        if (!destination.equals(DESTINATION_CONTENT.getValue())) {
            session.getProvenanceReporter().modifyAttributes(handlerFlowFile, "Added attributes to FlowFile from " + transitUri);
        }

        session.transfer(handlerFlowFile, REL_SUCCESS);
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
     * A ResultHandler that also provides access to a resulting FlowFile reference.
     */
    private interface FetchHBaseRowHandler extends ResultHandler {

        /**
         * @return returns the flow file reference that was used by this handler
         */
        FlowFile getFlowFile();

        /**
         * @return returns true if this handler handled a row
         */
        boolean handledRow();

    }

    /**
     * A FetchHBaseRowHandler that writes the resulting row to the FlowFile content.
     */
    private static class FlowFileContentHandler implements FetchHBaseRowHandler {

        private FlowFile flowFile;
        private final ProcessSession session;
        private final RowSerializer serializer;
        private boolean handledRow = false;

        public FlowFileContentHandler(final FlowFile flowFile, final ProcessSession session, final RowSerializer serializer) {
            this.flowFile = flowFile;
            this.session = session;
            this.serializer = serializer;
        }

        @Override
        public void handle(byte[] row, ResultCell[] resultCells) {
            flowFile = session.write(flowFile, (out) -> {
                serializer.serialize(row, resultCells, out);
            });
            handledRow = true;
        }

        @Override
        public FlowFile getFlowFile() {
            return flowFile;
        }

        @Override
        public boolean handledRow() {
            return handledRow;
        }
    }

    /**
     * A FetchHBaseRowHandler that writes the resulting row to FlowFile attributes.
     */
    private static class FlowFileAttributeHandler implements FetchHBaseRowHandler {

        private FlowFile flowFile;
        private final ProcessSession session;
        private final RowSerializer rowSerializer;
        private boolean handledRow = false;

        public FlowFileAttributeHandler(final FlowFile flowFile, final ProcessSession session, final RowSerializer serializer) {
            this.flowFile = flowFile;
            this.session = session;
            this.rowSerializer = serializer;
        }

        @Override
        public void handle(byte[] row, ResultCell[] resultCells) {
            final String serializedRow = rowSerializer.serialize(row, resultCells);
            flowFile = session.putAttribute(flowFile, HBASE_ROW_ATTR, serializedRow);
            handledRow = true;
        }

        @Override
        public FlowFile getFlowFile() {
            return flowFile;
        }

        @Override
        public boolean handledRow() {
            return handledRow;
        }

    }
}
