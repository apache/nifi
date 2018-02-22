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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

@WritesAttributes(
    value = {
        @WritesAttribute( attribute = "restart.index", description = "If a delete batch fails, 'restart.index' attribute is added to the FlowFile and sent to 'failure' " +
                "relationship, so that this processor can retry from there when the same FlowFile is routed again." ),
        @WritesAttribute( attribute = "rowkey.start", description = "The first rowkey in the flowfile. Only written when using the flowfile's content for the row IDs."),
        @WritesAttribute( attribute = "rowkey.end", description = "The last rowkey in the flowfile. Only written when using the flowfile's content for the row IDs.")
    }
)
@Tags({ "delete", "hbase" })
@CapabilityDescription(
        "Delete HBase records individually or in batches. The input can be a single row ID in the flowfile content, one ID per line, " +
        "row IDs separated by a configurable separator character (default is a comma). ")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class DeleteHBaseRow extends AbstractDeleteHBase {
    static final AllowableValue ROW_ID_CONTENT = new AllowableValue("content", "FlowFile content", "Get the row key(s) from the flowfile content.");
    static final AllowableValue ROW_ID_ATTR = new AllowableValue("attr", "FlowFile attributes", "Get the row key from an expression language statement.");

    static final String RESTART_INDEX = "restart.index";
    static final String ROWKEY_START = "rowkey.start";
    static final String ROWKEY_END   = "rowkey.end";

    static final PropertyDescriptor ROW_ID_LOCATION = new PropertyDescriptor.Builder()
            .name("delete-hb-row-id-location")
            .displayName("Row ID Location")
            .description("The location of the row ID to use for building the delete. Can be from the content or an expression language statement.")
            .required(true)
            .defaultValue(ROW_ID_CONTENT.getValue())
            .allowableValues(ROW_ID_CONTENT, ROW_ID_ATTR)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor FLOWFILE_FETCH_COUNT = new PropertyDescriptor.Builder()
            .name("delete-hb-flowfile-fetch-count")
            .displayName("Flowfile Fetch Count")
            .description("The number of flowfiles to fetch per run.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("delete-hb-batch-size")
            .displayName("Batch Size")
            .description("The number of deletes to send per batch.")
            .required(true)
            .defaultValue("50")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor KEY_SEPARATOR = new PropertyDescriptor.Builder()
            .name("delete-hb-separator")
            .displayName("Delete Row Key Separator")
            .description("The separator character(s) that separate multiple row keys " +
                    "when multiple row keys are provided in the flowfile content")
            .required(true)
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("delete-char-set")
            .displayName("Character Set")
            .description("The character set used to encode the row key for HBase.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    static final PropertyDescriptor VISIBLITY_LABEL = new PropertyDescriptor.Builder()
            .name("delete-visibility-label")
            .displayName("Visibility Label")
            .description("If visibility labels are enabled, a row cannot be deleted without supplying its visibility label(s) in the delete " +
                    "request. Note: this visibility label will be applied to all cells within the row that is specified. If some cells have " +
                    "different visibility labels, they will not be deleted. When that happens, the failure to delete will be considered a success " +
                    "because HBase does not report it as a failure.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(ROW_ID_LOCATION);
        properties.add(FLOWFILE_FETCH_COUNT);
        properties.add(BATCH_SIZE);
        properties.add(KEY_SEPARATOR);
        properties.add(VISIBLITY_LABEL);
        properties.add(CHARSET);

        return properties;
    }

    @Override
    protected void doDelete(ProcessContext context, ProcessSession session) throws Exception {
        final int batchSize      = context.getProperty(BATCH_SIZE).asInteger();
        final String location    = context.getProperty(ROW_ID_LOCATION).getValue();
        final int flowFileCount  = context.getProperty(FLOWFILE_FETCH_COUNT).asInteger();
        final String charset     = context.getProperty(CHARSET).getValue();
        List<FlowFile> flowFiles = session.get(flowFileCount);

        if (flowFiles != null && flowFiles.size() > 0) {
            for (int index = 0; index < flowFiles.size(); index++) {
                FlowFile flowFile = flowFiles.get(index);
                final String visibility  = context.getProperty(VISIBLITY_LABEL).isSet()
                        ? context.getProperty(VISIBLITY_LABEL).evaluateAttributeExpressions(flowFile).getValue() : null;
                final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
                try {
                    if (location.equals(ROW_ID_CONTENT.getValue())) {
                        flowFile = doDeleteFromContent(flowFile, context, session, tableName, batchSize, charset, visibility);
                        if (flowFile.getAttribute(RESTART_INDEX) != null) {
                            session.transfer(flowFile, REL_FAILURE);
                        } else {
                            final String transitUrl = clientService.toTransitUri(tableName, flowFile.getAttribute(ROWKEY_END));
                            session.transfer(flowFile, REL_SUCCESS);
                            session.getProvenanceReporter().invokeRemoteProcess(flowFile, transitUrl);
                        }
                    } else {
                        String transitUrl = doDeleteFromAttribute(flowFile, context, tableName, charset, visibility);
                        session.transfer(flowFile, REL_SUCCESS);
                        session.getProvenanceReporter().invokeRemoteProcess(flowFile, transitUrl);
                    }
                } catch (Exception ex) {
                    getLogger().error(ex.getMessage(), ex);
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
        }
    }

    private String doDeleteFromAttribute(FlowFile flowFile, ProcessContext context, String tableName, String charset, String visibility) throws Exception {
        String rowKey = context.getProperty(ROW_ID).evaluateAttributeExpressions(flowFile).getValue();
        clientService.delete(tableName, rowKey.getBytes(charset), visibility);

        return clientService.toTransitUri(tableName, rowKey);
    }

    private FlowFile doDeleteFromContent(FlowFile flowFile, ProcessContext context, ProcessSession session, String tableName, int batchSize, String charset, String visibility) throws Exception {
        String keySeparator = context.getProperty(KEY_SEPARATOR).evaluateAttributeExpressions(flowFile).getValue();
        final String restartIndex = flowFile.getAttribute(RESTART_INDEX);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        session.exportTo(flowFile, out);
        out.close();

        String data = new String(out.toByteArray(), charset);

        int restartFrom = -1;
        if (restartIndex != null) {
            restartFrom = Integer.parseInt(restartIndex);
        }

        String first = null, last = null;

        List<byte[]> batch = new ArrayList<>();
        if (data != null && data.length() > 0) {
            String[] parts = data.split(keySeparator);
            int index = 0;
            try {
                for (index = 0; index < parts.length; index++) {
                    if (restartFrom > 0 && index < restartFrom) {
                        continue;
                    }

                    if (first == null) {
                        first = parts[index];
                    }

                    batch.add(parts[index].getBytes(charset));
                    if (batch.size() == batchSize) {
                        clientService.delete(tableName, batch, visibility);
                        batch = new ArrayList<>();
                    }
                    last = parts[index];
                }
                if (batch.size() > 0) {
                    clientService.delete(tableName, batch, visibility);
                }

                flowFile = session.removeAttribute(flowFile, RESTART_INDEX);
                flowFile = session.putAttribute(flowFile, ROWKEY_START, first);
                flowFile = session.putAttribute(flowFile, ROWKEY_END, last);
            } catch (Exception ex) {
                getLogger().error("Error sending delete batch", ex);
                int restartPoint = index - batch.size() > 0 ? index - batch.size() : 0;
                flowFile = session.putAttribute(flowFile, RESTART_INDEX, String.valueOf(restartPoint));
            }
        }

        return flowFile;
    }
}
