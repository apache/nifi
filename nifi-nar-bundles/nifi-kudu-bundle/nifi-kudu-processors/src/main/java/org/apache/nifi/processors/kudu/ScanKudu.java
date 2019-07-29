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

package org.apache.nifi.processors.kudu;

import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.kudu.io.ResultHandler;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"kudu", "scan", "fetch", "get"})
@CapabilityDescription("Scans rows from a Kudu table with an optional list of predicates")
@WritesAttributes({
        @WritesAttribute(attribute = "kudu.table", description = "The name of the Kudu table that the row was fetched from"),
        @WritesAttribute(attribute = "mime.type", description = "Set to application/json when using a Destination of flowfile-content, not set or modified otherwise"),
        @WritesAttribute(attribute = "kudu.rows.count", description = "Number of rows in the content of given flow file"),
        @WritesAttribute(attribute = "scankudu.results.found", description = "Indicates whether at least one row has been found in given Kudu table with provided predicates. "
                + "Could be null (not present) if transfered to FAILURE")})
public class ScanKudu extends AbstractKuduProcessor {

    static final Pattern PREDICATES_PATTERN = Pattern.compile("\\w+((<=|>=|[=<>])(\\w|-)+)?(?:,\\w+((<=|>=|[=<>])(\\w|-)+)?)*");
    static final Pattern COLUMNS_PATTERN = Pattern.compile("\\w+((\\w)+)?(?:,\\w+((\\w)+)?)*");

    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Kudu Table to put data into")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor PREDICATES = new PropertyDescriptor.Builder()
            .name("Predicates")
            .description("A comma-separated list of Predicates, format: \"<colName>:<value>\" ")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createRegexMatchingValidator(PREDICATES_PATTERN))
            .build();

    static final PropertyDescriptor PROJECTED_COLUMNS = new PropertyDescriptor.Builder()
            .name("Projected Column Names")
            .description("A comma-separated list of \"<column>\" names to return when scanning, default all.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createRegexMatchingValidator(COLUMNS_PATTERN))
            .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to process in a single execution, between 1 - 100000. " +
                    "Depending on your memory size, and data size per row set an appropriate batch size. " +
                    "Gradually increase this number to find out the best one for best performances.")
            .defaultValue("500")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 100000, true))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this relationship")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input file will be routed to this destination, even if no rows are retrieved based on provided conditions.")
            .build();

    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Kudu")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KUDU_MASTERS);
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
        properties.add(KUDU_OPERATION_TIMEOUT_MS);
        properties.add(KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS);
        properties.add(TABLE_NAME);
        properties.add(PREDICATES);
        properties.add(PROJECTED_COLUMNS);
        properties.add(BATCH_SIZE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    static final Set<Relationship> relationships;
    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_ORIGINAL);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    static final String KUDU_TABLE_ATTR = "kudu.table";

    static final String KUDU_ROWS_COUNT_ATTR = "kudu.rows.count";

    protected int batchSize = 500;

    protected KuduTable kuduTable;


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws LoginException {
        batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        createKuduClient(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final KerberosUser user = getKerberosUser();
        if (user == null) {
            trigger(context, session);
            return;
        }

        final PrivilegedExceptionAction<Void> privelegedAction = () -> {
            trigger(context, session);
            return null;
        };

        final KerberosAction<Void> action = new KerberosAction<>(user, privelegedAction, getLogger());
        action.execute();
    }

    public void trigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final List<String> projctedColumnNames = Arrays.asList(context.getProperty(PROJECTED_COLUMNS).evaluateAttributeExpressions(flowFile).getValue().split(","));
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(tableName)) {
            getLogger().error("Table Name is blank or null for {}, transferring to failure", new Object[] {flowFile});
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        try {
            this.kuduTable = getKuduClient().openTable(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String predicate = context.getProperty(PREDICATES).evaluateAttributeExpressions(flowFile).getValue();
        final AtomicReference<Long> rowsPulledHolder = new AtomicReference<>(0L);
        final AtomicReference<Long> ffCountHolder = new AtomicReference<>(0L);
        ScanKuduResultHandler handler = new ScanKuduResultHandler(session, flowFile, rowsPulledHolder, ffCountHolder, tableName, batchSize);

        try {
            scan(context,
                session,
                this.kuduTable,
                predicate,
                projctedColumnNames,
                handler);

        } catch (Exception e) {
            if (handler.getFlowFile() != null){
                session.remove(handler.getFlowFile());
            }
            getLogger().error("Unable to fetch rows from Kudu table {} due to {}", new Object[] {tableName, e});
            flowFile = session.putAttribute(flowFile, "scankudu.results.found", Boolean.toString(handler.isHandledAny()));
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();

        flowFile = session.putAttribute(flowFile, "scankudu.results.found", Boolean.toString(handler.isHandledAny()));

        FlowFile openedFF = handler.getFlowFile();
        if (openedFF != null) {
            finalizeFlowFile(session, openedFF, tableName, handler.getRecordsCount(), null);
        }

        session.transfer(flowFile, REL_ORIGINAL);
        session.commit();

    }

    /**
     * Result Handler for Scan operation
     */
    private class ScanKuduResultHandler implements ResultHandler {

        final private ProcessSession session;
        final private FlowFile origFF;
        final private AtomicReference<Long> rowsPulledHolder;
        final private AtomicReference<Long> ffCountHolder;
        final private String tableName;
        final private Integer bulkSize;
        private FlowFile flowFile = null;
        final private byte[] JSON_ARRAY_DELIM = ",\n".getBytes();

        private boolean handledAny = false;

        ScanKuduResultHandler(final ProcessSession session,
                              final FlowFile origFF, final AtomicReference<Long> rowsPulledHolder, final AtomicReference<Long> ffCountHolder,
                              final String tableName, final Integer bulkSize){
            this.session = session;
            this.rowsPulledHolder = rowsPulledHolder;
            this.ffCountHolder = ffCountHolder;
            this.tableName = tableName;
            this.bulkSize = bulkSize == null ? 0 : bulkSize;
            this.origFF = origFF;

        }

        @Override
        public void handle(final Iterator<RowResult> resultCells) {

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
                    final String json = convertToJson(resultCells);
                    out.write(json.getBytes());
                });
                handledAny = true;

            }catch(Exception e){
                throw new RuntimeException(e);
            }

            rowsPulled++;

            // bulkSize controls number of records per flow file.
            if (bulkSize > 0 && rowsPulled >= bulkSize) {

                finalizeFlowFile(session, flowFile, tableName, rowsPulled, null);
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
            } else {
                rowsPulledHolder.set(rowsPulled);
            }
        }

        public boolean isHandledAny(){
            return handledAny;
        }

        @Override
        public FlowFile getFlowFile(){
            return flowFile;
        }

        public long getRecordsCount(){
            return rowsPulledHolder.get();
        }

    }


    /*
     * Initiates FF content, adds relevant attributes, and starts content with JSON array "["
     */
    private FlowFile initNewFlowFile(final ProcessSession session, final FlowFile origFF, final String tableName) throws IOException{

        FlowFile flowFile = session.create(origFF);
        flowFile = session.putAttribute(flowFile, KUDU_TABLE_ATTR, tableName);
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

    private void finalizeFlowFile(final ProcessSession session, FlowFile flowFile, final String tableName,
                                  Long rowsPulled, Exception e) {
        Relationship rel = REL_SUCCESS;
        flowFile = session.putAttribute(flowFile, KUDU_ROWS_COUNT_ATTR, rowsPulled.toString());

        final AtomicReference<IOException> ioe = new AtomicReference<>(null);
        flowFile = session.append(flowFile, (out) -> {
            try{
                out.write("]".getBytes());
            }catch(IOException ei){
                ioe.set(ei);
            }
        });
        if (e != null || ioe.get() != null) {
            flowFile = session.putAttribute(flowFile, "scankudu.error", (e==null?e:ioe.get()).toString());
            rel = REL_FAILURE;
        } else {
            session.getProvenanceReporter().receive(flowFile, tableName, "{ids}");
        }
        session.transfer(flowFile, rel);
    }

    private int getBatchSize(){
        return this.batchSize;
    }
}