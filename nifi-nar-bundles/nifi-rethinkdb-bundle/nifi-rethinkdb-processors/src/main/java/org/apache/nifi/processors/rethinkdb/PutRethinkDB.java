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
package org.apache.nifi.processors.rethinkdb;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.json.simple.parser.JSONParser;
import com.rethinkdb.gen.ast.Insert;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"rethinkdb", "stream","insert", "update", "write", "put"})
@CapabilityDescription("Processor to write the JSON content of a FlowFile to RethinkDB (https://www.rethinkdb.com/). The flow file should contain either JSON Object an array of JSON documents")
@WritesAttributes({
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_ERROR_MESSAGE, description = "RethinkDB error message"),
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_INSERT_RESULT_ERROR_KEY, description = "Error count while inserting documents"),
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_INSERT_RESULT_DELETED_KEY, description = "Number of documents deleted"),
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY, description = "Keys generated on inserting documents"),
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_INSERT_RESULT_INSERTED_KEY, description = "Number of documents inserted"),
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_INSERT_RESULT_REPLACED_KEY, description = "Number of documents replaced"),
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_INSERT_RESULT_SKIPPED_KEY, description = "Number of documents skipped because they already existed"),
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_INSERT_RESULT_UNCHANGED_KEY, description = "Number of documents unchanged since they already existed"),
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY, description = "First error while inserting documents"),
    @WritesAttribute(attribute = PutRethinkDB.RETHINKDB_INSERT_RESULT_WARNINGS_KEY, description = "Warning message in case of large number of ids being returned on insertion")
    })
@SeeAlso({GetRethinkDB.class,DeleteRethinkDB.class})
public class PutRethinkDB extends AbstractRethinkDBProcessor {

    public static AllowableValue CONFLICT_STRATEGY_UPDATE = new AllowableValue("update", "Update", "Update the document having same id with new values");
    public static AllowableValue CONFLICT_STRATEGY_REPLACE = new AllowableValue("replace", "Replace", "Replace the document with having same id new document");
    public static AllowableValue CONFLICT_STRATEGY_ERROR = new AllowableValue("error", "Error", "Return error if the document with same id exists");

    protected static final PropertyDescriptor CONFLICT_STRATEGY = new PropertyDescriptor.Builder()
            .name("rethinkdb-conflict-strategy")
            .displayName("Conflict strategy")
            .description("Conflict strategy to be used in case of inserting existing document.")
            .required(true)
            .defaultValue(CONFLICT_STRATEGY_UPDATE.getValue())
            .allowableValues(CONFLICT_STRATEGY_UPDATE, CONFLICT_STRATEGY_REPLACE, CONFLICT_STRATEGY_ERROR)
            .expressionLanguageSupported(true)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    public static final String RETHINKDB_INSERT_RESULT = "rethinkdb.insert.result";
    public static final String RETHINKDB_INSERT_RESULT_ERROR_KEY = "rethinkdb.insert.errors";
    public static final String RETHINKDB_INSERT_RESULT_DELETED_KEY = "rethinkdb.insert.deleted";
    public static final String RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY = "rethinkdb.insert.generated_keys";
    public static final String RETHINKDB_INSERT_RESULT_INSERTED_KEY = "rethinkdb.insert.inserted";
    public static final String RETHINKDB_INSERT_RESULT_REPLACED_KEY = "rethinkdb.insert.replaced";
    public static final String RETHINKDB_INSERT_RESULT_SKIPPED_KEY = "rethinkdb.insert.skipped";
    public static final String RETHINKDB_INSERT_RESULT_UNCHANGED_KEY = "rethinkdb.insert.unchanged";
    public static final String RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY = "rethinkdb.insert.first_error";
    public static final String RETHINKDB_INSERT_RESULT_WARNINGS_KEY = "rethinkdb.insert.warnings";

    public final String CONFLICT_OPTION_KEY = "conflict";

    static {
        final Set<Relationship> tempRelationships = new HashSet<>();
        tempRelationships.add(REL_SUCCESS);
        tempRelationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(tempRelationships);

        final List<PropertyDescriptor> tempDescriptors = new ArrayList<>();
        tempDescriptors.add(DB_NAME);
        tempDescriptors.add(DB_HOST);
        tempDescriptors.add(DB_PORT);
        tempDescriptors.add(USERNAME);
        tempDescriptors.add(PASSWORD);
        tempDescriptors.add(TABLE_NAME);
        tempDescriptors.add(CHARSET);
        tempDescriptors.add(CONFLICT_STRATEGY);
        tempDescriptors.add(DURABILITY);
        tempDescriptors.add(MAX_DOCUMENTS_SIZE);
        propertyDescriptors = Collections.unmodifiableList(tempDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        maxDocumentsSize = context.getProperty(MAX_DOCUMENTS_SIZE).asDataSize(DataUnit.B).longValue();
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if ( flowFile.getSize() == 0) {
            getLogger().error("Empty message");
            flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, "Empty message size " + flowFile.getSize());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if ( flowFile.getSize() > maxDocumentsSize) {
            getLogger().error("Message size exceeded {} max allowed is {}", new Object[] { flowFile.getSize(), maxDocumentsSize});
            flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, "Max message size exceeded " + flowFile.getSize());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());
        String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).evaluateAttributeExpressions(flowFile).getValue();
        String durability = context.getProperty(DURABILITY).evaluateAttributeExpressions(flowFile).getValue();

        try {
            long startTimeMillis = System.currentTimeMillis();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            session.exportTo(flowFile, baos);
            String documents = new String(baos.toByteArray(), charset);
            JSONParser parser = new JSONParser();
            Object jsonDocuments = parser.parse(documents);

            Insert insert = getRdbTable().insert(jsonDocuments)
                .optArg(CONFLICT_OPTION_KEY, conflictStrategy)
                .optArg(DURABILITY_OPTION_KEY, durability);

            HashMap<String,Object> result = runInsert(insert);
            final long endTimeMillis = System.currentTimeMillis();
            getLogger().debug("Json documents {} inserted Result: {}", new Object[] {documents, result});
            flowFile = populateAttributes(session, flowFile, result);

            if ( (Long)result.get(RESULT_ERROR_KEY) != 0 ) {
                getLogger().error("There were errors while inserting data documents {} result {}",
                   new Object [] {documents, result});
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().send(flowFile,
                    new StringBuilder("rethinkdb://").append(databaseName).append("/").append(tableName).toString(),
                    (endTimeMillis - startTimeMillis));
            }
        } catch (Exception exception) {
            getLogger().error("Failed to insert into RethinkDB due to {}",
                    new Object[]{exception.getLocalizedMessage()}, exception);
            flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, String.valueOf(exception.getMessage()));
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private FlowFile populateAttributes(final ProcessSession session, FlowFile flowFile,
            HashMap<String, Object> result) {
        Map<String,String> resultAttributes = new HashMap<>();
        resultAttributes.put(RETHINKDB_INSERT_RESULT, result.toString());
        resultAttributes.put(RETHINKDB_INSERT_RESULT_ERROR_KEY, String.valueOf(result.get(RESULT_ERROR_KEY)));
        resultAttributes.put(RETHINKDB_INSERT_RESULT_DELETED_KEY, String.valueOf(result.get(RESULT_DELETED_KEY)));
        resultAttributes.put(RETHINKDB_INSERT_RESULT_GENERATED_KEYS_KEY, String.valueOf(result.get(RESULT_GENERATED_KEYS_KEY)));
        resultAttributes.put(RETHINKDB_INSERT_RESULT_INSERTED_KEY, String.valueOf(result.get(RESULT_INSERTED_KEY)));
        resultAttributes.put(RETHINKDB_INSERT_RESULT_REPLACED_KEY, String.valueOf(result.get(RESULT_REPLACED_KEY)));
        resultAttributes.put(RETHINKDB_INSERT_RESULT_SKIPPED_KEY, String.valueOf(result.get(RESULT_SKIPPED_KEY)));
        resultAttributes.put(RETHINKDB_INSERT_RESULT_UNCHANGED_KEY, String.valueOf(result.get(RESULT_UNCHANGED_KEY)));
        resultAttributes.put(RETHINKDB_INSERT_RESULT_FIRST_ERROR_KEY, String.valueOf(result.get(RESULT_FIRST_ERROR_KEY)));
        resultAttributes.put(RETHINKDB_INSERT_RESULT_WARNINGS_KEY, String.valueOf(result.get(RESULT_WARNINGS_KEY)));
        flowFile = session.putAllAttributes(flowFile, resultAttributes);
        return flowFile;
    }

    protected HashMap<String,Object> runInsert(Insert insert) {
        return insert.run(rethinkDbConnection);
    }

    /**
     * Shutdown client connection
     */
    @OnStopped
    public void close() {
        super.close();
    }
}