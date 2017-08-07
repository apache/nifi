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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
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
@Tags({"rethinkdb", "delete", "remove"})
@CapabilityDescription("Processor to remove a JSON document from RethinkDB (https://www.rethinkdb.com/) using the document id.")
@WritesAttributes({
    @WritesAttribute(attribute = DeleteRethinkDB.RETHINKDB_ERROR_MESSAGE, description = "RethinkDB error message"),
    @WritesAttribute(attribute = DeleteRethinkDB.RETHINKDB_DELETE_RESULT_ERROR_KEY, description = "Error count while delete documents"),
    @WritesAttribute(attribute = DeleteRethinkDB.RETHINKDB_DELETE_RESULT_DELETED_KEY, description = "Number of documents deleted"),
    @WritesAttribute(attribute = DeleteRethinkDB.RETHINKDB_DELETE_RESULT_INSERTED_KEY, description = "Number of documents inserted"),
    @WritesAttribute(attribute = DeleteRethinkDB.RETHINKDB_DELETE_RESULT_REPLACED_KEY, description = "Number of documents replaced"),
    @WritesAttribute(attribute = DeleteRethinkDB.RETHINKDB_DELETE_RESULT_SKIPPED_KEY, description = "Number of documents skipped"),
    @WritesAttribute(attribute = DeleteRethinkDB.RETHINKDB_DELETE_RESULT_UNCHANGED_KEY, description = "Number of documents unchanged since they already existed"),
    })
@SeeAlso({PutRethinkDB.class,GetRethinkDB.class})
public class DeleteRethinkDB extends AbstractRethinkDBProcessor {

    public static AllowableValue RETURN_CHANGES_TRUE = new AllowableValue("true", "True", "Return changed document");
    public static AllowableValue RETURN_CHANGES_FALSE = new AllowableValue("false", "False", "Do not return changed document");

    protected static final PropertyDescriptor RETURN_CHANGES = new PropertyDescriptor.Builder()
            .name("rethinkdb-return-result")
            .displayName("Return deleted value")
            .description("Return old value which were deleted")
            .required(true)
            .defaultValue(RETURN_CHANGES_TRUE.getValue())
            .allowableValues(RETURN_CHANGES_TRUE, RETURN_CHANGES_FALSE)
            .expressionLanguageSupported(true)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    public static final String RETHINKDB_DELETE_RESULT_ERROR_KEY = "rethinkdb.delete.errors";
    public static final String RETHINKDB_DELETE_RESULT_DELETED_KEY = "rethinkdb.delete.deleted";
    public static final String RETHINKDB_DELETE_RESULT_INSERTED_KEY = "rethinkdb.delete.inserted";
    public static final String RETHINKDB_DELETE_RESULT_REPLACED_KEY = "rethinkdb.delete.replaced";
    public static final String RETHINKDB_DELETE_RESULT_SKIPPED_KEY = "rethinkdb.delete.skipped";
    public static final String RETHINKDB_DELETE_RESULT_UNCHANGED_KEY = "rethinkdb.delete.unchanged";

    public static final String RESULT_CHANGES_KEY = "changes";
    public static final String RETURN_CHANGES_OPTION_KEY = "return_changes";

    protected Gson gson = new Gson();

    static {
        final Set<Relationship> tempRelationships = new HashSet<>();
        tempRelationships.add(REL_SUCCESS);
        tempRelationships.add(REL_FAILURE);
        tempRelationships.add(REL_NOT_FOUND);
        relationships = Collections.unmodifiableSet(tempRelationships);

        final List<PropertyDescriptor> tempDescriptors = new ArrayList<>();
        tempDescriptors.add(DB_NAME);
        tempDescriptors.add(DB_HOST);
        tempDescriptors.add(DB_PORT);
        tempDescriptors.add(USERNAME);
        tempDescriptors.add(PASSWORD);
        tempDescriptors.add(TABLE_NAME);
        tempDescriptors.add(CHARSET);
        tempDescriptors.add(RETHINKDB_DOCUMENT_ID);
        tempDescriptors.add(RETURN_CHANGES);
        tempDescriptors.add(DURABILITY);
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
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());
        String id = context.getProperty(RETHINKDB_DOCUMENT_ID).evaluateAttributeExpressions(flowFile).getValue();
        String durablity = context.getProperty(DURABILITY).evaluateAttributeExpressions(flowFile).getValue();
        Boolean returnChanges = context.getProperty(RETURN_CHANGES).evaluateAttributeExpressions(flowFile).asBoolean();

        if ( StringUtils.isEmpty(id) ) {
            getLogger().error(DOCUMENT_ID_EMPTY_MESSAGE);
            flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, DOCUMENT_ID_EMPTY_MESSAGE);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            long startTimeMillis = System.currentTimeMillis();
            Map<String,Object> result = deleteDocument(id, durablity, returnChanges);
            final long endTimeMillis = System.currentTimeMillis();

            getLogger().debug("Json document {} deleted Result: {}", new Object[] {id, result});

            flowFile = populateAttributes(session, flowFile, result);

            Long deletedCount = ((Long)result.get(RESULT_DELETED_KEY)).longValue();

            if ( deletedCount == 0L ) {
                getLogger().debug("Deleted count should be 1 but was " + deletedCount + " for document with id '" + id + "'");

                flowFile = populateAttributes(session, flowFile, result);

                flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, "Deleted count should be 1 but was " + deletedCount + " for document with id '" + id + "'");
                session.transfer(flowFile, REL_NOT_FOUND);
                return;
            }

            if ( returnChanges ) {
                String json = gson.toJson(((List)result.get(RESULT_CHANGES_KEY)).get(0));

                byte [] documentBytes = json.getBytes(charset);

                ByteArrayInputStream bais = new ByteArrayInputStream(documentBytes);
                session.importFrom(bais, flowFile);

                session.getProvenanceReporter().modifyContent(flowFile,
                        new StringBuilder("rethinkdb://").append(databaseName).append("/").append(tableName).append("/").append(id).toString(),
                        (endTimeMillis - startTimeMillis));
            }

            session.transfer(flowFile, REL_SUCCESS);


        } catch (Exception exception) {
            getLogger().error("Failed to delete document from RethinkDB due to error {}",
                    new Object[]{exception.getLocalizedMessage()}, exception);
            flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, exception.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private FlowFile populateAttributes(final ProcessSession session, FlowFile flowFile,
            Map<String, Object> result) {
        Map<String,String> resultAttributes = new HashMap<>();
        resultAttributes.put(RETHINKDB_DELETE_RESULT_ERROR_KEY, String.valueOf(result.get(RESULT_ERROR_KEY)));
        resultAttributes.put(RETHINKDB_DELETE_RESULT_DELETED_KEY, String.valueOf(result.get(RESULT_DELETED_KEY)));
        resultAttributes.put(RETHINKDB_DELETE_RESULT_INSERTED_KEY, String.valueOf(result.get(RESULT_INSERTED_KEY)));
        resultAttributes.put(RETHINKDB_DELETE_RESULT_REPLACED_KEY, String.valueOf(result.get(RESULT_REPLACED_KEY)));
        resultAttributes.put(RETHINKDB_DELETE_RESULT_SKIPPED_KEY, String.valueOf(result.get(RESULT_SKIPPED_KEY)));
        resultAttributes.put(RETHINKDB_DELETE_RESULT_UNCHANGED_KEY, String.valueOf(result.get(RESULT_UNCHANGED_KEY)));
        flowFile = session.putAllAttributes(flowFile, resultAttributes);
        return flowFile;
    }

    protected Map<String,Object> deleteDocument(String id, String durablity, Boolean returnChanges) {
        return getRdbTable().get(id).delete().optArg(DURABILITY_OPTION_KEY,durablity).optArg(RETURN_CHANGES_OPTION_KEY, returnChanges).run(rethinkDbConnection);
    }

    @OnStopped
    public void close() {
        super.close();
    }
}