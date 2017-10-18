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
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@Tags({"rethinkdb", "get", "read", "fetch"})
@CapabilityDescription("Processor to get a JSON document from RethinkDB (https://www.rethinkdb.com/) using the document id. The FlowFile will contain the retrieved document")
@WritesAttributes({
    @WritesAttribute(attribute = GetRethinkDB.RETHINKDB_ERROR_MESSAGE, description = "RethinkDB error message"),
    })
@SeeAlso({PutRethinkDB.class,DeleteRethinkDB.class})
public class GetRethinkDB extends AbstractRethinkDBProcessor {

    public static AllowableValue READ_MODE_SINGLE = new AllowableValue("single", "Single", "Read values from memory from primary replica (Default)");
    public static AllowableValue READ_MODE_MAJORITY = new AllowableValue("majority", "Majority", "Read values committed to disk on majority of replicas");
    public static AllowableValue READ_MODE_OUTDATED = new AllowableValue("outdated", "Outdated", "Read values from memory from an arbitrary replica ");

    protected static final PropertyDescriptor READ_MODE = new PropertyDescriptor.Builder()
            .name("rethinkdb-read-mode")
            .displayName("Read Mode")
            .description("Read mode used for consistency")
            .required(true)
            .defaultValue(READ_MODE_SINGLE.getValue())
            .allowableValues(READ_MODE_SINGLE, READ_MODE_MAJORITY, READ_MODE_OUTDATED)
            .expressionLanguageSupported(true)
            .build();

    protected String READ_MODE_KEY = "read_mode";

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

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
        tempDescriptors.add(READ_MODE);
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

        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());
        String id = context.getProperty(RETHINKDB_DOCUMENT_ID).evaluateAttributeExpressions(flowFile).getValue();
        String readMode = context.getProperty(READ_MODE).evaluateAttributeExpressions(flowFile).getValue();

        if ( StringUtils.isEmpty(id) ) {
            getLogger().error(DOCUMENT_ID_EMPTY_MESSAGE);
            flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, DOCUMENT_ID_EMPTY_MESSAGE);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            long startTimeMillis = System.currentTimeMillis();
            Map<String,Object> document = getDocument(id, readMode);

            if ( document == null ) {
                getLogger().debug("Document with id '" + id + "' not found");
                flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, "Document with id '" + id + "' not found");
                session.transfer(flowFile, REL_NOT_FOUND);
                return;
            }

            String json = gson.toJson(document);

            byte [] documentBytes = json.getBytes(charset);

            if ( documentBytes.length > maxDocumentsSize ) {
                getLogger().error("Document too big with size " + documentBytes.length + " and max limit is " + maxDocumentsSize );
                flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, "Document too big size " + documentBytes.length + " bytes");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(documentBytes);
            session.importFrom(bais, flowFile);
            final long endTimeMillis = System.currentTimeMillis();

            getLogger().debug("Json document {} retrieved Result: {}", new Object[] {id, document});

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().fetch(flowFile,
                new StringBuilder("rethinkdb://").append(databaseName).append("/").append(tableName).append("/").append(id).toString(),
                (endTimeMillis - startTimeMillis));

        } catch (Exception exception) {
            getLogger().error("Failed to get document from RethinkDB due to error {}",
                    new Object[]{exception.getLocalizedMessage()}, exception);
            flowFile = session.putAttribute(flowFile, RETHINKDB_ERROR_MESSAGE, exception.getMessage() + "");
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    protected Map<String,Object> getDocument(String id, String readMode) {
        return getRdbTable().optArg(READ_MODE_KEY,readMode).get(id).run(rethinkDbConnection);
    }

    @OnStopped
    public void close() {
        super.close();
    }
}