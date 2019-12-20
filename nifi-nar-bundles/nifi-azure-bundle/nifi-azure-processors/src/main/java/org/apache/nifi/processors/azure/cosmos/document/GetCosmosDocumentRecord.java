/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.azure.cosmos.document;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.azure.cosmos.CosmosItemProperties;
import com.azure.cosmos.FeedOptions;
import com.azure.cosmos.FeedResponse;

@Tags({ "azure", "cosmos", "record", "read", "fetch" })
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("A record-based version of GetCosmosDocument that uses the Record writers to write the SQL select query result set.")
public class GetCosmosDocumentRecord extends AbstractCosmosDocumentProcessor {
    public static final PropertyDescriptor WRITER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-writer-factory")
        .displayName("Record Writer")
        .description("The record writer to use to write the result sets.")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();
    public static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
        .name("schema-name")
        .displayName("Schema Name")
        .description("The name of the schema in the configured schema registry to use for the query results.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .defaultValue("${schema.name}")
        .required(true)
        .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder().name("SQL Core Document Query")
            .description("The SQL select query to execute. "
                    + "This should be a valid SQL select query to cosmo document database with core sql api.")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    public static final PropertyDescriptor MAX_RESPONSE_PAGE_SIZE = new PropertyDescriptor.Builder().name("max_page_size")
            .description("The maximum number of elements in a response page from cosmos document database")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();


    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;
    private ComponentLog logger;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(WRITER_FACTORY);
        _propertyDescriptors.add(SCHEMA_NAME);
        _propertyDescriptors.add(MAX_RESPONSE_PAGE_SIZE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_ORIGINAL);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private boolean sendEmpty;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        Collection<ValidationResult> result = super.customValidate(context);

        boolean queryIsSet = context.getProperty(QUERY).isSet();
        if (!queryIsSet) {
            final String msg = GetCosmosDocument.QUERY.getDisplayName() + " must be set.";
            result.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        return result;
    }

    private String getQuery(ProcessContext context, ProcessSession session, FlowFile input) {
        String query = null;

        if (context.getProperty(QUERY).isSet()) {
            query = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
        } else if (!context.getProperty(QUERY).isSet() && input == null) {
            query = "select top 100 * from c";
        }
        return query;
    }

    private Map<String, String> getAttributes(ProcessContext context, FlowFile input) {
        final Map<String, String> attributes = new HashMap<>();

        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");

        if (context.getProperty(QUERY).isSet()) {
            final String query = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
            attributes.put("query", query);
        }
        return attributes;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        this.writerFactory =context.getProperty(WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);

        final FlowFile input = context.hasIncomingConnection() ? session.get() : null;
        logger = getLogger();
        logger.debug("inside GetCosmosDocumentRecord.onTrigger");

        if (input == null && context.hasNonLoopConnection()) {
            logger.debug("No flowfile input and NonLoopConnection. Ending onTrigger... ");
            return;
        }
        final String query;
        final Map<String, String> attributes;
        try {
            query = getQuery(context, session, input);
            attributes = getAttributes(context, input);
        } catch (Exception ex) {
            logger.error("Error parsing query or getting attirbutes.", ex);
            if (input != null) {
                session.transfer(input, REL_FAILURE);
            }
            return; // We need to stop immediately.
        }

        final FeedOptions queryOptions = new FeedOptions();
        queryOptions.setEnableCrossPartitionQuery(true);
        if (context.getProperty(MAX_RESPONSE_PAGE_SIZE).isSet()) {
            final int max_page_size = context.getProperty(MAX_RESPONSE_PAGE_SIZE).evaluateAttributeExpressions(input)
                    .asInteger();
            logger.debug("setting max page size : " + max_page_size);
            queryOptions.maxItemCount(max_page_size);
        }
        logger.debug("Running Cosmos SQL query : " + query);



        Iterator<FeedResponse<CosmosItemProperties>> pages = container != null
                ? container.queryItems(query, queryOptions)
                : null;
        if (pages == null) {
            logger.error("Fails to get FeedResponse<CosmosItemProperties> Iterator");
            return;
        }
        FlowFile output = input != null ? session.create(input) : session.create();
        try {
            logger.info("Start to processing data from cosmo database");
            final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(input).getValue();
            try (OutputStream out = session.write(output)) {
                Map<String, String> attrs = input != null ? input.getAttributes() : new HashMap<String, String>(){{
                    put("schema.name", schemaName);
                }};
                RecordSchema schema = writerFactory.getSchema(attrs, null);
                RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, attrs);
                long count = 0L;
                writer.beginRecordSet();

                while(pages.hasNext()) {

                    FeedResponse<CosmosItemProperties> page = pages.next();
                    for(CosmosItemProperties doc: page.getResults()){
                        Map<String,Object> mapObj = doc.getMap();
                        Record record = new MapRecord(schema, mapObj);
                        writer.write(record);
                        count++;
                    }
                }
                writer.finishRecordSet();
                writer.close();
                out.close();
                attributes.put("record.count", String.valueOf(count));
            }catch (SchemaNotFoundException e) {
                throw new RuntimeException(e);
            }
            output = session.putAllAttributes(output, attributes);

            session.getProvenanceReporter().fetch(output, getURI(context));
            session.transfer(output, REL_SUCCESS);
            if (input != null) {
                session.transfer(input, REL_ORIGINAL);
            }

        }catch(Exception e ){
            logger.error("Failed to wait for query to be completed with: " +e);
            session.remove(output);
            if (input != null) {
                session.transfer(input, REL_FAILURE);
            }
        }
    }

    private void warmupCosmosContainer() {
        // This method runs once during OnSchedule so that actual queries, run during onTrigger, perform fasters
        // Without this method, the intial first query may take much longer and noticable delay than other subsequent queries.
        if(cosmosClient != null){
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setEnableCrossPartitionQuery(true)
                        .maxItemCount(10);
            container.queryItems("select top 1 c.id from c", queryOptions);
        }
    }
    private RecordSetWriterFactory writerFactory;

    @Override
    protected void doPostActionOnSchedule(final ProcessContext context) {
        try{
            warmupCosmosContainer();
        }catch(Exception e) {
            logger.error("failure in doPostActionOnSchedule with "+ e.getMessage());
        }
    }
}
