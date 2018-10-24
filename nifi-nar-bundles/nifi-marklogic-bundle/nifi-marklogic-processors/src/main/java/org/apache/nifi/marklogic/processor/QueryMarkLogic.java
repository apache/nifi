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
package org.apache.nifi.marklogic.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.impl.GenericDocumentImpl;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.RawStructuredQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryDefinition;

@Tags({"MarkLogic", "Get", "Query", "Read"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria," +
    " retrieved from a MarkLogic server using the MarkLogic Data Movement SDK (DMSDK)")
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The filename is set to the uri of the document retrieved from MarkLogic")})
public class QueryMarkLogic extends AbstractMarkLogicProcessor {

    public static final PropertyDescriptor CONSISTENT_SNAPSHOT = new PropertyDescriptor.Builder()
        .name("Consistent Snapshot")
        .displayName("Consistent Snapshot")
        .defaultValue("true")
        .description("Boolean used to indicate that the matching documents were retrieved from a " +
            "consistent snapshot")
        .required(true)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("Query")
        .displayName("Query")
        .description("Query text that corresponds with the selected Query Type")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor QUERY_TYPE = new PropertyDescriptor.Builder()
        .name("Query Type")
        .displayName("Query Type")
        .description("Type of query that will be used to retrieve data from MarkLogic")
        .required(true)
        .allowableValues(QueryTypes.values())
        .defaultValue(QueryTypes.COMBINED_JSON.name())
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor COLLECTIONS = new PropertyDescriptor.Builder()
        .name("Collections")
        .displayName("Collections")
        .description("**Deprecated: Use Query Type and Query** Comma-separated list of collections to query from a MarkLogic server")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    protected static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are created from documents read from MarkLogic are routed to" +
            " this success relationship.")
        .build();
    private QueryBatcher queryBatcher;

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        List<PropertyDescriptor> list = new ArrayList<>(properties);
        list.add(CONSISTENT_SNAPSHOT);
        list.add(QUERY);
        list.add(QUERY_TYPE);
        list.add(COLLECTIONS);
        properties = Collections.unmodifiableList(list);
        Set<Relationship> set = new HashSet<>();
        set.add(SUCCESS);
        relationships = Collections.unmodifiableSet(set);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Set<ValidationResult> validationResultSet = new HashSet<>();
        String collections = validationContext.getProperty(COLLECTIONS).getValue();
        String query = validationContext.getProperty(QUERY).getValue();
        if(collections == null && query == null) {
            validationResultSet.add(new ValidationResult.Builder().subject("Query").valid(false).explanation("The Query value must be set. " +
              "The deprecated Collections property will be migrated appropriately.").build());
        }
        return validationResultSet;
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session);
        } catch (final Throwable t) {
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
            session.rollback(true);
            throw new ProcessException(t);
        }
    }

    public final void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = null;

        if (context.hasIncomingConnection()) {
            input = session.get();
        }

        DataMovementManager dataMovementManager = getDatabaseClient(context).newDataMovementManager();
        queryBatcher = createQueryBatcherWithQueryCriteria(context, input, getDatabaseClient(context), dataMovementManager);
        if(context.getProperty(BATCH_SIZE).asInteger() != null) queryBatcher.withBatchSize(context.getProperty(BATCH_SIZE).asInteger());
        if(context.getProperty(THREAD_COUNT).asInteger() != null) queryBatcher.withThreadCount(context.getProperty(THREAD_COUNT).asInteger());
        final boolean consistentSnapshot;
        if(context.getProperty(CONSISTENT_SNAPSHOT).asBoolean() != null && !context.getProperty(CONSISTENT_SNAPSHOT).asBoolean()) {
            consistentSnapshot = false;
        } else {
            queryBatcher.withConsistentSnapshot();
            consistentSnapshot = true;
        }

        queryBatcher.onUrisReady(batch -> {
            try( DocumentPage docs = getDocs(batch, consistentSnapshot) ) {
                while ( docs.hasNext() ) {
                    DocumentRecord documentRecord = docs.next();
                    FlowFile flowFile = session.create();
                    flowFile = session.write(flowFile, out -> out.write(documentRecord.getContent(new BytesHandle()).get()));
                    session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), documentRecord.getUri());
                    session.transfer(flowFile, SUCCESS);
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("Routing " + documentRecord.getUri() + " to " + SUCCESS.getName());
                    }
                }
                session.commit();
            } catch (final Throwable t) {
                getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
                session.rollback(true);
                throw new ProcessException(t);
            }
        });
        dataMovementManager.startJob(queryBatcher);
        queryBatcher.awaitCompletion();
        dataMovementManager.stopJob(queryBatcher);
    }

    private QueryBatcher createQueryBatcherWithQueryCriteria(ProcessContext context, FlowFile flowFile, DatabaseClient databaseClient, DataMovementManager dataMovementManager) {
        final PropertyValue queryProperty = context.getProperty(QUERY);
        final String queryValue;
        final String queryTypeValue;

        // Gracefully migrate old Collections templates
        String collectionsValue = context.getProperty(COLLECTIONS).getValue();
        if (!(collectionsValue == null || "".equals(collectionsValue))) {
            queryValue = collectionsValue;
            queryTypeValue = QueryTypes.COLLECTION.name();
        } else {
            queryValue = queryProperty.evaluateAttributeExpressions(flowFile).getValue();
            queryTypeValue = context.getProperty(QUERY_TYPE).getValue();
        }
        if (queryValue != null) {
            final QueryManager queryManager = databaseClient.newQueryManager();
            switch (QueryTypes.valueOf(queryTypeValue)) {
                case COLLECTION:
                    String[] collections = getArrayFromCommaSeparatedString(queryValue);
                    if (collections != null) {
                        StructuredQueryDefinition query = queryManager.newStructuredQueryBuilder()
                                .collection(collections);
                        queryBatcher = dataMovementManager.newQueryBatcher(query);
                    }
                    break;
                case COMBINED_JSON:
                    queryBatcher = batcherFromCombinedQuery(dataMovementManager, queryManager, queryValue, Format.JSON);
                    break;
                case COMBINED_XML:
                    queryBatcher = batcherFromCombinedQuery(dataMovementManager, queryManager, queryValue, Format.XML);
                    break;
                case STRING:
                    StringQueryDefinition strDef = queryManager.newStringDefinition().withCriteria(queryValue);
                    queryBatcher = dataMovementManager.newQueryBatcher(strDef);
                    break;
                case STRUCTURED_JSON:
                    queryBatcher = batcherFromStructuredQuery(dataMovementManager, queryManager, queryValue, Format.JSON);
                    break;
                case STRUCTURED_XML:
                    queryBatcher = batcherFromStructuredQuery(dataMovementManager, queryManager, queryValue, Format.XML);
                    break;
                default:
                    throw new IllegalStateException("No valid Query type selected!");
            }
        }
        if(queryBatcher == null) {
            throw new IllegalStateException("No valid Query criteria specified!");
        }
        return queryBatcher;
    }

    private DocumentPage getDocs(QueryBatch batch, boolean consistentSnapshot) {
        GenericDocumentManager docMgr = batch.getClient().newDocumentManager();
        if (consistentSnapshot) {
            return ((GenericDocumentImpl) docMgr).read( batch.getServerTimestamp(), batch.getItems() );
        } else {
            return docMgr.read( batch.getItems() );
        }
    }

    QueryBatcher getQueryBatcher() {
        return this.queryBatcher;
    }

    StringHandle handleForQuery(String queryValue, Format format) {
        StringHandle handle = new StringHandle().withFormat(format).with(queryValue);
        return handle;
    }

    QueryBatcher batcherFromCombinedQuery(DataMovementManager dataMovementManager, QueryManager queryMgr, String queryValue, Format format) {
        RawCombinedQueryDefinition qdef = queryMgr.newRawCombinedQueryDefinition(handleForQuery(queryValue, format));
        return dataMovementManager.newQueryBatcher(qdef);
    }

    QueryBatcher batcherFromStructuredQuery(DataMovementManager dataMovementManager, QueryManager queryMgr, String queryValue, Format format) {
        RawStructuredQueryDefinition qdef = queryMgr.newRawStructuredQueryDefinition(handleForQuery(queryValue, format));
        return dataMovementManager.newQueryBatcher(qdef);
    }

    public enum QueryTypes {
        COLLECTION("Collection Query"),
        COMBINED_JSON("Combined Query (JSON)"),
        COMBINED_XML("Combined Query (XML)"),
        STRING("String Query"),
        STRUCTURED_JSON("Structured Query (JSON)"),
        STRUCTURED_XML("Structured Query (XML)");
        private final String text;

        /**
         * @param text Display text for Query Type enumerators
         */
        QueryTypes(final String text) {
           this.text = text;
       }

        @Override
        public String toString() {
           return text;
       }
    }
}