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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.impl.GenericDocumentImpl;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StructuredQueryDefinition;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"MarkLogic", "Get", "Query", "Read"})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria," +
    " retrieved from a MarkLogic server using the MarkLogic Data Movement SDK (DMSDK)")
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The filename is set to the uri of the document retrieved from MarkLogic")})
public class QueryMarkLogic extends AbstractMarkLogicProcessor {

    public static final PropertyDescriptor CONSISTENT_SNAPSHOT = new PropertyDescriptor.Builder()
        .name("Consistent snapshot")
        .displayName("Consistent snapshot")
        .defaultValue("true")
        .description("Boolean used to indicate that the matching documents were retrieved from a " +
            "consistent snapshot")
        .required(true)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    public static final PropertyDescriptor COLLECTIONS = new PropertyDescriptor.Builder()
        .name("Collections")
        .displayName("Collections")
        .description("Comma-separated list of collections to query from a MarkLogic server")
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
        if(collections == null) {
            validationResultSet.add(new ValidationResult.Builder().subject("Query").valid(false).explanation("one " +
                "of the following properties need to be set - " + COLLECTIONS.getDisplayName()).build());
        }
        return validationResultSet;
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        DataMovementManager dataMovementManager = getDatabaseClient(context).newDataMovementManager();
        queryBatcher = createQueryBatcherWithQueryCriteria(context, getDatabaseClient(context), dataMovementManager);
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
            final ProcessSession session = sessionFactory.createSession();
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

    private QueryBatcher createQueryBatcherWithQueryCriteria(ProcessContext context, DatabaseClient databaseClient, DataMovementManager dataMovementManager) {
        String collectionsValue = context.getProperty(COLLECTIONS).getValue();
        QueryManager queryManager = databaseClient.newQueryManager();
        if(collectionsValue != null) {
            StructuredQueryDefinition query = queryManager.newStructuredQueryBuilder()
                    .collection(collectionsValue.split(","));
            queryBatcher = dataMovementManager.newQueryBatcher(query);
        }
        if(queryBatcher == null) {
            throw new IllegalStateException("No valid Query criteria specified!");
        }
        return queryBatcher;
    }

    private DocumentPage getDocs(QueryBatch batch, boolean consistentSnapshot) {
        GenericDocumentManager docMgr = batch.getClient().newDocumentManager();
        if ( consistentSnapshot == true ) {
            return ((GenericDocumentImpl) docMgr).read( batch.getServerTimestamp(), batch.getItems() );
        } else {
            return docMgr.read( batch.getItems() );
        }
    }

    QueryBatcher getQueryBatcher() {
        return this.queryBatcher;
    }
}