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
package com.marklogic.nifi.processor;

import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.ext.datamovement.job.SimpleQueryBatcherJob;
import com.marklogic.client.impl.GenericDocumentImpl;
import com.marklogic.client.io.BytesHandle;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
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
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Creates FlowFiles from batches of documents, matching the given criteria," +
    " retrieved from a MarkLogic server using the MarkLogic Data Movement SDK (DMSDK)")
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

    public static final PropertyDescriptor URIS_QUERY = new PropertyDescriptor.Builder()
        .name("URIs query")
        .displayName("URIs query")
        .description("CTS URI Query for retrieving documents from a MarkLogic server")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor URI_PATTERN = new PropertyDescriptor.Builder()
        .name("URI pattern")
        .displayName("URI pattern")
        .description("URI pattern for retrieving documents from a MarkLogic server")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    protected static final Relationship SUCCESS = new Relationship.Builder()
        .name("SUCCESS")
        .description("All FlowFiles that are created from documents read from MarkLogic are routed to" +
            " this success relationship.")
        .build();

    @Override
    public void init(ProcessorInitializationContext context) {
        super.init(context);

        List<PropertyDescriptor> list = new ArrayList<>();
        list.addAll(properties);
        list.add(CONSISTENT_SNAPSHOT);
        list.add(COLLECTIONS);
        list.add(URIS_QUERY);
        list.add(URI_PATTERN);
        properties = Collections.unmodifiableList(list);
        Set<Relationship> set = new HashSet<>();
        set.add(SUCCESS);
        relationships = Collections.unmodifiableSet(set);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Set<ValidationResult> validationResultSet = new HashSet<>();
        String collections = validationContext.getProperty(COLLECTIONS).getValue();
        String uriPattern = validationContext.getProperty(URI_PATTERN).getValue();
        String uriQuery = validationContext.getProperty(URIS_QUERY).getValue();
        if(collections == null && uriPattern == null && uriQuery == null) {
            validationResultSet.add(new ValidationResult.Builder().subject("Query").valid(false).explanation("one " +
                "of the following properties need to be set - " + COLLECTIONS.getDisplayName() + ", "
                + URI_PATTERN.getDisplayName() + ", or " + URIS_QUERY.getDisplayName()).build());
        }
        return validationResultSet;
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        SimpleQueryBatcherJob job = new SimpleQueryBatcherJob();
        job.setBatchSize(context.getProperty(BATCH_SIZE).asInteger());
        job.setThreadCount(context.getProperty(THREAD_COUNT).asInteger());
        job.setConsistentSnapshot(context.getProperty(CONSISTENT_SNAPSHOT).asBoolean());

        String value = context.getProperty(COLLECTIONS).getValue();
        if (value != null) {
            job.setWhereCollections(value.split(","));
        }

        job.setWhereUriPattern(context.getProperty(URI_PATTERN).getValue());
        job.setWhereUrisQuery(context.getProperty(URIS_QUERY).getValue());

        job.addUrisReadyListener(batch -> {
            final ProcessSession session = sessionFactory.createSession();
            try( DocumentPage docs = getDocs(batch, context.getProperty(CONSISTENT_SNAPSHOT).asBoolean()) ) {
                while ( docs.hasNext() ) {
                    DocumentRecord documentRecord = docs.next();
                    FlowFile flowFile = session.create();
                    flowFile = session.write(flowFile, out -> out.write(documentRecord.getContent(new BytesHandle()).get()));
                    session.putAttribute(flowFile, "uri", documentRecord.getUri());
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
        job.setAwaitCompletion(true);
        job.setStopJobAfterCompletion(true);
        runJob(job, context);
    }

    private DocumentPage getDocs(QueryBatch batch, boolean consistentSnapshot) {
        GenericDocumentManager docMgr = batch.getClient().newDocumentManager();
        if ( consistentSnapshot == true ) {
            return ((GenericDocumentImpl) docMgr).read( batch.getServerTimestamp(), batch.getItems() );
        } else {
            return docMgr.read( batch.getItems() );
        }
    }

    /**
     * Protected so that it can be overridden in a unit test.
     */
    protected void runJob(SimpleQueryBatcherJob job, ProcessContext context) {
        job.run(getDatabaseClient(context));
    }
}