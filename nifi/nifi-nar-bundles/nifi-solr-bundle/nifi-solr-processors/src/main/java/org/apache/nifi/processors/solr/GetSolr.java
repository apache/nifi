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
package org.apache.nifi.processors.solr;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Tags({"Apache", "Solr", "Get", "Pull"})
@CapabilityDescription("Queries Solr and outputs the results as a FlowFile")
public class GetSolr extends SolrProcessor {

    public static final PropertyDescriptor SOLR_QUERY = new PropertyDescriptor
            .Builder().name("Solr Query")
            .description("A query to execute against Solr")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETURN_FIELDS = new PropertyDescriptor
            .Builder().name("Return Fields")
            .description("Comma-separated list of fields names to return")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SORT_CLAUSE = new PropertyDescriptor
            .Builder().name("Sort Clause")
            .description("A Solr sort clause, ex: field1 asc, field2 desc")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATE_FIELD = new PropertyDescriptor
            .Builder().name("Date Field")
            .description("The name of a date field in Solr used to filter results")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder().name("Batch Size")
            .description("Number of rows per Solr query")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The results of querying Solr")
            .build();

    static final String FILE_PREFIX = "conf/.getSolr-";
    static final String LAST_END_DATE = "LastEndDate";
    static final String LAST_END_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    static final String UNINITIALIZED_LAST_END_DATE_VALUE;

    static {
        SimpleDateFormat sdf = new SimpleDateFormat(LAST_END_DATE_PATTERN, Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        UNINITIALIZED_LAST_END_DATE_VALUE = sdf.format(new Date(1L));
    }

    final AtomicReference<String> lastEndDatedRef = new AtomicReference<>(UNINITIALIZED_LAST_END_DATE_VALUE);

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private final Lock fileLock = new ReentrantLock();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SOLR_TYPE);
        descriptors.add(SOLR_LOCATION);
        descriptors.add(COLLECTION);
        descriptors.add(SOLR_QUERY);
        descriptors.add(RETURN_FIELDS);
        descriptors.add(SORT_CLAUSE);
        descriptors.add(DATE_FIELD);
        descriptors.add(BATCH_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        lastEndDatedRef.set(UNINITIALIZED_LAST_END_DATE_VALUE);
    }

    @OnShutdown
    public void onShutdown() {
        writeLastEndDate();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ProcessorLog logger = getLogger();
        readLastEndDate();

        final SimpleDateFormat sdf = new SimpleDateFormat(LAST_END_DATE_PATTERN, Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        final String currDate = sdf.format(new Date());

        final boolean initialized = !UNINITIALIZED_LAST_END_DATE_VALUE.equals(lastEndDatedRef.get());

        final String query = context.getProperty(SOLR_QUERY).getValue();
        final SolrQuery solrQuery = new SolrQuery(query);
        solrQuery.setRows(context.getProperty(BATCH_SIZE).asInteger());

        // if initialized then apply a filter to restrict results from the last end time til now
        if (initialized) {
            StringBuilder filterQuery = new StringBuilder();
            filterQuery.append(context.getProperty(DATE_FIELD).getValue())
                    .append(":{").append(lastEndDatedRef.get()).append(" TO ")
                    .append(currDate).append("]");
            solrQuery.addFilterQuery(filterQuery.toString());
            logger.info("Applying filter query {}", new Object[]{filterQuery.toString()});
        }

        final String returnFields = context.getProperty(RETURN_FIELDS).getValue();
        if (returnFields != null && !returnFields.trim().isEmpty()) {
            for (String returnField : returnFields.trim().split("[,]")) {
                solrQuery.addField(returnField.trim());
            }
        }

        final String fullSortClause = context.getProperty(SORT_CLAUSE).getValue();
        if (fullSortClause != null && !fullSortClause.trim().isEmpty()) {
            for (String sortClause : fullSortClause.split("[,]")) {
                String[] sortParts = sortClause.trim().split("[ ]");
                solrQuery.addSort(sortParts[0], SolrQuery.ORDER.valueOf(sortParts[1]));
            }
        }

        try {
            // run the initial query and send out the first page of results
            final StopWatch stopWatch = new StopWatch(true);
            QueryResponse response = getSolrClient().query(solrQuery);
            stopWatch.stop();

            long duration = stopWatch.getDuration(TimeUnit.MILLISECONDS);

            final SolrDocumentList documentList = response.getResults();
            logger.info("Retrieved {} results from Solr for {} in {} ms",
                    new Object[] {documentList.getNumFound(), query, duration});

            if (documentList != null && documentList.getNumFound() > 0) {
                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, new QueryResponseOutputStreamCallback(response));
                session.transfer(flowFile, REL_SUCCESS);

                StringBuilder transitUri = new StringBuilder("solr://");
                transitUri.append(context.getProperty(SOLR_LOCATION).getValue());
                if (SOLR_TYPE_CLOUD.equals(context.getProperty(SOLR_TYPE).getValue())) {
                    transitUri.append("/").append(context.getProperty(COLLECTION).getValue());
                }

                session.getProvenanceReporter().receive(flowFile, transitUri.toString(), duration);

                // if initialized then page through the results and send out each page
                if (initialized) {
                    int endRow = response.getResults().size();
                    long totalResults = response.getResults().getNumFound();

                    while (endRow < totalResults) {
                        solrQuery.setStart(endRow);

                        stopWatch.start();
                        response = getSolrClient().query(solrQuery);
                        stopWatch.stop();

                        duration = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                        logger.info("Retrieved results for {} in {} ms", new Object[]{query, duration});

                        flowFile = session.create();
                        flowFile = session.write(flowFile, new QueryResponseOutputStreamCallback(response));
                        session.transfer(flowFile, REL_SUCCESS);
                        session.getProvenanceReporter().receive(flowFile, transitUri.toString(), duration);
                        endRow += response.getResults().size();
                    }
                }
            }

            lastEndDatedRef.set(currDate);
            writeLastEndDate();
        } catch (SolrServerException | IOException e) {
            context.yield();
            session.rollback();
            logger.error("Failed to execute query {} due to {}", new Object[]{query, e}, e);
            throw new ProcessException(e);
        } catch (final Throwable t) {
            context.yield();
            session.rollback();
            logger.error("Failed to execute query {} due to {}", new Object[]{query, t}, t);
            throw t;
        }
    }

    private void readLastEndDate() {
        fileLock.lock();
        File lastEndDateCache = new File(FILE_PREFIX + getIdentifier());
        try (FileInputStream fis = new FileInputStream(lastEndDateCache)) {
            Properties props = new Properties();
            props.load(fis);
            lastEndDatedRef.set(props.getProperty(LAST_END_DATE));
        } catch (IOException swallow) {
        } finally {
            fileLock.unlock();
        }
    }

    private void writeLastEndDate() {
        fileLock.lock();
        File lastEndDateCache = new File(FILE_PREFIX + getIdentifier());
        try (FileOutputStream fos = new FileOutputStream(lastEndDateCache)) {
            Properties props = new Properties();
            props.setProperty(LAST_END_DATE, lastEndDatedRef.get());
            props.store(fos, "GetSolr LastEndDate value");
        } catch (IOException e) {
            getLogger().error("Failed to persist LastEndDate due to " + e, e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * Writes each SolrDocument in XML format to the OutputStream.
     */
    private class QueryResponseOutputStreamCallback implements OutputStreamCallback {
        private QueryResponse response;

        public QueryResponseOutputStreamCallback(QueryResponse response) {
            this.response = response;
        }

        @Override
        public void process(OutputStream out) throws IOException {
            for (SolrDocument doc : response.getResults()) {
                String xml = ClientUtils.toXML(ClientUtils.toSolrInputDocument(doc));
                IOUtils.write(xml, out);
            }
        }
    }
}
