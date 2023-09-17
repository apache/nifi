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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nifi.processors.solr.SolrUtils.BASIC_PASSWORD;
import static org.apache.nifi.processors.solr.SolrUtils.BASIC_USERNAME;
import static org.apache.nifi.processors.solr.SolrUtils.COLLECTION;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_PASSWORD;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_PRINCIPAL;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_USER_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.RECORD_WRITER;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_LOCATION;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_MAX_CONNECTIONS;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_MAX_CONNECTIONS_PER_HOST;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_SOCKET_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_TYPE;
import static org.apache.nifi.processors.solr.SolrUtils.SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.ZK_CLIENT_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.ZK_CONNECTION_TIMEOUT;

@Tags({"Apache", "Solr", "Get", "Pull", "Records"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Queries Solr and outputs the results as a FlowFile in the format of XML or using a Record Writer")
@Stateful(scopes = {Scope.CLUSTER}, description = "Stores latest date of Date Field so that the same data will not be fetched multiple times.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class GetSolr extends SolrProcessor {

    public static final String STATE_MANAGER_FILTER = "stateManager_filter";
    public static final String STATE_MANAGER_CURSOR_MARK = "stateManager_cursorMark";
    public static final AllowableValue MODE_XML = new AllowableValue("XML");
    public static final AllowableValue MODE_REC = new AllowableValue("Records");

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor
            .Builder().name("Return Type")
            .displayName("Return Type")
            .description("Write Solr documents to FlowFiles as XML or using a Record Writer")
            .required(true)
            .allowableValues(MODE_XML, MODE_REC)
            .defaultValue(MODE_XML.getValue())
            .build();

    public static final PropertyDescriptor SOLR_QUERY = new PropertyDescriptor
            .Builder().name("Solr Query")
            .displayName("Solr Query")
            .description("A query to execute against Solr")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATE_FIELD = new PropertyDescriptor
            .Builder().name("Date Field")
            .displayName("Date Field")
            .description("The name of a date field in Solr used to filter results")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATE_FILTER = new PropertyDescriptor
            .Builder().name("Initial Date Filter")
            .displayName("Initial Date Filter")
            .description("Date value to filter results. Documents with an earlier date will not be fetched. The format has to correspond to the date pattern of Solr 'YYYY-MM-DDThh:mm:ssZ'")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETURN_FIELDS = new PropertyDescriptor
            .Builder().name("Return Fields")
            .displayName("Return Fields")
            .description("Comma-separated list of field names to return")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder().name("Batch Size")
            .displayName("Batch Size")
            .description("Number of rows per Solr query")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The results of querying Solr")
            .build();

    private final AtomicBoolean clearState = new AtomicBoolean(false);
    private final AtomicBoolean dateFieldNotInSpecifiedFieldsList = new AtomicBoolean(false);
    private volatile String id_field = null;

    private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
    static {
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SOLR_TYPE);
        descriptors.add(SOLR_LOCATION);
        descriptors.add(COLLECTION);
        descriptors.add(RETURN_TYPE);
        descriptors.add(RECORD_WRITER);
        descriptors.add(SOLR_QUERY);
        descriptors.add(DATE_FIELD);
        descriptors.add(DATE_FILTER);
        descriptors.add(RETURN_FIELDS);
        descriptors.add(BATCH_SIZE);
        descriptors.add(KERBEROS_CREDENTIALS_SERVICE);
        descriptors.add(KERBEROS_USER_SERVICE);
        descriptors.add(KERBEROS_PRINCIPAL);
        descriptors.add(KERBEROS_PASSWORD);
        descriptors.add(BASIC_USERNAME);
        descriptors.add(BASIC_PASSWORD);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(SOLR_SOCKET_TIMEOUT);
        descriptors.add(SOLR_CONNECTION_TIMEOUT);
        descriptors.add(SOLR_MAX_CONNECTIONS);
        descriptors.add(SOLR_MAX_CONNECTIONS_PER_HOST);
        descriptors.add(ZK_CLIENT_TIMEOUT);
        descriptors.add(ZK_CONNECTION_TIMEOUT);
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

    final static Set<String> propertyNamesForActivatingClearState = new HashSet<String>();
    static {
        propertyNamesForActivatingClearState.add(SOLR_TYPE.getName());
        propertyNamesForActivatingClearState.add(SOLR_LOCATION.getName());
        propertyNamesForActivatingClearState.add(COLLECTION.getName());
        propertyNamesForActivatingClearState.add(SOLR_QUERY.getName());
        propertyNamesForActivatingClearState.add(DATE_FIELD.getName());
        propertyNamesForActivatingClearState.add(RETURN_FIELDS.getName());
        propertyNamesForActivatingClearState.add(DATE_FILTER.getName());
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (propertyNamesForActivatingClearState.contains(descriptor.getName()))
            clearState.set(true);
    }

    @OnScheduled
    public void clearState(final ProcessContext context) throws IOException {
        if (clearState.getAndSet(false))
            context.getStateManager().clear(Scope.CLUSTER);

        final Map<String,String> stateMap = new HashMap<String,String>();
        stateMap.putAll(context.getStateManager().getState(Scope.CLUSTER).toMap());
        final AtomicBoolean stateMapHasChanged = new AtomicBoolean(false);

        if (stateMap.get(STATE_MANAGER_CURSOR_MARK) == null) {
            stateMap.put(STATE_MANAGER_CURSOR_MARK, "*");
            stateMapHasChanged.set(true);
        }

        if (stateMap.get(STATE_MANAGER_FILTER) == null) {
            final String initialDate = context.getProperty(DATE_FILTER).getValue();
            if (StringUtils.isBlank(initialDate))
                stateMap.put(STATE_MANAGER_FILTER, "*");
            else
                stateMap.put(STATE_MANAGER_FILTER, initialDate);
            stateMapHasChanged.set(true);
        }

        if (stateMapHasChanged.get()) {
            context.getStateManager().setState(stateMap, Scope.CLUSTER);
        }

        id_field = null;
    }

    @Override
    protected final Collection<ValidationResult> additionalCustomValidation(ValidationContext context) {
        final Collection<ValidationResult> problems = new ArrayList<>();

        if (context.getProperty(RETURN_TYPE).evaluateAttributeExpressions().getValue().equals(MODE_REC.getValue())
                && !context.getProperty(RECORD_WRITER).isSet()) {
            problems.add(new ValidationResult.Builder()
                    .explanation("for writing records a record writer has to be configured")
                    .valid(false)
                    .subject("Record writer check")
                    .build());
        }
        return problems;
    }

    private String getFieldNameOfUniqueKey() {
        final SolrQuery solrQuery = new SolrQuery();
        try {
            solrQuery.setRequestHandler("/schema/uniquekey");
            final QueryRequest req = new QueryRequest(solrQuery);
            if (isBasicAuthEnabled()) {
                req.setBasicAuthCredentials(getUsername(), getPassword());
            }

            return(req.process(getSolrClient()).getResponse().get("uniqueKey").toString());
        } catch (SolrServerException | IOException e) {
            getLogger().error("Solr query to retrieve uniqueKey-field failed due to {}", solrQuery.toString(), e, e);
            throw new ProcessException(e);
        }
    }

    @Override
    public void doOnTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ComponentLog logger = getLogger();
        final AtomicBoolean continuePaging = new AtomicBoolean(true);
        final SolrQuery solrQuery = new SolrQuery();

        try {
            if (id_field == null) {
                id_field = getFieldNameOfUniqueKey();
            }

            final String dateField = context.getProperty(DATE_FIELD).getValue();

            final Map<String,String> stateMap = new HashMap<String,String>();
            stateMap.putAll(session.getState(Scope.CLUSTER).toMap());

            solrQuery.setQuery("*:*");
            final String query = context.getProperty(SOLR_QUERY).getValue();
            if (!StringUtils.isBlank(query) && !query.equals("*:*")) {
                solrQuery.addFilterQuery(query);
            }
            final StringBuilder automatedFilterQuery = (new StringBuilder())
                    .append(dateField)
                    .append(":[")
                    .append(stateMap.get(STATE_MANAGER_FILTER))
                    .append(" TO *]");
            solrQuery.addFilterQuery(automatedFilterQuery.toString());

            final List<String> fieldList = new ArrayList<String>();
            final String returnFields = context.getProperty(RETURN_FIELDS).getValue();
            if (!StringUtils.isBlank(returnFields)) {
                fieldList.addAll(Arrays.asList(returnFields.trim().split("[,]")));
                if (!fieldList.contains(dateField)) {
                    fieldList.add(dateField);
                    dateFieldNotInSpecifiedFieldsList.set(true);
                }
                for (String returnField : fieldList) {
                    solrQuery.addField(returnField.trim());
                }
            }

            solrQuery.setParam(CursorMarkParams.CURSOR_MARK_PARAM, stateMap.get(STATE_MANAGER_CURSOR_MARK));
            solrQuery.setRows(context.getProperty(BATCH_SIZE).asInteger());

            final StringBuilder sortClause = (new StringBuilder())
                    .append(dateField)
                    .append(" asc,")
                    .append(id_field)
                    .append(" asc");
            solrQuery.setParam("sort", sortClause.toString());

            while (continuePaging.get()) {
                StopWatch timer = new StopWatch(true);

                final QueryRequest req = new QueryRequest(solrQuery);
                if (isBasicAuthEnabled()) {
                    req.setBasicAuthCredentials(getUsername(), getPassword());
                }

                logger.debug(solrQuery.toQueryString());
                final QueryResponse response = req.process(getSolrClient());
                final SolrDocumentList documentList = response.getResults();

                if (response.getResults().size() > 0) {
                    final SolrDocument lastSolrDocument = documentList.get(response.getResults().size()-1);
                    final String latestDateValue = df.format(lastSolrDocument.get(dateField));
                    final String newCursorMark = response.getNextCursorMark();

                    solrQuery.setParam(CursorMarkParams.CURSOR_MARK_PARAM, newCursorMark);
                    stateMap.put(STATE_MANAGER_CURSOR_MARK, newCursorMark);
                    stateMap.put(STATE_MANAGER_FILTER, latestDateValue);

                    FlowFile flowFile = session.create();
                    flowFile = session.putAttribute(flowFile, "solrQuery", solrQuery.toString());

                    if (context.getProperty(RETURN_TYPE).getValue().equals(MODE_XML.getValue())){
                        if (dateFieldNotInSpecifiedFieldsList.get()) {
                            for (SolrDocument doc : response.getResults()) {
                                doc.removeFields(dateField);
                            }
                        }
                        flowFile = session.write(flowFile, SolrUtils.getOutputStreamCallbackToTransformSolrResponseToXml(response));
                        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/xml");

                    } else {
                        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).evaluateAttributeExpressions()
                                .asControllerService(RecordSetWriterFactory.class);
                        final RecordSchema schema = writerFactory.getSchema(null, null);
                        final RecordSet recordSet = SolrUtils.solrDocumentsToRecordSet(response.getResults(), schema);
                        final StringBuffer mimeType = new StringBuffer();
                        final FlowFile flowFileRef = flowFile;
                        flowFile = session.write(flowFile, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream out) throws IOException {
                                try {
                                    final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, flowFileRef);
                                    writer.write(recordSet);
                                    writer.flush();
                                    mimeType.append(writer.getMimeType());
                                } catch (SchemaNotFoundException e) {
                                    throw new ProcessException("Could not parse Solr response", e);
                                }
                            }
                        });

                        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType.toString());
                    }

                    timer.stop();
                    StringBuilder transitUri = new StringBuilder("solr://");
                    transitUri.append(getSolrLocation());
                    if (getSolrLocation().equals(SolrUtils.SOLR_TYPE_CLOUD.getValue())) {
                        transitUri.append(":").append(context.getProperty(COLLECTION).evaluateAttributeExpressions().getValue());
                    }
                    final long duration = timer.getDuration(TimeUnit.MILLISECONDS);
                    session.getProvenanceReporter().receive(flowFile, transitUri.toString(), duration);

                    session.transfer(flowFile, REL_SUCCESS);
                }
                continuePaging.set(response.getResults().size() == Integer.parseInt(context.getProperty(BATCH_SIZE).getValue()));
            }

            session.setState(stateMap, Scope.CLUSTER);
        } catch (final SolrServerException | SchemaNotFoundException | IOException e) {
            context.yield();
            session.rollback();
            logger.error("Failed to execute query {} due to {}", solrQuery.toString(), e, e);
            throw new ProcessException(e);
        } catch (final Throwable t) {
            context.yield();
            session.rollback();
            logger.error("Failed to execute query {} due to {}", solrQuery.toString(), t, t);
            throw t;
        }
    }



}
