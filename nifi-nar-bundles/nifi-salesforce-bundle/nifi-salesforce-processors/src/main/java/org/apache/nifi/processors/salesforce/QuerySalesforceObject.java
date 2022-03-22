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
package org.apache.nifi.processors.salesforce;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.salesforce.util.SalesforceRestService;
import org.apache.nifi.processors.salesforce.util.SalesforceToRecordSchemaConverter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"salesforce", "sobject", "soql", "query", "select"})
@CapabilityDescription("Retrieves records from a Salesforce SObject. Users can add arbitrary filter conditions by setting the 'Custom WHERE Condition' property."
        + " Supports incremental retrieval: users can define a field in the 'Age Field' property that will be used to determine when the record was created."
        + " When this property is set the processor will retrieve new records. It's also possible to define an initial cutoff value for the age, fitering out all older records"
        + " even for the first run. This processor is intended to be run on the Primary Node only."
        + " FlowFile attribute 'record.count' indicates how many records were retrieved and written to the output.")
@Stateful(scopes = Scope.CLUSTER, description = "When 'Age Field' is set, after performing a query the time of execution is stored. Subsequent queries will be augmented"
        + " with an additional condition so that only records that are newer than the stored execution time (adjusted with the optional value of 'Age Delay') will be retrieved."
        + " State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected,"
        + " the new node can pick up where the previous node left off, without duplicating the data.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer."),
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the FlowFile.")
})
public class QuerySalesforceObject extends AbstractProcessor {

    public static final PropertyDescriptor CUSTOM_WHERE_CONDITION = new PropertyDescriptor.Builder()
            .name("custom-where-condition")
            .displayName("Custom WHERE Condition")
            .description("A custom expression to be added in the WHERE clause of the query.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SOBJECT_NAME = new PropertyDescriptor.Builder()
            .name("sobject-name")
            .displayName("SObject Name")
            .description("The name of the sobject to be queried.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    
    static final PropertyDescriptor FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("field-names")
            .displayName("Field Names")
            .description("A coma-separated list of field names to be used in the query.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor AGE_FIELD = new PropertyDescriptor.Builder()
            .name("age-field")
            .displayName("Age Field")
            .description("The name of a TIMESTAMP field that will be used to limit all and filter already retrieved records."
                    + " Only records that are older than the previous run time of this processor will be retrieved."
            )
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor INITIAL_AGE_FILTER = new PropertyDescriptor.Builder()
            .name("initial-age-filter")
            .displayName("Initial Age Filter")
            .description("When 'Age Field' is set the value of this property will serve as a filter when this processor runs the first time."
                    + " Only records that are older than this value be retrieved."
            )
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor AGE_DELAY = new PropertyDescriptor.Builder()
            .name("age-delay")
            .displayName("Age Delay")
            .description("When 'Age Field' is set the age-based filter will be adjusted by this amount."
                    + " Only records that are older than the previous run time of this processor, by at least this amount, will be retrieved."
            )
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 s")
            .build();

    static final PropertyDescriptor BASE_URL = new PropertyDescriptor.Builder()
            .name("salesforce-base-url")
            .displayName("Base URL")
            .description("The URL of the Salesforce instance.")
            .required(true)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder()
            .name("salesforce-api-version")
            .displayName("API Version")
            .description("The version of the Salesforce REST API.")
            .required(true)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("54.0")
            .build();

    static final PropertyDescriptor AUTH_SERVICE = new PropertyDescriptor.Builder()
            .name("auth-service")
            .displayName("OAuth2 Access Token Provider")
            .description("Controller service to handle Oauth2 authorization.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
            .name("include-zero-record-flowfiles")
            .displayName("Include Zero Record FlowFiles")
            .description("When converting an incoming FlowFile, if the conversion results in no data, "
                    + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final PropertyDescriptor RESPONSE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("salesforce-http-response-timeout")
            .displayName("Response Timeout")
            .description("Max wait time for a response from the Salesforce REST API.")
            .required(true)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful query.")
            .build();

    private static final String LAST_AGE_FILTER = "last_age_filter";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss.SSSX";
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ";

    private volatile SalesforceToRecordSchemaConverter salesForceToRecordSchemaConverter;
    private volatile SalesforceRestService salesforceRestService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        String dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        String timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        String timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();

        salesForceToRecordSchemaConverter = new SalesforceToRecordSchemaConverter(
                dateFormat,
                timestampFormat,
                timeFormat
        );

        String salesforceVersion = context.getProperty(API_VERSION).getValue();
        String baseUrl = context.getProperty(BASE_URL).getValue();
        OAuth2AccessTokenProvider accessTokenProvider = context.getProperty(AUTH_SERVICE).asControllerService(OAuth2AccessTokenProvider.class);

        salesforceRestService = new SalesforceRestService(
                salesforceVersion,
                baseUrl,
                () -> accessTokenProvider.getAccessDetails().getAccessToken(),
                context.getProperty(RESPONSE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue()
        );
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(
                SOBJECT_NAME,
                FIELD_NAMES,
                CUSTOM_WHERE_CONDITION,
                AGE_FIELD,
                INITIAL_AGE_FILTER,
                AGE_DELAY,
                API_VERSION,
                BASE_URL,
                AUTH_SERVICE,
                RECORD_WRITER,
                INCLUDE_ZERO_RECORD_FLOWFILES,
                RESPONSE_TIMEOUT
        ));
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        if (validationContext.getProperty(INITIAL_AGE_FILTER).isSet() && !validationContext.getProperty(AGE_FIELD).isSet()) {
            results.add(
                    new ValidationResult.Builder()
                            .subject(INITIAL_AGE_FILTER.getDisplayName())
                            .valid(false)
                            .explanation("it requires " + AGE_FIELD.getDisplayName() + " also to be set.")
                            .build()
            );
        }
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        String sObject = context.getProperty(SOBJECT_NAME).getValue();
        String fields = context.getProperty(FIELD_NAMES).getValue();
        String customWhereClause = context.getProperty(CUSTOM_WHERE_CONDITION).getValue();
        RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        boolean includeZeroRecordFlowFiles = context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean();

        String ageField = context.getProperty(AGE_FIELD).getValue();
        String initialAgeFilter = context.getProperty(INITIAL_AGE_FILTER).getValue();
        Long ageDelayMs = context.getProperty(AGE_DELAY).asTimePeriod(TimeUnit.MILLISECONDS);

        String ageFilterLower;
        StateMap state;
        try {
            state = context.getStateManager().getState(Scope.CLUSTER);
            ageFilterLower = state.get(LAST_AGE_FILTER);
        } catch (IOException e) {
            throw new ProcessException("Last Age Filter state retrieval failed", e);
        }

        String ageFilterUpper;
        if (ageField == null) {
            ageFilterUpper = null;
        } else {
            Instant ageFilterUpperTime;
            if (ageDelayMs == null) {
                ageFilterUpperTime = Instant.now();
            } else {
                ageFilterUpperTime = Instant.now().minus(ageDelayMs, ChronoUnit.MILLIS);
            }
            ageFilterUpper = DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    .withZone(ZoneId.systemDefault())
                    .format(ageFilterUpperTime);
        }

        String describeSObjectResult = salesforceRestService.describeSObject(sObject);
        ConvertedSalesforceSchema convertedSalesforceSchema = convertSchema(describeSObjectResult, fields);

        String querySObject = buildQuery(
                sObject,
                fields,
                customWhereClause,
                ageField,
                initialAgeFilter,
                ageFilterLower,
                ageFilterUpper
        );
        String querySObjectResult = salesforceRestService.query(querySObject);

        FlowFile flowFile = session.create();

        Map<String, String> originalAttributes = flowFile.getAttributes();
        Map<String, String> attributes = new HashMap<>();

        AtomicInteger recordCountHolder = new AtomicInteger();

        flowFile = session.write(flowFile, out -> {
            InputStream querySObjectResultInputStream = new ByteArrayInputStream(querySObjectResult.getBytes());
            try (
                    JsonTreeRowRecordReader jsonReader = new JsonTreeRowRecordReader(
                            querySObjectResultInputStream,
                            getLogger(),
                            convertedSalesforceSchema.querySObjectResultSchema,
                            DATE_FORMAT,
                            TIME_FORMAT,
                            DATE_TIME_FORMAT
                    );

                    RecordSetWriter writer = writerFactory.createWriter(
                            getLogger(),
                            writerFactory.getSchema(
                                    originalAttributes,
                                    convertedSalesforceSchema.recordSchema
                            ),
                            out,
                            originalAttributes
                    )
            ) {
                writer.beginRecordSet();

                Record querySObjectRecord;
                while ((querySObjectRecord = jsonReader.nextRecord()) != null) {
                    Object[] recordObjectList = querySObjectRecord.getAsArray("records");
                    for (Object recordObject : recordObjectList) {
                        writer.write((Record) recordObject);
                    }
                }

                WriteResult writeResult = writer.finishRecordSet();

                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());

                recordCountHolder.set(writeResult.getRecordCount());

                if (ageFilterUpper != null) {
                    Map<String, String> newState = new HashMap<>(state.toMap());
                    newState.put(LAST_AGE_FILTER, ageFilterUpper);
                    updateState(context, newState);
                }
            } catch (SchemaNotFoundException e) {
                throw new ProcessException("Couldn't create record writer", e);
            } catch (MalformedRecordException e) {
                throw new ProcessException("Couldn't read records from input", e);
            }
        });

        int recordCount = recordCountHolder.get();

        if (!includeZeroRecordFlowFiles && recordCount == 0) {
            session.remove(flowFile);
        } else {
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);

            session.adjustCounter("Records Processed", recordCount, false);
            getLogger().info("Successfully written {} records for {}", recordCount, flowFile);
        }
    }

    private void updateState(ProcessContext context, Map<String, String> newState) {
        try {
            context.getStateManager().setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Last Age Filter state update failed", e);
        }
    }

    protected ConvertedSalesforceSchema convertSchema(String describeSObjectResult, String fields) {
        try {
            RecordSchema recordSchema = salesForceToRecordSchemaConverter.convertSchema(describeSObjectResult, fields);

            RecordSchema querySObjectResultSchema = new SimpleRecordSchema(Collections.singletonList(
                    new RecordField("records", RecordFieldType.ARRAY.getArrayDataType(
                            RecordFieldType.RECORD.getRecordDataType(
                                    recordSchema
                            )
                    ))
            ));

            return new ConvertedSalesforceSchema(querySObjectResultSchema, recordSchema);
        } catch (IOException e) {
            throw new ProcessException("SObject to Record schema conversion failed", e);
        }
    }

    protected String buildQuery(
            String sObject,
            String fields,
            String customWhereClause,
            String ageField,
            String initialAgeFilter,
            String ageFilterLower,
            String ageFilterUpper
    ) {
        StringBuilder queryBuilder = new StringBuilder("SELECT ")
                .append(fields)
                .append(" FROM ")
                .append(sObject);

        List<String> whereItems = new ArrayList<>();
        if (customWhereClause != null) {
            whereItems.add("( " + customWhereClause + " )");
        }

        if (ageField != null) {
            if (ageFilterLower != null) {
                whereItems.add(ageField + " >= " + ageFilterLower);
            } else if (initialAgeFilter != null) {
                whereItems.add(ageField + " >= " + initialAgeFilter);
            }

            whereItems.add(ageField + " < " + ageFilterUpper);
        }

        if (!whereItems.isEmpty()) {
            String finalWhereClause = String.join(" AND ", whereItems);
            queryBuilder.append(" WHERE ").append(finalWhereClause);
        }

        return queryBuilder.toString();
    }

    static class ConvertedSalesforceSchema {
        RecordSchema querySObjectResultSchema;
        RecordSchema recordSchema;

        public ConvertedSalesforceSchema(RecordSchema querySObjectResultSchema, RecordSchema recordSchema) {
            this.querySObjectResultSchema = querySObjectResultSchema;
            this.recordSchema = recordSchema;
        }
    }
}
