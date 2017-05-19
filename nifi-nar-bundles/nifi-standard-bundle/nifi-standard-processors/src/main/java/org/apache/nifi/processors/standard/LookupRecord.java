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

package org.apache.nifi.processors.standard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.Tuple;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile")
})
@Tags({"lookup", "enrichment", "route", "record", "csv", "json", "avro", "logs", "convert", "filter"})
@CapabilityDescription("Extracts a field from a Record and looks up its value in a LookupService. If a result is returned by the LookupService, "
    + "that result is optionally added to the Record. In this case, the processor functions as an Enrichment processor. Regardless, the Record is then "
    + "routed to either the 'matched' relationship or 'unmatched' relationship (if the 'Routing Strategy' property is configured to do so), "
    + "indicating whether or not a result was returned by the LookupService, "
    + "allowing the processor to also function as a Routing processor. If any record in the incoming FlowFile has multiple fields match the configured "
    + "Lookup RecordPath or if no fields match, then that record will be routed to 'unmatched' (or 'success', depending on the configuration of the 'Routing Strategy' property). "
    + "If one or more fields match the Result RecordPath, all fields "
    + "that match will be updated. If there is no match in the configured LookupService, then no fields will be updated. I.e., it will not overwrite an existing value in the Record "
    + "with a null value. Please note, however, that if the results returned by the LookupService are not accounted for in your schema (specifically, "
    + "the schema that is configured for your Record Writer) then the fields will not be written out to the FlowFile.")
@SeeAlso(value = {ConvertRecord.class, SplitRecord.class}, classNames = {"org.apache.nifi.lookup.SimpleKeyValueLookupService", "org.apache.nifi.lookup.maxmind.IPLookupService"})
public class LookupRecord extends AbstractRouteRecord<Tuple<RecordPath, RecordPath>> {

    private volatile RecordPathCache recordPathCache = new RecordPathCache(25);
    private volatile LookupService<?> lookupService;

    static final AllowableValue ROUTE_TO_SUCCESS = new AllowableValue("route-to-success", "Route to 'success'",
        "Records will be routed to a 'success' Relationship regardless of whether or not there is a match in the configured Lookup Service");
    static final AllowableValue ROUTE_TO_MATCHED_UNMATCHED = new AllowableValue("route-to-matched-unmatched", "Route to 'matched' or 'unmatched'",
        "Records will be routed to either a 'matched' or an 'unmatched' Relationship depending on whether or not there was a match in the configured Lookup Service. "
            + "A single input FlowFile may result in two different output FlowFiles.");

    static final PropertyDescriptor LOOKUP_SERVICE = new PropertyDescriptor.Builder()
        .name("lookup-service")
        .displayName("Lookup Service")
        .description("The Lookup Service to use in order to lookup a value in each Record")
        .identifiesControllerService(LookupService.class)
        .required(true)
        .build();

    static final PropertyDescriptor LOOKUP_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("lookup-record-path")
        .displayName("Lookup RecordPath")
        .description("A RecordPath that points to the field whose value will be looked up in the configured Lookup Service")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(true)
        .required(true)
        .build();

    static final PropertyDescriptor RESULT_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("result-record-path")
        .displayName("Result RecordPath")
        .description("A RecordPath that points to the field whose value should be updated with whatever value is returned from the Lookup Service. "
            + "If not specified, the value that is returned from the Lookup Service will be ignored, except for determining whether the FlowFile should "
            + "be routed to the 'matched' or 'unmatched' Relationship.")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(true)
        .required(false)
        .build();

    static final PropertyDescriptor ROUTING_STRATEGY = new PropertyDescriptor.Builder()
        .name("routing-strategy")
        .displayName("Routing Strategy")
        .description("Specifies how to route records after a Lookup has completed")
        .expressionLanguageSupported(false)
        .allowableValues(ROUTE_TO_SUCCESS, ROUTE_TO_MATCHED_UNMATCHED)
        .defaultValue(ROUTE_TO_SUCCESS.getValue())
        .required(true)
        .build();

    static final Relationship REL_MATCHED = new Relationship.Builder()
        .name("matched")
        .description("All records for which the lookup returns a value will be routed to this relationship")
        .build();
    static final Relationship REL_UNMATCHED = new Relationship.Builder()
        .name("unmatched")
        .description("All records for which the lookup does not have a matching value will be routed to this relationship")
        .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All records will be sent to this Relationship if configured to do so, unless a failure occurs")
        .build();

    private static final Set<Relationship> MATCHED_COLLECTION = Collections.singleton(REL_MATCHED);
    private static final Set<Relationship> UNMATCHED_COLLECTION = Collections.singleton(REL_UNMATCHED);
    private static final Set<Relationship> SUCCESS_COLLECTION = Collections.singleton(REL_SUCCESS);

    private volatile Set<Relationship> relationships = new HashSet<>(Arrays.asList(new Relationship[] {REL_SUCCESS, REL_FAILURE}));
    private volatile boolean routeToMatchedUnmatched = false;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.lookupService = context.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.addAll(super.getSupportedPropertyDescriptors());
        properties.add(LOOKUP_SERVICE);
        properties.add(LOOKUP_RECORD_PATH);
        properties.add(RESULT_RECORD_PATH);
        properties.add(ROUTING_STRATEGY);
        return properties;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (ROUTING_STRATEGY.equals(descriptor)) {
            if (ROUTE_TO_MATCHED_UNMATCHED.getValue().equalsIgnoreCase(newValue)) {
                final Set<Relationship> matchedUnmatchedRels = new HashSet<>();
                matchedUnmatchedRels.add(REL_MATCHED);
                matchedUnmatchedRels.add(REL_UNMATCHED);
                matchedUnmatchedRels.add(REL_FAILURE);
                this.relationships = matchedUnmatchedRels;

                this.routeToMatchedUnmatched = true;
            } else {
                final Set<Relationship> successRels = new HashSet<>();
                successRels.add(REL_SUCCESS);
                successRels.add(REL_FAILURE);
                this.relationships = successRels;

                this.routeToMatchedUnmatched = false;
            }
        }
    }

    @Override
    protected Set<Relationship> route(final Record record, final RecordSchema writeSchema, final FlowFile flowFile, final ProcessContext context,
        final Tuple<RecordPath, RecordPath> flowFileContext) {

        final RecordPathResult lookupPathResult = flowFileContext.getKey().evaluate(record);
        final List<FieldValue> lookupFieldValues = lookupPathResult.getSelectedFields()
            .filter(fieldVal -> fieldVal.getValue() != null)
            .collect(Collectors.toList());

        if (lookupFieldValues.isEmpty()) {
            final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
            getLogger().debug("Lookup RecordPath did not match any fields in a record for {}; routing record to " + rels, new Object[] {flowFile});
            return rels;
        }

        if (lookupFieldValues.size() > 1) {
            final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
            getLogger().debug("Lookup RecordPath matched {} fields in a record for {}; routing record to " + rels, new Object[] {lookupFieldValues.size(), flowFile});
            return rels;
        }

        final FieldValue fieldValue = lookupFieldValues.get(0);
        final String lookupKey = DataTypeUtils.toString(fieldValue.getValue(), (String) null);

        final Optional<?> lookupValue;
        try {
            lookupValue = lookupService.lookup(lookupKey);
        } catch (final Exception e) {
            throw new ProcessException("Failed to lookup value '" + lookupKey + "' in Lookup Service", e);
        }

        if (!lookupValue.isPresent()) {
            final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
            return rels;
        }

        // Ensure that the Record has the appropriate schema to account for the newly added values
        final RecordPath resultPath = flowFileContext.getValue();
        if (resultPath != null) {
            record.incorporateSchema(writeSchema);

            final Object replacementValue = lookupValue.get();
            final RecordPathResult resultPathResult = flowFileContext.getValue().evaluate(record);
            resultPathResult.getSelectedFields().forEach(fieldVal -> fieldVal.updateValue(replacementValue));
        }

        final Set<Relationship> rels = routeToMatchedUnmatched ? MATCHED_COLLECTION : SUCCESS_COLLECTION;
        return rels;
    }

    @Override
    protected boolean isRouteOriginal() {
        return false;
    }

    @Override
    protected Tuple<RecordPath, RecordPath> getFlowFileContext(final FlowFile flowFile, final ProcessContext context) {
        final String lookupPathText = context.getProperty(LOOKUP_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final RecordPath lookupRecordPath = recordPathCache.getCompiled(lookupPathText);

        final RecordPath resultRecordPath;
        if (context.getProperty(RESULT_RECORD_PATH).isSet()) {
            final String resultPathText = context.getProperty(RESULT_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
            resultRecordPath = recordPathCache.getCompiled(resultPathText);
        } else {
            resultRecordPath = null;
        }

        return new Tuple<>(lookupRecordPath, resultRecordPath);
    }
}
