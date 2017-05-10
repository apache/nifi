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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
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
@Tags({"lookup", "enrich", "route", "record", "csv", "json", "avro", "logs", "convert", "filter"})
@CapabilityDescription("Extracts a field from a Record and looks up its value in a LookupService. If a result is returned by the LookupService, "
    + "that result is optionally added to the Record. In this case, the processor functions as an Enrichment processor. Regardless, the Record is then "
    + "routed to either the 'matched' relationship or 'unmatched' relationship, indicating whether or not a result was returned by the LookupService, "
    + "allowing the processor to also function as a Routing processor. If any record in the incoming FlowFile has multiple fields match the configured "
    + "Lookup RecordPath or if no fields match, then that record will be routed to failure. If one or more fields match the Result RecordPath, all fields "
    + "that match will be updated.")
@SeeAlso({ConvertRecord.class, SplitRecord.class})
public class LookupRecord extends AbstractRouteRecord<Tuple<RecordPath, RecordPath>> {

    private volatile RecordPathCache recordPathCache = new RecordPathCache(25);
    private volatile LookupService<?> lookupService;

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

    static final Relationship REL_MATCHED = new Relationship.Builder()
        .name("matched")
        .description("All records for which the lookup returns a value will be routed to this relationship")
        .build();
    static final Relationship REL_UNMATCHED = new Relationship.Builder()
        .name("unmatched")
        .description("All records for which the lookup does not have a matching value will be routed to this relationship")
        .build();

    private static final Set<Relationship> MATCHED_COLLECTION = Collections.singleton(REL_MATCHED);
    private static final Set<Relationship> UNMATCHED_COLLECTION = Collections.singleton(REL_UNMATCHED);
    private static final Set<Relationship> FAILURE_COLLECTION = Collections.singleton(REL_FAILURE);


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.lookupService = context.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class);
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCHED);
        relationships.add(REL_UNMATCHED);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.addAll(super.getSupportedPropertyDescriptors());
        properties.add(LOOKUP_SERVICE);
        properties.add(LOOKUP_RECORD_PATH);
        properties.add(RESULT_RECORD_PATH);
        return properties;
    }

    @Override
    protected Set<Relationship> route(final Record record, final RecordSchema writeSchema, final FlowFile flowFile, final ProcessContext context,
        final Tuple<RecordPath, RecordPath> flowFileContext) {

        final RecordPathResult lookupPathResult = flowFileContext.getKey().evaluate(record);
        final List<FieldValue> lookupFieldValues = lookupPathResult.getSelectedFields()
            .filter(fieldVal -> fieldVal.getValue() != null)
            .collect(Collectors.toList());
        if (lookupFieldValues.isEmpty()) {
            getLogger().error("Lookup RecordPath did not match any fields in a record for {}; routing record to failure", new Object[] {flowFile});
            return FAILURE_COLLECTION;
        }

        if (lookupFieldValues.size() > 1) {
            getLogger().error("Lookup RecordPath matched {} fields in a record for {}; routing record to failure", new Object[] {lookupFieldValues.size(), flowFile});
            return FAILURE_COLLECTION;
        }

        final FieldValue fieldValue = lookupFieldValues.get(0);
        final String lookupKey = DataTypeUtils.toString(fieldValue.getValue(), (String) null);

        final Optional<?> lookupValue;
        try {
            lookupValue = lookupService.lookup(lookupKey);
        } catch (final Exception e) {
            getLogger().error("Failed to lookup value '{}' in Lookup Service for a record in {}; routing record to failure", new Object[] {lookupKey, flowFile, e});
            return Collections.singleton(REL_FAILURE);
        }

        if (!lookupValue.isPresent()) {
            return UNMATCHED_COLLECTION;
        }

        // Ensure that the Record has the appropriate schema to account for the newly added values
        final RecordPath resultPath = flowFileContext.getValue();
        if (resultPath != null) {
            record.incorporateSchema(writeSchema);

            final Object replacementValue = lookupValue.get();
            final RecordPathResult resultPathResult = flowFileContext.getValue().evaluate(record);
            resultPathResult.getSelectedFields().forEach(fieldVal -> fieldVal.updateValue(replacementValue));
        }

        return MATCHED_COLLECTION;
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
