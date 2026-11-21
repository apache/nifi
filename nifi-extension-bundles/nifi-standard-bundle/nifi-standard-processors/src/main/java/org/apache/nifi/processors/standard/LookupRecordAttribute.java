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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"lookup", "cache", "enrich", "join", "attributes", "database"})
@SeeAlso({LookupAttribute.class, LookupRecord.class})
@CapabilityDescription("""
        Performs a single lookup of a key in a Record Lookup Service and adds a FlowFile attribute for each configured field \
        of the Record returned by the service. Unlike LookupAttribute, which requires a String Lookup Service, this Processor \
        uses a Record Lookup Service, so several values from the same lookup can be added without repeating the lookup.""")
@DynamicProperty(name = "The name of the attribute to add to the FlowFile",
        value = "The name of the field in the looked-up Record whose value is used for the attribute",
        description = "Adds a FlowFile attribute specified by the dynamic property's name with the value of the named field of the Record returned by the lookup service")
public class LookupRecordAttribute extends AbstractProcessor {

    static final PropertyDescriptor LOOKUP_SERVICE = new PropertyDescriptor.Builder()
            .name("Lookup Service")
            .description("The Record Lookup Service to use for the lookup. The service must require exactly one lookup key.")
            .identifiesControllerService(RecordLookupService.class)
            .required(true)
            .build();

    static final PropertyDescriptor LOOKUP_KEY = new PropertyDescriptor.Builder()
            .name("Lookup Key")
            .description("The value to look up in the Lookup Service. It is passed as the value of the single key required by the service.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor INCLUDE_EMPTY_VALUES = new PropertyDescriptor.Builder()
            .name("Include Empty Values")
            .description("Include attributes for fields whose value is null or blank. If false, such attributes are not added.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            LOOKUP_SERVICE,
            LOOKUP_KEY,
            INCLUDE_EMPTY_VALUES
    );

    static final Relationship REL_MATCHED = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles for which at least one attribute was added from a matching lookup are routed to this relationship")
            .build();

    static final Relationship REL_UNMATCHED = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles with no matching lookup, or for which no attribute was added, are routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles with failing lookups are routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_MATCHED, REL_UNMATCHED, REL_FAILURE);

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(validationContext));

        final boolean hasDynamicProperty = validationContext.getProperties().keySet().stream().anyMatch(PropertyDescriptor::isDynamic);
        if (!hasDynamicProperty) {
            errors.add(new ValidationResult.Builder()
                    .subject("User-Defined Properties")
                    .valid(false)
                    .explanation("At least one user-defined property must be specified.")
                    .build());
        }

        if (validationContext.getProperty(LOOKUP_SERVICE).isSet()) {
            final Set<String> requiredKeys = validationContext.getProperty(LOOKUP_SERVICE).asControllerService(RecordLookupService.class).getRequiredKeys();
            if (requiredKeys == null || requiredKeys.size() != 1) {
                errors.add(new ValidationResult.Builder()
                        .subject(LOOKUP_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation("LookupRecordAttribute requires a lookup service supporting exactly one required key.")
                        .build());
            }
        }

        return errors;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordLookupService lookupService = context.getProperty(LOOKUP_SERVICE).asControllerService(RecordLookupService.class);
        final boolean includeEmptyValues = context.getProperty(INCLUDE_EMPTY_VALUES).asBoolean();

        final Map<String, String> fieldsByAttribute = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                fieldsByAttribute.put(entry.getKey().getName(), entry.getValue());
            }
        }

        final Set<String> requiredKeys = lookupService.getRequiredKeys();
        if (requiredKeys == null || requiredKeys.size() != 1) {
            throw new ProcessException("LookupRecordAttribute requires a lookup service supporting exactly one required key, was: "
                    + (requiredKeys == null ? "null" : String.valueOf(requiredKeys.size())));
        }

        final String coordinateKey = requiredKeys.iterator().next();
        final String lookupKey = context.getProperty(LOOKUP_KEY).evaluateAttributeExpressions(flowFile).getValue();

        final Optional<Record> lookedUp;
        try {
            lookedUp = lookupService.lookup(Collections.singletonMap(coordinateKey, lookupKey), flowFile.getAttributes());
        } catch (final LookupFailureException e) {
            getLogger().error("Lookup failed for key [{}]", lookupKey, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (lookedUp.isEmpty()) {
            getLogger().debug("No such value for key: {}", lookupKey);
            session.transfer(flowFile, REL_UNMATCHED);
            return;
        }

        final Record record = lookedUp.get();
        final Map<String, String> attributes = new HashMap<>();
        for (final Map.Entry<String, String> entry : fieldsByAttribute.entrySet()) {
            final String value = record.getAsString(entry.getValue());
            if (StringUtils.isNotBlank(value)) {
                attributes.put(entry.getKey(), value);
            } else if (includeEmptyValues) {
                attributes.put(entry.getKey(), "");
            }
        }

        if (attributes.isEmpty()) {
            session.transfer(flowFile, REL_UNMATCHED);
        } else {
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_MATCHED);
        }
    }
}
