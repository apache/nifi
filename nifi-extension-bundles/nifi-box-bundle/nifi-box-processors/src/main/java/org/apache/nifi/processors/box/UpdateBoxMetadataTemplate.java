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
package org.apache.nifi.processors.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.MetadataTemplate;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.box.CreateBoxMetadataTemplate.SCOPE_ENTERPRISE;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "metadata", "templates", "update"})
@CapabilityDescription("""
         Updates a Box metadata template using the desired schema from the flowFile content.\s
         Takes in the desired end state of the template, compares it with the existing template, 
         and computes the necessary operations to transform the template to the desired state.
         Admin permissions are required to update templates.
        """)
@SeeAlso({ListBoxFileMetadataTemplates.class, CreateBoxMetadataTemplate.class, UpdateBoxFileMetadataInstance.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.template.key", description = "The template key that was updated"),
        @WritesAttribute(attribute = "box.template.scope", description = "The template scope"),
        @WritesAttribute(attribute = "box.template.operations.count", description = "Number of operations performed on the template"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class UpdateBoxMetadataTemplate extends AbstractProcessor {

    private static final Set<String> VALID_FIELD_TYPES = Set.of("string", "float", "date", "enum", "multiSelect");

    public static final PropertyDescriptor TEMPLATE_KEY = new PropertyDescriptor.Builder()
            .name("Template Key")
            .description("The key of the metadata template to update.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SCOPE = new PropertyDescriptor.Builder()
            .name("Scope")
            .description("The scope of the metadata template. Usually 'enterprise'.")
            .required(true)
            .defaultValue(SCOPE_ENTERPRISE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for parsing the incoming data with the desired template schema")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after a template has been successfully updated.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if an error occurs during template update.")
            .build();

    public static final Relationship REL_TEMPLATE_NOT_FOUND = new Relationship.Builder()
            .name("template not found")
            .description("FlowFiles for which the specified metadata template was not found will be routed to this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_TEMPLATE_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BoxClientService.BOX_CLIENT_SERVICE,
            TEMPLATE_KEY,
            SCOPE,
            RECORD_READER
    );

    private volatile BoxAPIConnection boxAPIConnection;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        boxAPIConnection = getBoxAPIConnection(context);
    }

    protected BoxAPIConnection getBoxAPIConnection(final ProcessContext context) {
        final BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE)
                .asControllerService(BoxClientService.class);
        return boxClientService.getBoxApiConnection();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String templateKey = context.getProperty(TEMPLATE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String scope = context.getProperty(SCOPE).evaluateAttributeExpressions(flowFile).getValue();
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        try {
            // Get the current template
            final MetadataTemplate existingTemplate = getMetadataTemplate(scope, templateKey);

            // Parse the desired state from the flowFile
            final List<FieldDefinition> desiredFields = readDesiredFields(session, flowFile, recordReaderFactory);

            if (desiredFields.isEmpty()) {
                flowFile = session.putAttribute(flowFile, "box.error.message", "No valid metadata field specifications found in the input");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            // Generate operations to transform existing template to desired state
            final List<MetadataTemplate.FieldOperation> operations = generateOperations(existingTemplate, desiredFields);

            if (!operations.isEmpty()) {
                getLogger().info("Updating metadata template {} with {} operations", templateKey, operations.size());
                updateMetadataTemplate(scope, templateKey, operations);
            }

            final Map<String, String> attributes = Map.of(
                    "box.template.key", templateKey,
                    "box.template.scope", scope,
                    "box.template.operations.count", String.valueOf(operations.size()));
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().modifyAttributes(flowFile, "Updated Box metadata template: " + templateKey);
            session.transfer(flowFile, REL_SUCCESS);

        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());

            if (e.getResponseCode() == 404) {
                getLogger().warn("Box metadata template with key {} in scope {} was not found", templateKey, scope);
                session.transfer(flowFile, REL_TEMPLATE_NOT_FOUND);
            } else {
                getLogger().error("Couldn't update metadata template with key [{}]", templateKey, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final Exception e) {
            getLogger().error("Error processing metadata template update", e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private List<FieldDefinition> readDesiredFields(final ProcessSession session,
                                                    final FlowFile flowFile,
                                                    final RecordReaderFactory recordReaderFactory) throws Exception {
        final List<FieldDefinition> fields = new ArrayList<>();
        final Set<String> processedKeys = new HashSet<>();
        final List<String> errors = new ArrayList<>();

        try (final InputStream inputStream = session.read(flowFile);
             final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, inputStream, getLogger())) {

            Record record;
            while ((record = recordReader.nextRecord()) != null) {
                processFieldRecord(record, fields, processedKeys, errors);
            }
        }

        if (!errors.isEmpty()) {
            String errorMessage = "Error parsing field definitions: " + String.join(", ", errors);
            throw new ProcessException(errorMessage);
        }

        return fields;
    }

    private void processFieldRecord(final Record record,
                                    final List<FieldDefinition> fields,
                                    final Set<String> processedKeys,
                                    final List<String> errors) {
        // Extract and validate key (required)
        final Object keyObj = record.getValue("key");
        if (keyObj == null) {
            errors.add("Record is missing a key field");
            return;
        }
        final String key = keyObj.toString();

        if (processedKeys.contains(key)) {
            errors.add("Duplicate key '" + key + "' found in record");
            return;
        }

        // Extract and validate type (required)
        final Object typeObj = record.getValue("type");
        if (typeObj == null) {
            errors.add("Record with key '" + key + "' is missing a type field");
            return;
        }
        final String type = typeObj.toString().toLowerCase();

        if (!VALID_FIELD_TYPES.contains(type)) {
            errors.add("Record with key '" + key + "' has an invalid type: '" + type +
                    "'. Valid types are: " + String.join(", ", VALID_FIELD_TYPES));
            return;
        }

        final FieldDefinition field = new FieldDefinition();
        field.key = key;
        field.type = type;

        final Object displayNameObj = record.getValue("displayName");
        if (displayNameObj != null) {
            field.displayName = displayNameObj.toString();
        }

        final Object hiddenObj = record.getValue("hidden");
        if (hiddenObj != null) {
            field.hidden = Boolean.parseBoolean(hiddenObj.toString());
        }

        final Object descriptionObj = record.getValue("description");
        if (descriptionObj != null) {
            field.description = descriptionObj.toString();
        }

        if ("enum".equals(type) || "multiSelect".equals(type)) {
            final Object optionsObj = record.getValue("options");
            if (optionsObj instanceof List<?> optionsList) {
                field.options = optionsList.stream()
                        .filter(obj -> obj != null)
                        .map(Object::toString)
                        .collect(Collectors.toList());
            }
        }

        fields.add(field);
        processedKeys.add(key);
    }

    private List<MetadataTemplate.FieldOperation> generateOperations(final MetadataTemplate existingTemplate,
                                                                     final List<FieldDefinition> desiredFields) {
        final List<MetadataTemplate.FieldOperation> operations = new ArrayList<>();
        final Map<String, MetadataTemplate.Field> existingFieldsByKey = new HashMap<>();

        // Create a map of existing fields by key for efficient lookup
        for (MetadataTemplate.Field field : existingTemplate.getFields()) {
            existingFieldsByKey.put(field.getKey(), field);
        }

        // Process each desired field
        for (FieldDefinition desiredField : desiredFields) {
            MetadataTemplate.Field existingField = existingFieldsByKey.get(desiredField.key);

            if (existingField == null) {
                // Field doesn't exist - add it
                operations.add(createAddFieldOperation(desiredField));
            } else {
                // Field exists - check if it needs updating
                Map<String, Object> changes = getFieldChanges(existingField, desiredField);
                if (!changes.isEmpty()) {
                    operations.add(createEditFieldOperation(existingField.getKey(), changes));
                }

                // Remove processed field from the map so we can track which fields to remove
                existingFieldsByKey.remove(desiredField.key);
            }
        }

        // Any remaining fields in existingFieldsByKey are not in the desired state - remove them
        for (String keyToRemove : existingFieldsByKey.keySet()) {
            operations.add(createRemoveFieldOperation(keyToRemove));
        }

        return operations;
    }

    private Map<String, Object> getFieldChanges(final MetadataTemplate.Field existingField,
                                                final FieldDefinition desiredField) {
        final Map<String, Object> changes = new HashMap<>();

        // Check if key has changed
        if (!existingField.getKey().equals(desiredField.key)) {
            changes.put("key", desiredField.key);
        }

        // Check if displayName has changed
        if (desiredField.displayName != null &&
                (existingField.getDisplayName() == null || !existingField.getDisplayName().equals(desiredField.displayName))) {
            changes.put("displayName", desiredField.displayName);
        }

        // Check if type has changed (this is a rare case)
        if (!existingField.getType().equals(desiredField.type)) {
            changes.put("type", desiredField.type);
        }

        // Check if hidden state has changed
        if (desiredField.hidden != existingField.getIsHidden()) {
            changes.put("hidden", desiredField.hidden);
        }

        // Check if description has changed
        if (desiredField.description != null &&
                (existingField.getDescription() == null || !existingField.getDescription().equals(desiredField.description))) {
            changes.put("description", desiredField.description);
        }

        // For enum and multiSelect fields, check if options have changed
        if (("enum".equals(desiredField.type) || "multiSelect".equals(desiredField.type)) &&
                desiredField.options != null && !desiredField.options.isEmpty()) {

            List<String> existingOptions = existingField.getOptions();
            if (existingOptions == null || !new HashSet<>(existingOptions).equals(new HashSet<>(desiredField.options))) {
                changes.put("options", desiredField.options);
            }
        }

        return changes;
    }

    private MetadataTemplate.FieldOperation createAddFieldOperation(final FieldDefinition field) {
        final StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"op\":\"addField\",\"data\":{");

        // Add mandatory fields
        jsonBuilder.append("\"key\":\"").append(field.key).append("\",");
        jsonBuilder.append("\"type\":\"").append(field.type).append("\",");

        // Add optional fields
        if (field.displayName != null) {
            jsonBuilder.append("\"displayName\":\"").append(field.displayName).append("\",");
        }

        jsonBuilder.append("\"hidden\":").append(field.hidden);

        if (field.description != null) {
            jsonBuilder.append(",\"description\":\"").append(field.description).append("\"");
        }

        // Add options for enum or multiSelect fields
        if (("enum".equals(field.type) || "multiSelect".equals(field.type)) &&
                field.options != null && !field.options.isEmpty()) {
            jsonBuilder.append(",\"options\":[");

            for (int i = 0; i < field.options.size(); i++) {
                jsonBuilder.append("{\"key\":\"").append(field.options.get(i)).append("\"}");
                if (i < field.options.size() - 1) {
                    jsonBuilder.append(",");
                }
            }

            jsonBuilder.append("]");
        }

        jsonBuilder.append("}}");

        return new MetadataTemplate.FieldOperation(jsonBuilder.toString());
    }

    private MetadataTemplate.FieldOperation createEditFieldOperation(final String fieldKey, final Map<String, Object> changes) {
        final StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\"op\":\"editField\",\"fieldKey\":\"").append(fieldKey).append("\",\"data\":{");

        int i = 0;
        for (Map.Entry<String, Object> entry : changes.entrySet()) {
            if (i > 0) {
                jsonBuilder.append(",");
            }

            jsonBuilder.append("\"").append(entry.getKey()).append("\":");

            if (entry.getValue() instanceof String) {
                jsonBuilder.append("\"").append(entry.getValue()).append("\"");
            } else if (entry.getValue() instanceof Boolean) {
                jsonBuilder.append(entry.getValue());
            } else if (entry.getValue() instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> options = (List<String>) entry.getValue();
                jsonBuilder.append("[");
                for (int j = 0; j < options.size(); j++) {
                    jsonBuilder.append("{\"key\":\"").append(options.get(j)).append("\"}");
                    if (j < options.size() - 1) {
                        jsonBuilder.append(",");
                    }
                }
                jsonBuilder.append("]");
            } else {
                jsonBuilder.append(entry.getValue());
            }

            i++;
        }

        jsonBuilder.append("}}");

        return new MetadataTemplate.FieldOperation(jsonBuilder.toString());
    }

    private MetadataTemplate.FieldOperation createRemoveFieldOperation(final String fieldKey) {
        // Create the operation JSON
        String removeFieldJson = String.format("{\"op\":\"removeField\",\"fieldKey\":\"%s\"}", fieldKey);

        return new MetadataTemplate.FieldOperation(removeFieldJson);
    }

    protected MetadataTemplate getMetadataTemplate(final String scope, final String templateKey) {
        return MetadataTemplate.getMetadataTemplate(boxAPIConnection, scope, templateKey);
    }

    protected void updateMetadataTemplate(final String scope,
                                          final String templateKey,
                                          final List<MetadataTemplate.FieldOperation> operations) {
        MetadataTemplate.updateMetadataTemplate(boxAPIConnection, scope, templateKey, operations);
    }

    private static class FieldDefinition {
        String key;
        String type;
        String displayName;
        boolean hidden;
        String description;
        List<String> options;
    }
}
