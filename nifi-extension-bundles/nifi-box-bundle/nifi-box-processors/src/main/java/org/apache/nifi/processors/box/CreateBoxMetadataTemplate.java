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
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "metadata", "templates", "create"})
@CapabilityDescription("Creates a Box metadata template using field specifications from the flowFile content.")
@SeeAlso({ListBoxFileMetadataTemplates.class, UpdateBoxFileMetadataInstance.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.template.name", description = "The template name that was created"),
        @WritesAttribute(attribute = "box.template.key", description = "The template key that was created"),
        @WritesAttribute(attribute = "box.template.scope", description = "The template scope."),
        @WritesAttribute(attribute = "box.template.fields.count", description = "Number of fields created for the template"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class CreateBoxMetadataTemplate extends AbstractProcessor {

    public static final String SCOPE_ENTERPRISE = "enterprise";

    private static final Set<String> VALID_FIELD_TYPES = new HashSet<>(Arrays.asList("string", "float", "date", "enum", "multiSelect"));

    public static final PropertyDescriptor TEMPLATE_NAME = new PropertyDescriptor.Builder()
            .name("Template Name")
            .description("The display name of the metadata template to create.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMPLATE_KEY = new PropertyDescriptor.Builder()
            .name("Template Key")
            .description("The key of the metadata template to create (used for API calls).")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HIDDEN = new PropertyDescriptor.Builder()
            .name("Hidden")
            .description("Whether the template should be hidden in the Box UI.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for parsing the incoming data")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final PropertyDescriptor KEY_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("Template Key Field Record Path")
            .description("Specifies the RecordPath to use for getting the field key names.")
            .required(true)
            .addValidator(new RecordPathValidator())
            .defaultValue("/key")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TYPE_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("Template Type Field Record Path")
            .description("Specifies the RecordPath to use for getting the field type (string, float, date).")
            .required(true)
            .addValidator(new RecordPathValidator())
            .defaultValue("/type")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DISPLAY_NAME_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("Template Display Name Field Record Path")
            .description("Specifies the RecordPath to use for getting the field display name. If not specified or if the path doesn't resolve to a value, the key will be used.")
            .required(false)
            .addValidator(new RecordPathValidator())
            .defaultValue("/displayName")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after a template has been successfully created.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if an error occurs during template creation.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BoxClientService.BOX_CLIENT_SERVICE,
            TEMPLATE_NAME,
            TEMPLATE_KEY,
            HIDDEN,
            RECORD_READER,
            KEY_RECORD_PATH,
            TYPE_RECORD_PATH,
            DISPLAY_NAME_RECORD_PATH
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

        final String templateName = context.getProperty(TEMPLATE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String templateKey = context.getProperty(TEMPLATE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final boolean hidden = Boolean.parseBoolean(context.getProperty(HIDDEN).evaluateAttributeExpressions(flowFile).getValue());
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String keyRecordPathStr = context.getProperty(KEY_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final String typeRecordPathStr = context.getProperty(TYPE_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();

        final String displayNameRecordPathStr;
        if (context.getProperty(DISPLAY_NAME_RECORD_PATH).isSet()) {
            displayNameRecordPathStr = context.getProperty(DISPLAY_NAME_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
        } else {
            displayNameRecordPathStr = null;
        }

        try (final InputStream inputStream = session.read(flowFile);
             final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, inputStream, getLogger())) {

            final RecordPath keyRecordPath = RecordPath.compile(keyRecordPathStr);
            final RecordPath typeRecordPath = RecordPath.compile(typeRecordPathStr);
            final RecordPath displayNameRecordPath = displayNameRecordPathStr != null ? RecordPath.compile(displayNameRecordPathStr) : null;

            // Create list to hold fields for the template
            final List<MetadataTemplate.Field> fields = new ArrayList<>();
            final List<String> errors = new ArrayList<>();
            final Set<String> processedKeys = new HashSet<>();

            Record record;
            try {
                while ((record = recordReader.nextRecord()) != null) {
                    processRecord(record, keyRecordPath, typeRecordPath, displayNameRecordPath, fields, processedKeys, errors);
                }
            } catch (final Exception e) {
                getLogger().error("Error processing record: {}", e.getMessage(), e);
                errors.add("Error processing record: " + e.getMessage());
            }

            if (!errors.isEmpty()) {
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, String.join(", ", errors));
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            if (fields.isEmpty()) {
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, "No valid metadata field specifications found in the input");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            // Create the template
            createBoxMetadataTemplate(
                    boxAPIConnection,
                    templateKey,
                    templateName,
                    hidden,
                    fields);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("box.template.name", templateName);
            attributes.put("box.template.key", templateKey);
            attributes.put("box.template.scope", SCOPE_ENTERPRISE);
            attributes.put("box.template.fields.count", String.valueOf(fields.size()));
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().create(flowFile, "Created Box metadata template: " + templateName);
            session.transfer(flowFile, REL_SUCCESS);

        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            getLogger().error("Couldn't create metadata template with name [{}]", templateName, e);
            session.transfer(flowFile, REL_FAILURE);
        } catch (final Exception e) {
            getLogger().error("Error processing metadata template creation", e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void processRecord(final Record record,
                               final RecordPath keyRecordPath,
                               final RecordPath typeRecordPath,
                               final RecordPath displayNameRecordPath,
                               final List<MetadataTemplate.Field> fields,
                               final Set<String> processedKeys,
                               final List<String> errors) {
        final RecordPathResult keyPathResult = keyRecordPath.evaluate(record);
        final List<FieldValue> keyValues = keyPathResult.getSelectedFields().toList();

        if (keyValues.isEmpty()) {
            errors.add("Record is missing a key field");
            return;
        }

        final Object keyObj = keyValues.getFirst().getValue();
        if (keyObj == null) {
            errors.add("Record has a null key value");
            return;
        }

        final String key = keyObj.toString();

        // Skip if we've already processed this key
        if (processedKeys.contains(key)) {
            getLogger().warn("Duplicate key '{}' found in record, skipping", key);
            return;
        }

        final RecordPathResult typePathResult = typeRecordPath.evaluate(record);
        final List<FieldValue> typeValues = typePathResult.getSelectedFields().toList();

        if (typeValues.isEmpty()) {
            errors.add("Record with key '" + key + "' is missing a type field");
            return;
        }

        final Object typeObj = typeValues.getFirst().getValue();
        if (typeObj == null) {
            errors.add("Record with key '" + key + "' has a null type value");
            return;
        }

        final String type = typeObj.toString().toLowerCase();

        // Validate field type
        if (!VALID_FIELD_TYPES.contains(type)) {
            errors.add("Record with key '" + key + "' has an invalid type: '" + type + "'. Valid types are: " +
                    String.join(", ", VALID_FIELD_TYPES));
            return;
        }

        // Get the display name from the record (falls back to key if missing)
        String displayName = key;
        if (displayNameRecordPath != null) {
            final RecordPathResult displayNamePathResult = displayNameRecordPath.evaluate(record);
            final List<FieldValue> displayNameValues = displayNamePathResult.getSelectedFields().toList();

            if (!displayNameValues.isEmpty()) {
                final Object displayNameObj = displayNameValues.getFirst().getValue();
                if (displayNameObj != null) {
                    displayName = displayNameObj.toString();
                }
            }
        }

        // Create and add the field
        final MetadataTemplate.Field metadataField = new MetadataTemplate.Field();
        metadataField.setType(type);
        metadataField.setKey(key);
        metadataField.setDisplayName(displayName);

        fields.add(metadataField);
        processedKeys.add(key);
    }

    protected void createBoxMetadataTemplate(final BoxAPIConnection boxAPIConnection,
                                             final String templateKey,
                                             final String templateName,
                                             final boolean isHidden,
                                             final List<MetadataTemplate.Field> fields) {
        MetadataTemplate.createMetadataTemplate(
                boxAPIConnection,
                CreateBoxMetadataTemplate.SCOPE_ENTERPRISE,
                templateKey,
                templateName,
                isHidden,
                fields);
    }
}
