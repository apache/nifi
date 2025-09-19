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
import com.box.sdk.BoxFile;
import com.box.sdk.Metadata;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.box.utils.BoxDate;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.io.InputStream;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "metadata", "templates", "update"})
@CapabilityDescription("""
         Updates metadata template values for a Box file using the record in the given flowFile.\s
         This record represents the desired end state of the template after the update.\s
         The processor will calculate the necessary changes (add/replace/remove) to transform
         the current metadata to the desired state. The input record should be a flat key-value object.
        """)
@SeeAlso({ListBoxFileMetadataTemplates.class, ListBoxFile.class, FetchBoxFile.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.id", description = "The ID of the file whose metadata was updated"),
        @WritesAttribute(attribute = "box.template.name", description = "The template name used for metadata update"),
        @WritesAttribute(attribute = "box.template.scope", description = "The template scope used for metadata update"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class UpdateBoxFileMetadataInstance extends AbstractBoxProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the file for which to update metadata.")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMPLATE_KEY = new PropertyDescriptor.Builder()
            .name("Template Key")
            .description("The key of the metadata template to update.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for parsing the incoming data")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after metadata has been successfully updated.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if an error occurs during metadata update.")
            .build();

    public static final Relationship REL_FILE_NOT_FOUND = new Relationship.Builder()
            .name("file not found")
            .description("FlowFiles for which the specified Box file was not found will be routed to this relationship.")
            .build();

    public static final Relationship REL_TEMPLATE_NOT_FOUND = new Relationship.Builder()
            .name("template not found")
            .description("FlowFiles for which the specified metadata template was not found will be routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            FILE_ID,
            TEMPLATE_KEY,
            RECORD_READER
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_FILE_NOT_FOUND,
            REL_TEMPLATE_NOT_FOUND
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
        final BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE)
                .asControllerService(BoxClientService.class);
        boxAPIConnection = boxClientService.getBoxApiConnection();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String templateKey = context.getProperty(TEMPLATE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        try {
            final BoxFile boxFile = getBoxFile(fileId);
            final Map<String, Object> desiredState = readDesiredState(session, flowFile, recordReaderFactory);

            if (desiredState.isEmpty()) {
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, "No valid metadata key-value pairs found in the input");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final Metadata metadata = getMetadata(boxFile, templateKey);
            updateMetadata(metadata, desiredState);

            if (!metadata.getOperations().isEmpty()) {
                getLogger().info("Updating {} metadata fields for file {}", metadata.getOperations().size(), fileId);
                updateBoxFileMetadata(boxFile, metadata);
            }

            final Map<String, String> attributes = Map.of(
                    "box.id", fileId,
                    "box.template.key", templateKey);
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().modifyAttributes(flowFile, "%s%s/metadata/enterprise/%s".formatted(BoxFileUtils.BOX_URL, fileId, templateKey));
            session.transfer(flowFile, REL_SUCCESS);

        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            final String errorBody = e.getResponse();
            if (errorBody != null && errorBody.toLowerCase().contains("specified metadata template not found")) {
                getLogger().warn("Box metadata template with key {} was not found.", templateKey);
                session.transfer(flowFile, REL_TEMPLATE_NOT_FOUND);
            } else {
                getLogger().warn("Box file with ID {} was not found.", fileId);
                session.transfer(flowFile, REL_FILE_NOT_FOUND);
            }
        } catch (Exception e) {
            getLogger().error("Error processing metadata update for Box file [{}]", fileId, e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private Map<String, Object> readDesiredState(final ProcessSession session,
                                                 final FlowFile flowFile,
                                                 final RecordReaderFactory recordReaderFactory) throws Exception {
        final Map<String, Object> desiredState = new HashMap<>();

        try (final InputStream inputStream = session.read(flowFile);
             final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, inputStream, getLogger())) {

            final Record record = recordReader.nextRecord();
            if (record != null) {
                final List<RecordField> fields = record.getSchema().getFields();
                for (final RecordField field : fields) {
                    final String fieldName = field.getFieldName();
                    final RecordFieldType type = field.getDataType().getFieldType();

                    final Object value = RecordFieldType.DATE.equals(type)
                            ? record.getAsLocalDate(fieldName, null) // Ensuring dates are read as LocalDate.
                            : record.getValue(field);

                    desiredState.put(fieldName, value);
                }
            }
        }

        return desiredState;
    }

    private void updateMetadata(final Metadata metadata,
                                final Map<String, Object> desiredState) {
        final List<String> currentKeys = metadata.getPropertyPaths();

        // Remove fields not in desired state
        for (final String propertyPath : currentKeys) {
            final String fieldName = propertyPath.substring(1); // Remove leading '/'

            if (!desiredState.containsKey(fieldName)) {
                metadata.remove(propertyPath);
                getLogger().debug("Removing metadata field: {}", fieldName);
            }
        }

        // Add or update fields
        for (final Map.Entry<String, Object> entry : desiredState.entrySet()) {
            final String fieldName = entry.getKey();
            final Object value = entry.getValue();
            final String propertyPath = "/" + fieldName;

            updateField(metadata, propertyPath, value, currentKeys.contains(propertyPath));
        }
    }

    private void updateField(final Metadata metadata,
                             final String propertyPath,
                             final Object value,
                             final boolean exists) {
        if (value == null) {
            throw new IllegalArgumentException("Null value found for property path: " + propertyPath);
        }

        if (exists) {
            final Object currentValue = metadata.getValue(propertyPath);

            // Only update if values are different
            if (Objects.equals(currentValue, value)) {
                return;
            }

            // Update
            switch (value) {
                case Number n -> metadata.replace(propertyPath, n.doubleValue());
                case List<?> l -> metadata.replace(propertyPath, convertListToStringList(l, propertyPath));
                case LocalDate d -> metadata.replace(propertyPath, BoxDate.of(d).format());
                default -> metadata.replace(propertyPath, value.toString());
            }
        } else {
            // Add new field
            switch (value) {
                case Number n -> metadata.add(propertyPath, n.doubleValue());
                case List<?> l -> metadata.add(propertyPath, convertListToStringList(l, propertyPath));
                case LocalDate d -> metadata.add(propertyPath, BoxDate.of(d).format());
                default -> metadata.add(propertyPath, value.toString());
            }
        }
    }

    private List<String> convertListToStringList(final List<?> list,
                                                 final String fieldName) {
        return list.stream()
                .map(obj -> {
                    if (obj == null) {
                        throw new IllegalArgumentException("Null value found in list for field: " + fieldName);
                    }
                    return obj.toString();
                })
                .collect(Collectors.toList());
    }

    /**
     * Retrieves the metadata for a Box file.
     * Visible for testing purposes.
     *
     * @param boxFile     The Box file to retrieve metadata from.
     * @param templateKey The key of the metadata template.
     * @return The metadata for the Box file.
     */
    Metadata getMetadata(final BoxFile boxFile,
                         final String templateKey) {
        return boxFile.getMetadata(templateKey);
    }

    /**
     * Returns a BoxFile object for the given file ID.
     *
     * @param fileId The ID of the file.
     * @return A BoxFile object for the given file ID.
     */
    BoxFile getBoxFile(final String fileId) {
        return new BoxFile(boxAPIConnection, fileId);
    }

    /**
     * Updates the metadata for a Box file.
     *
     * @param boxFile  The Box file to update.
     * @param metadata The metadata to update.
     */
    void updateBoxFileMetadata(final BoxFile boxFile, final Metadata metadata) {
        boxFile.updateMetadata(metadata);
    }
}
