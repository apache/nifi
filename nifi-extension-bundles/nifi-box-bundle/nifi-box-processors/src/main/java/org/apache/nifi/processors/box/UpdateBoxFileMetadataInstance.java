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

import com.box.sdkgen.box.errors.BoxAPIError;
import com.box.sdkgen.client.BoxClient;
import com.box.sdkgen.managers.filemetadata.GetFileMetadataByIdScope;
import com.box.sdkgen.managers.filemetadata.UpdateFileMetadataByIdRequestBody;
import com.box.sdkgen.managers.filemetadata.UpdateFileMetadataByIdRequestBodyOpField;
import com.box.sdkgen.managers.filemetadata.UpdateFileMetadataByIdScope;
import com.box.sdkgen.schemas.metadatafull.MetadataFull;
import com.box.sdkgen.schemas.metadatainstancevalue.MetadataInstanceValue;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

    private static final String DEFAULT_METADATA_TYPE = "properties";

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

    private volatile BoxClient boxClient;

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
        boxClient = boxClientService.getBoxClient();
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
            final Map<String, Object> desiredState = readDesiredState(session, flowFile, recordReaderFactory);

            if (desiredState.isEmpty()) {
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, "No valid metadata key-value pairs found in the input");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final MetadataFull currentMetadata = getMetadata(fileId, templateKey);
            final List<UpdateFileMetadataByIdRequestBody> operations = buildUpdateOperations(currentMetadata, desiredState);

            if (!operations.isEmpty()) {
                getLogger().info("Updating {} metadata fields for file {}", operations.size(), fileId);
                updateBoxFileMetadata(fileId, templateKey, operations);
            }

            final Map<String, String> attributes = Map.of(
                    "box.id", fileId,
                    "box.template.key", templateKey);
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().modifyAttributes(flowFile, "%s%s/metadata/enterprise/%s".formatted(BoxFileUtils.BOX_URL, fileId, templateKey));
            session.transfer(flowFile, REL_SUCCESS);

        } catch (final BoxAPIError e) {
            final int statusCode = e.getResponseInfo() != null ? e.getResponseInfo().getStatusCode() : 0;
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(statusCode));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            final String errorMessage = e.getMessage();
            if (errorMessage != null && errorMessage.toLowerCase().contains("specified metadata template not found")) {
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

    private List<UpdateFileMetadataByIdRequestBody> buildUpdateOperations(final MetadataFull currentMetadata,
                                                                          final Map<String, Object> desiredState) {
        final List<UpdateFileMetadataByIdRequestBody> operations = new ArrayList<>();

        // Get current field names from extra data
        final Set<String> currentKeys = new HashSet<>();
        final Map<String, Object> extraData = currentMetadata.getExtraData();
        if (extraData != null) {
            currentKeys.addAll(extraData.keySet());
        }

        // Remove fields not in desired state
        for (final String fieldName : currentKeys) {
            if (!desiredState.containsKey(fieldName)) {
                final String path = "/" + fieldName;
                getLogger().debug("Removing metadata field: {}", fieldName);
                operations.add(new UpdateFileMetadataByIdRequestBody.Builder()
                        .op(UpdateFileMetadataByIdRequestBodyOpField.REMOVE)
                        .path(path)
                        .build());
            }
        }

        // Add or update fields
        for (final Map.Entry<String, Object> entry : desiredState.entrySet()) {
            final String fieldName = entry.getKey();
            final Object value = entry.getValue();
            final String path = "/" + fieldName;
            final boolean exists = currentKeys.contains(fieldName);

            buildFieldOperation(path, value, exists, extraData).ifPresent(operations::add);
        }

        return operations;
    }

    private Optional<UpdateFileMetadataByIdRequestBody> buildFieldOperation(final String path,
                                                                            final Object value,
                                                                            final boolean exists,
                                                                            final Map<String, Object> extraData) {
        if (value == null) {
            throw new IllegalArgumentException("Null value found for property path: " + path);
        }

        // If exists, check if values are different
        if (exists && extraData != null) {
            final String fieldName = path.substring(1);
            final Object currentValue = extraData.get(fieldName);
            if (Objects.equals(currentValue, value)) {
                return Optional.empty(); // No change needed
            }
        }

        final MetadataInstanceValue metadataValue = convertToMetadataInstanceValue(value, path);

        // Box API uses replace for both adding new fields and updating existing fields
        return Optional.of(new UpdateFileMetadataByIdRequestBody.Builder()
                .op(UpdateFileMetadataByIdRequestBodyOpField.REPLACE)
                .path(path)
                .value(metadataValue)
                .build());
    }

    private MetadataInstanceValue convertToMetadataInstanceValue(final Object value, final String path) {
        return switch (value) {
            case Float f -> new MetadataInstanceValue(f.doubleValue());
            case Double d -> new MetadataInstanceValue(d);
            case Number n -> new MetadataInstanceValue(n.longValue());
            case List<?> l -> {
                final List<String> stringList = l.stream()
                        .map(obj -> {
                            if (obj == null) {
                                throw new IllegalArgumentException("Null value found in list for field: " + path);
                            }
                            return obj.toString();
                        })
                        .toList();
                yield new MetadataInstanceValue(stringList);
            }
            case LocalDate ld -> new MetadataInstanceValue(BoxDate.of(ld).format());
            default -> new MetadataInstanceValue(value.toString());
        };
    }

    /**
     * Retrieves the metadata for a Box file.
     * Visible for testing purposes.
     *
     * @param fileId      The ID of the file.
     * @param templateKey The key of the metadata template.
     * @return The metadata for the Box file.
     */
    MetadataFull getMetadata(final String fileId, final String templateKey) {
        final GetFileMetadataByIdScope scope = DEFAULT_METADATA_TYPE.equals(templateKey)
                ? GetFileMetadataByIdScope.GLOBAL
                : GetFileMetadataByIdScope.ENTERPRISE;
        return boxClient.getFileMetadata().getFileMetadataById(fileId, scope, templateKey);
    }

    /**
     * Updates the metadata for a Box file.
     *
     * @param fileId      The ID of the file.
     * @param templateKey The key of the metadata template.
     * @param operations  The list of update operations.
     */
    void updateBoxFileMetadata(final String fileId, final String templateKey, final List<UpdateFileMetadataByIdRequestBody> operations) {
        final UpdateFileMetadataByIdScope scope = DEFAULT_METADATA_TYPE.equals(templateKey)
                ? UpdateFileMetadataByIdScope.GLOBAL
                : UpdateFileMetadataByIdScope.ENTERPRISE;
        boxClient.getFileMetadata().updateFileMetadataById(fileId, scope, templateKey, operations);
    }
}
