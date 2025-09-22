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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "metadata", "templates", "create"})
@CapabilityDescription("""
        Creates a metadata instance for a Box file using a specified template with values from the flowFile content.\s
        The Box API requires newly created templates to be created with the scope set as enterprise so no scope is required.\s
        The input record should be a flat key-value object where each field name is used as the metadata key.
        """)
@SeeAlso({ListBoxFileMetadataTemplates.class, UpdateBoxFileMetadataInstance.class, ListBoxFile.class, FetchBoxFile.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.id", description = "The ID of the file for which metadata was created"),
        @WritesAttribute(attribute = "box.template.key", description = "The template key used for metadata creation"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class CreateBoxFileMetadataInstance extends AbstractBoxProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the file for which to create metadata.")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMPLATE_KEY = new PropertyDescriptor.Builder()
            .name("Template Key")
            .description("The key of the metadata template to use for creation.")
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
            .description("A FlowFile is routed to this relationship after metadata has been successfully created.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if an error occurs during metadata creation.")
            .build();

    public static final Relationship REL_FILE_NOT_FOUND = new Relationship.Builder()
            .name("file not found")
            .description("FlowFiles for which the specified Box file was not found will be routed to this relationship.")
            .build();

    public static final Relationship REL_TEMPLATE_NOT_FOUND = new Relationship.Builder()
            .name("template not found")
            .description("FlowFiles for which the specified metadata template was not found will be routed to this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_FILE_NOT_FOUND,
            REL_TEMPLATE_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            FILE_ID,
            TEMPLATE_KEY,
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

        try (final InputStream inputStream = session.read(flowFile);
             final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, inputStream, getLogger())) {

            final Metadata metadata = new Metadata();
            final List<String> errors = new ArrayList<>();

            Record record = recordReader.nextRecord();
            if (record != null) {
                processRecord(record, metadata, errors);
            } else {
                errors.add("No records found in input");
            }

            if (!errors.isEmpty()) {
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, String.join(", ", errors));
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            if (metadata.getOperations().isEmpty()) {
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, "No valid metadata key-value pairs found in the input");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final BoxFile boxFile = getBoxFile(fileId);
            boxFile.createMetadata(templateKey, metadata);
        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            if (e.getResponseCode() == 404) {
                final String errorBody = e.getResponse();
                if (errorBody != null && errorBody.toLowerCase().contains("specified metadata template not found")) {
                    getLogger().warn("Box metadata template with key {} was not found.", templateKey);
                    session.transfer(flowFile, REL_TEMPLATE_NOT_FOUND);
                } else {
                    getLogger().warn("Box file with ID {} was not found.", fileId);
                    session.transfer(flowFile, REL_FILE_NOT_FOUND);
                }
            } else {
                getLogger().error("Couldn't create metadata for file with id [{}]", fileId, e);
                session.transfer(flowFile, REL_FAILURE);
            }
            return;
        } catch (Exception e) {
            getLogger().error("Error processing metadata creation for Box file [{}]", fileId, e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> attributes = Map.of(
                "box.id", fileId,
                "box.template.key", templateKey);
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.getProvenanceReporter().create(flowFile, "%s%s/metadata/enterprise/%s".formatted(BoxFileUtils.BOX_URL, fileId, templateKey));
        session.transfer(flowFile, REL_SUCCESS);
    }

    private void processRecord(Record record, Metadata metadata, List<String> errors) {
        if (record == null) {
            errors.add("No record found in input");
            return;
        }

        final List<RecordField> fields = record.getSchema().getFields();

        if (fields.isEmpty()) {
            errors.add("Record has no fields");
            return;
        }

        for (final RecordField field : fields) {
            addValueToMetadata(metadata, record, field);
        }
    }

    private void addValueToMetadata(final Metadata metadata, final Record record, final RecordField field) {
        if (record.getValue(field) == null) {
            return;
        }

        final RecordFieldType fieldType = field.getDataType().getFieldType();
        final String fieldName = field.getFieldName();
        final String path = "/" + fieldName;

        if (isNumber(fieldType)) {
            metadata.add(path, record.getAsDouble(fieldName));
        } else if (isDate(fieldType)) {
            final LocalDate date = record.getAsLocalDate(fieldName, null);
            metadata.add(path, BoxDate.of(date).format());
        } else if (isArray(fieldType)) {
            final List<String> values = Arrays.stream(record.getAsArray(fieldName))
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .toList();

            metadata.add(path, values);
        } else {
            metadata.add(path, record.getAsString(fieldName));
        }
    }

    private boolean isNumber(final RecordFieldType fieldType) {
        final boolean isInteger = RecordFieldType.BIGINT.equals(fieldType) || RecordFieldType.BIGINT.isWiderThan(fieldType);
        final boolean isFloat = RecordFieldType.DECIMAL.equals(fieldType) || RecordFieldType.DECIMAL.isWiderThan(fieldType);
        return isInteger || isFloat;
    }

    private boolean isDate(final RecordFieldType fieldType) {
        return RecordFieldType.DATE.equals(fieldType);
    }

    private boolean isArray(final RecordFieldType fieldType) {
        return RecordFieldType.ARRAY.equals(fieldType);
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
}
