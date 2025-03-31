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
@Tags({"box", "storage", "metadata", "templates", "update"})
@CapabilityDescription("Updates metadata template values for a Box file using records from the flowFile content.")
@SeeAlso({ListBoxFileMetadataTemplates.class, ListBoxFile.class, FetchBoxFile.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.id", description = "The ID of the file whose metadata was updated"),
        @WritesAttribute(attribute = "box.template.name", description = "The template name used for metadata update"),
        @WritesAttribute(attribute = "box.template.scope", description = "The template scope used for metadata update"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class UpdateBoxFileMetadataInstance extends AbstractProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the file for which to update metadata.")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMPLATE_NAME = new PropertyDescriptor.Builder()
            .name("Template Name")
            .description("The name of the metadata template to update.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMPLATE_SCOPE = new PropertyDescriptor.Builder()
            .name("Template Scope")
            .description("The scope of the metadata template to update (e.g., 'enterprise', 'global').")
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

    public static final PropertyDescriptor KEY_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("Metadata Key Record Path")
            .description("Specifies the RecordPath to use for getting the metadata key to update.")
            .required(true)
            .addValidator(new RecordPathValidator())
            .defaultValue("/key")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor VALUE_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("Metadata Value Record Path")
            .description("Specifies the record path to use for getting the metadata value to update.")
            .required(true)
            .addValidator(new RecordPathValidator())
            .defaultValue("/value")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after metadata has been successfully updated.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if an error occurs during metadata update.")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("FlowFiles for which the specified Box file was not found will be routed to this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BoxClientService.BOX_CLIENT_SERVICE,
            FILE_ID,
            TEMPLATE_NAME,
            TEMPLATE_SCOPE,
            RECORD_READER,
            KEY_RECORD_PATH,
            VALUE_RECORD_PATH
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
        final BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE)
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
        final String templateName = context.getProperty(TEMPLATE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String templateScope = context.getProperty(TEMPLATE_SCOPE).evaluateAttributeExpressions(flowFile).getValue();
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String keyRecordPathStr = context.getProperty(KEY_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final String valueRecordPathStr = context.getProperty(VALUE_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();

        try (final InputStream inputStream = session.read(flowFile);
             final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, inputStream, getLogger())) {

            final RecordPath keyRecordPath = RecordPath.compile(keyRecordPathStr);
            final RecordPath valueRecordPath = RecordPath.compile(valueRecordPathStr);

            // Create metadata object
            final BoxFile boxFile = getBoxFile(fileId);
            final Metadata metadata = boxFile.getMetadata(templateName, templateScope);
            final Set<String> updatedKeys = new HashSet<>();
            final List<String> errors = new ArrayList<>();

            Record record;
            try {
                while ((record = recordReader.nextRecord()) != null) {
                    processRecord(record, keyRecordPath, valueRecordPath, metadata, updatedKeys, errors);
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

            if (updatedKeys.isEmpty()) {
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, "No valid metadata key-value pairs found in the input");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            boxFile.updateMetadata(metadata);

            // Update FlowFile attributes
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("box.id", fileId);
            attributes.put("box.template.name", templateName);
            attributes.put("box.template.scope", templateScope);
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().modifyAttributes(flowFile, BoxFileUtils.BOX_URL + fileId + "/metadata/enterprise/" + templateName);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            if (e.getResponseCode() == 404) {
                getLogger().warn("Box file with ID {} was not found.", fileId);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Couldn't update metadata for file with id [{}]", fileId, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (Exception e) {
            getLogger().error("Error processing metadata update for Box file [{}]", fileId, e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void processRecord(Record record, RecordPath keyRecordPath, RecordPath valueRecordPath,
                               Metadata metadata, Set<String> updatedKeys, List<String> errors) {
        // Get the key from the record
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

        // Get the value from the record
        final RecordPathResult valuePathResult = valueRecordPath.evaluate(record);
        final List<FieldValue> valueValues = valuePathResult.getSelectedFields().toList();

        if (valueValues.isEmpty()) {
            errors.add("Record with key '" + key + "' is missing a value field");
            return;
        }

        final Object valueObj = valueValues.getFirst().getValue();
        final String value = valueObj != null ? valueObj.toString() : null;

        // Add the key-value pair to the metadata update
        metadata.add("/" + key, value);
        updatedKeys.add(key);
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
