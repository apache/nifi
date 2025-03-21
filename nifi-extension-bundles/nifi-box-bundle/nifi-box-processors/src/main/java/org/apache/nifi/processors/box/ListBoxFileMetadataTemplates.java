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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "metadata", "templates"})
@CapabilityDescription("Retrieves all metadata templates associated with a Box file. Takes a flowFile with a file ID " +
        "attribute and outputs a flowFile with records containing metadata template information and values.")
@SeeAlso({ListBoxFile.class, FetchBoxFile.class, FetchBoxFileInfo.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.file.id", description = "The ID of the file from which metadata was fetched"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "box.metadata.templates.names", description = "Comma-separated list of template names"),
        @WritesAttribute(attribute = "box.metadata.templates.count", description = "Number of metadata templates found"),
        @WritesAttribute(attribute = "box.metadata.templates.scopes", description = "Comma-separated list of template scopes"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class ListBoxFileMetadataTemplates extends AbstractProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the file for which to fetch metadata.")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing the metadata records. Must be set.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile containing the metadata template records will be routed to this relationship upon successful processing.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile will be routed here if there is an error fetching metadata templates from the file.")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("FlowFiles for which the specified Box file was not found will be routed to this relationship.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BoxClientService.BOX_CLIENT_SERVICE,
            FILE_ID,
            RECORD_WRITER
    );

    private static final RecordSchema METADATA_RECORD_SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField("id", RecordFieldType.STRING.getDataType(), false),
            new RecordField("template", RecordFieldType.STRING.getDataType(), false),
            new RecordField("scope", RecordFieldType.STRING.getDataType(), false),
            new RecordField("metadata", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), false)
    ));

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
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        try {
            final BoxFile boxFile = getBoxFile(fileId);

            final List<Record> metadataRecords = new ArrayList<>();
            final Iterable<Metadata> metadataList = boxFile.getAllMetadata();
            final Iterator<Metadata> iterator = metadataList.iterator();

            final Set<String> templateNames = new LinkedHashSet<>();
            final Set<String> templateScopes = new LinkedHashSet<>();

            if (!iterator.hasNext()) {
                Map<String, String> emptyAttributes = new HashMap<>();
                emptyAttributes.put("box.file.id", fileId);
                emptyAttributes.put("box.metadata.templates.count", "0");
                flowFile = session.putAllAttributes(flowFile, emptyAttributes);
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }

            while (iterator.hasNext()) {
                final Metadata metadata = iterator.next();
                final Map<String, Object> metadataValues = new HashMap<>();

                String scope = metadata.getScope();
                final int underscoreIndex = scope.indexOf('_');
                if (underscoreIndex > 0) {
                    scope = scope.substring(0, underscoreIndex);
                }

                templateNames.add(metadata.getTemplateName());
                templateScopes.add(scope);

                final Map<String, Object> fields = new HashMap<>();
                for (String fieldName : metadata.getPropertyPaths()) {
                    if (metadata.getValue(fieldName) != null) {
                        String cleanFieldName = fieldName.startsWith("/") ? fieldName.substring(1) : fieldName;
                        String fieldValue = metadata.getValue(fieldName).asString();
                        fields.put(cleanFieldName, fieldValue);
                    }
                }

                metadataValues.put("id", metadata.getID());
                metadataValues.put("template", metadata.getTemplateName());
                metadataValues.put("scope", scope);
                metadataValues.put("metadata", fields);

                final Record record = new MapRecord(METADATA_RECORD_SCHEMA, metadataValues);
                metadataRecords.add(record);
            }
            
            flowFile = session.putAttribute(flowFile, "box.file.id", fileId);

            try {
                final WriteResult writeResult;
                final String mimeType;

                try (final OutputStream out = session.write(flowFile);
                     final RecordSetWriter writer = writerFactory.createWriter(getLogger(), METADATA_RECORD_SCHEMA, out, flowFile)) {

                    writer.beginRecordSet();

                    for (final Record record : metadataRecords) {
                        writer.write(record);
                    }

                    writeResult = writer.finishRecordSet();
                    mimeType = writer.getMimeType();
                }

                final Map<String, String> recordAttributes = new HashMap<>(writeResult.getAttributes());
                recordAttributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                recordAttributes.put(CoreAttributes.MIME_TYPE.key(), mimeType);
                recordAttributes.put("box.metadata.templates.names", String.join(",", templateNames));
                recordAttributes.put("box.metadata.templates.count", String.valueOf(metadataRecords.size()));
                recordAttributes.put("box.metadata.templates.scopes", String.join(",", templateScopes));
                flowFile = session.putAllAttributes(flowFile, recordAttributes);

                session.getProvenanceReporter().receive(flowFile, BoxFileUtils.BOX_URL + fileId);
                session.transfer(flowFile, REL_SUCCESS);
            } catch (final SchemaNotFoundException | IOException e) {
                getLogger().error("Failed writing records for metadata templates from file [{}]", fileId, e);
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            if (e.getResponseCode() == 404) {
                getLogger().warn("Box file with ID {} was not found.", fileId);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Couldn't fetch metadata templates from file with id [{}]", fileId, e);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final Exception e) {
            getLogger().error("Failed to process metadata templates for file [{}]", fileId, e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
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
