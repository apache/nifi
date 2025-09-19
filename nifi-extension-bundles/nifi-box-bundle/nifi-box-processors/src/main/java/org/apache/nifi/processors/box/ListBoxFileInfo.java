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
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "fetch", "folder", "files"})
@CapabilityDescription("Fetches file metadata for each file in a Box Folder. Takes a flowFile with a folder ID attribute and outputs flowFiles with records containing all file metadata.")
@SeeAlso({ListBoxFile.class, FetchBoxFile.class, PutBoxFile.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.folder.id", description = "The ID of the folder from which files were fetched"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class ListBoxFileInfo extends AbstractBoxProcessor {

    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("Folder ID")
            .description("The ID of the folder from which to fetch files.")
            .required(true)
            .defaultValue("${box.folder.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
            .name("Search Recursively")
            .description("When 'true', will include files from sub-folders." +
                    " Otherwise, will return only files that are within the folder defined by the 'Folder ID' property.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("Minimum File Age")
            .description("The minimum age a file must be in order to be considered; any files younger than this will be ignored.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing the metadata records. Must be set.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile containing the file metadata records will be routed to this relationship upon successful processing.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile will be routed here if there is an error fetching file metadata from the folder.")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("FlowFiles for which the specified Box folder was not found will be routed to this relationship.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            FOLDER_ID,
            RECURSIVE_SEARCH,
            MIN_AGE,
            RECORD_WRITER
    );

    private static final RecordSchema RECORD_SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField("id", RecordFieldType.STRING.getDataType(), false),
            new RecordField("filename", RecordFieldType.STRING.getDataType(), false),
            new RecordField("path", RecordFieldType.STRING.getDataType(), false),
            new RecordField("size", RecordFieldType.LONG.getDataType(), false),
            new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType(), false)
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

        final String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions(flowFile).getValue();
        final Boolean recursive = context.getProperty(RECURSIVE_SEARCH).asBoolean();
        final Long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        try {
            final long startNanos = System.nanoTime();
            long createdAtMax = Instant.now().toEpochMilli() - minAge;
            final List<BoxFile.Info> fileInfos = new ArrayList<>();

            listFolder(fileInfos, folderId, recursive, createdAtMax);

            if (fileInfos.isEmpty()) {
                flowFile = session.putAttribute(flowFile, "box.folder.id", folderId);
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }

            flowFile = session.putAttribute(flowFile, "box.folder.id", folderId);

            try {
                final WriteResult writeResult;
                final String mimeType;

                try (final OutputStream out = session.write(flowFile);
                     final RecordSetWriter writer = writerFactory.createWriter(getLogger(), RECORD_SCHEMA, out, flowFile)) {

                    writer.beginRecordSet();

                    for (final BoxFile.Info fileInfo : fileInfos) {
                        final Map<String, Object> values = Map.of(
                                "id", fileInfo.getID(),
                                "filename", fileInfo.getName(),
                                "path", BoxFileUtils.getParentPath(fileInfo),
                                "size", fileInfo.getSize(),
                                "timestamp", new Timestamp(fileInfo.getModifiedAt().getTime())
                        );

                        final Record record = new MapRecord(RECORD_SCHEMA, values);
                        writer.write(record);
                    }

                    writeResult = writer.finishRecordSet();
                    mimeType = writer.getMimeType();
                }

                final Map<String, String> recordAttributes = new HashMap<>(writeResult.getAttributes());
                recordAttributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                recordAttributes.put(CoreAttributes.MIME_TYPE.key(), mimeType);
                flowFile = session.putAllAttributes(flowFile, recordAttributes);

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().receive(flowFile, BoxFileUtils.BOX_URL + folderId, transferMillis);

                session.transfer(flowFile, REL_SUCCESS);
            } catch (final SchemaNotFoundException | IOException e) {
                getLogger().error("Failed writing records for files from folder [{}]", folderId, e);
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            if (e.getResponseCode() == 404) {
                getLogger().warn("Box folder with ID {} was not found.", folderId);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Couldn't fetch files from folder with id [{}]", folderId, e);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private void listFolder(final List<BoxFile.Info> fileInfos,
                            final String folderId,
                            final Boolean recursive,
                            final long createdAtMax) {
        final BoxFolder folder = getFolder(folderId);
        for (final BoxItem.Info itemInfo : folder.getChildren(
                "id",
                "name",
                "item_status",
                "size",
                "created_at",
                "modified_at",
                "content_created_at",
                "content_modified_at",
                "path_collection"
        )) {
            if (itemInfo instanceof BoxFile.Info fileInfo) {
                long createdAt = itemInfo.getCreatedAt().getTime();

                if (createdAt <= createdAtMax) {
                    fileInfos.add(fileInfo);
                }
            } else if (recursive && itemInfo instanceof BoxFolder.Info subFolderInfo) {
                listFolder(fileInfos, subFolderInfo.getID(), recursive, createdAtMax);
            }
        }
    }

    /**
     * Returns a BoxFolder object for the given folder ID.
     *
     * @param folderId The ID of the folder.
     * @return A BoxFolder object for the given folder ID.
     */
    BoxFolder getFolder(final String folderId) {
        return new BoxFolder(boxAPIConnection, folderId);
    }
}
