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
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.PATH_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP_DESC;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "fetch", "folder", "files"})
@CapabilityDescription("Fetches file metadata for each file in a Box Folder. Takes a flowFile with a folder ID attribute and outputs a flowFile for each file in the folder.")
@SeeAlso({ListBoxFile.class, FetchBoxFile.class, PutBoxFile.class})
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "path", description = PATH_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class FetchBoxFilesInFolder extends AbstractProcessor {

    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("box-folder-id")
            .displayName("Folder ID")
            .description("The ID of the folder from which to fetch files.")
            .required(true)
            .defaultValue("${box.folder.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
            .name("recursive-search")
            .displayName("Search Recursively")
            .description("When 'true', will include files from sub-folders." +
                    " Otherwise, will return only files that are within the folder defined by the 'Folder ID' property.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("min-age")
            .displayName("Minimum File Age")
            .description("The minimum age a file must be in order to be considered; any files younger than this will be ignored.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile will be routed here for each successfully fetched file metadata.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile will be routed here if there is an error fetching file metadata from the folder.")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile will be routed here after all files have been fetched.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_ORIGINAL
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BoxClientService.BOX_CLIENT_SERVICE,
            FOLDER_ID,
            RECURSIVE_SEARCH,
            MIN_AGE
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

        final String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions(flowFile).getValue();
        final Boolean recursive = context.getProperty(RECURSIVE_SEARCH).asBoolean();
        final Long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        try {
            final long startNanos = System.nanoTime();
            long createdAtMax = Instant.now().toEpochMilli() - minAge;
            final List<BoxFile.Info> fileInfos = new ArrayList<>();

            listFolder(fileInfos, folderId, recursive, createdAtMax);

            for (final BoxFile.Info fileInfo : fileInfos) {
                FlowFile outputFlowFile = session.create(flowFile);
                try {
                    final Map<String, String> attributes = BoxFileUtils.createAttributeMap(fileInfo);
                    outputFlowFile = session.putAllAttributes(outputFlowFile, attributes);

                    final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    final String boxUrlOfFile = BoxFileUtils.BOX_URL + fileInfo.getID();
                    session.getProvenanceReporter().receive(outputFlowFile, boxUrlOfFile, transferMillis);

                    session.transfer(outputFlowFile, REL_SUCCESS);
                } catch (final Exception e) {
                    getLogger().error("Failed fetching metadata for file with id [{}] from folder [{}]", fileInfo.getID(), folderId, e);
                    outputFlowFile = session.putAttribute(outputFlowFile, ERROR_MESSAGE, e.getMessage());
                    outputFlowFile = session.penalize(outputFlowFile);
                    session.transfer(outputFlowFile, REL_FAILURE);
                }
            }

            session.transfer(flowFile, REL_ORIGINAL);

        } catch (final BoxAPIResponseException e) {
            getLogger().error("Couldn't fetch files from folder with id [{}]", folderId, e);
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } catch (final Exception e) {
            getLogger().error("Failed fetching files from folder with id [{}]", folderId, e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
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