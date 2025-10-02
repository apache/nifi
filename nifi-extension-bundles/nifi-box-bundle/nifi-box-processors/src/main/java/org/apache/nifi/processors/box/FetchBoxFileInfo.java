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
import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxUser;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
@Tags({"box", "storage", "metadata", "fetch"})
@CapabilityDescription("Fetches metadata for files from Box and adds it to the FlowFile's attributes.")
@SeeAlso({ListBoxFile.class, FetchBoxFile.class, PutBoxFile.class})
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "path", description = PATH_DESC),
        @WritesAttribute(attribute = "box.path.folder.ids", description = "A comma separated list of file path_collection IDs"),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = "box.created.at", description = "The creation date of the file"),
        @WritesAttribute(attribute = "box.owner", description = "The name of the file owner"),
        @WritesAttribute(attribute = "box.owner.id", description = "The ID of the file owner"),
        @WritesAttribute(attribute = "box.owner.login", description = "The login of the file owner"),
        @WritesAttribute(attribute = "box.description", description = "The description of the file"),
        @WritesAttribute(attribute = "box.etag", description = "The etag of the file"),
        @WritesAttribute(attribute = "box.sha1", description = "The SHA-1 hash of the file"),
        @WritesAttribute(attribute = "box.content.created.at", description = "The date the content was created"),
        @WritesAttribute(attribute = "box.content.modified.at", description = "The date the content was modified"),
        @WritesAttribute(attribute = "box.item.status", description = "The status of the file (active, trashed, etc.)"),
        @WritesAttribute(attribute = "box.sequence_id", description = "The sequence ID of the file"),
        @WritesAttribute(attribute = "box.parent.folder.id", description = "The ID of the parent folder"),
        @WritesAttribute(attribute = "box.trashed.at", description = "The date the file was trashed, if applicable"),
        @WritesAttribute(attribute = "box.purged.at", description = "The date the file was purged, if applicable"),
        @WritesAttribute(attribute = "box.shared.link", description = "The shared link of the file, if any"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class FetchBoxFileInfo extends AbstractBoxProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the File to fetch metadata for")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile will be routed here after successfully fetching the file metadata.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile will be routed here if fetching the file metadata fails.")
            .build();

    static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("FlowFiles for which the specified Box file was not found.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            FILE_ID
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
        BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxAPIConnection = boxClientService.getBoxApiConnection();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();
        try {
            flowFile = fetchFileMetadata(fileId, session, flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            flowFile = session.putAttribute(flowFile, ERROR_CODE, String.valueOf(e.getResponseCode()));

            if (e.getResponseCode() == 404) {
                getLogger().warn("Box file with ID {} was not found.", fileId);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Failed to retrieve Box file representation for file [{}]", fileId, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final BoxAPIException e) {
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            flowFile = session.putAttribute(flowFile, ERROR_CODE, String.valueOf(e.getResponseCode()));
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Fetches the BoxFile instance for a given file ID. For testing purposes.
     *
     * @param fileId the ID of the file
     * @return BoxFile instance
     */
    protected BoxFile getBoxFile(final String fileId) {
        return new BoxFile(boxAPIConnection, fileId);
    }

    private FlowFile fetchFileMetadata(final String fileId,
                                       final ProcessSession session,
                                       final FlowFile flowFile) {
        final BoxFile boxFile = getBoxFile(fileId);
        final BoxFile.Info fileInfo = boxFile.getInfo("name", "description", "size", "created_at", "modified_at",
                "owned_by", "parent", "etag", "sha1", "item_status", "sequence_id", "path_collection",
                "content_created_at", "content_modified_at", "trashed_at", "purged_at", "shared_link");

        final Map<String, String> attributes = new HashMap<>(BoxFileUtils.createAttributeMap(fileInfo));

        addAttributeIfNotNull(attributes, "box.description", fileInfo.getDescription());
        addAttributeIfNotNull(attributes, "box.etag", fileInfo.getEtag());
        addAttributeIfNotNull(attributes, "box.sha1", fileInfo.getSha1());
        addAttributeIfNotNull(attributes, "box.content.created.at", fileInfo.getContentCreatedAt());
        addAttributeIfNotNull(attributes, "box.content.modified.at", fileInfo.getContentModifiedAt());
        addAttributeIfNotNull(attributes, "box.item.status", fileInfo.getItemStatus());
        addAttributeIfNotNull(attributes, "box.sequence.id", fileInfo.getSequenceID());
        addAttributeIfNotNull(attributes, "box.created.at", fileInfo.getCreatedAt());
        addAttributeIfNotNull(attributes, "box.trashed.at", fileInfo.getTrashedAt());
        addAttributeIfNotNull(attributes, "box.purged.at", fileInfo.getPurgedAt());
        addAttributeIfNotNull(attributes, "box.path.folder.ids", BoxFileUtils.getParentIds(fileInfo));

        // Handle special cases
        final BoxUser.Info owner = fileInfo.getOwnedBy();
        if (owner != null) {
            addAttributeIfNotNull(attributes, "box.owner", owner.getName());
            addAttributeIfNotNull(attributes, "box.owner.id", owner.getID());
            addAttributeIfNotNull(attributes, "box.owner.login", owner.getLogin());
        }

        if (fileInfo.getParent() != null) {
            attributes.put("box.parent.folder.id", fileInfo.getParent().getID());
        }

        if (fileInfo.getSharedLink() != null && fileInfo.getSharedLink().getURL() != null) {
            attributes.put("box.shared.link", fileInfo.getSharedLink().getURL());
        }

        return session.putAllAttributes(flowFile, attributes);
    }

    private void addAttributeIfNotNull(final Map<String, String> attributes,
                                       final String key,
                                       final Object value) {
        if (value != null) {
            attributes.put(key, String.valueOf(value));
        }
    }
}
