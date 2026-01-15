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
import com.box.sdkgen.schemas.metadata.Metadata;
import com.box.sdkgen.schemas.metadatafull.MetadataFull;
import com.box.sdkgen.schemas.metadatas.Metadatas;
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.box.utils.BoxMetadataUtils.processBoxMetadataInstance;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "metadata", "templates"})
@CapabilityDescription("Retrieves all metadata templates associated with a Box file.")
@SeeAlso({ListBoxFile.class, FetchBoxFile.class, FetchBoxFileInfo.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.file.id", description = "The ID of the file from which metadata was fetched"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "box.metadata.templates.names", description = "Comma-separated list of template names"),
        @WritesAttribute(attribute = "box.metadata.templates.count", description = "Number of metadata templates found"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class ListBoxFileMetadataTemplates extends AbstractBoxProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the file for which to fetch metadata.")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FETCH_FULL_METADATA = new PropertyDescriptor.Builder()
            .name("Fetch Full Metadata")
            .description("When enabled, makes an additional API call for each metadata template to retrieve full metadata "
                    + "including the $id field and custom field values. When disabled, only basic metadata fields "
                    + "($parent, $template, $scope, $version) are returned. Enabling this may increase API calls but "
                    + "provides complete metadata information.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
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
            .name("not found")
            .description("FlowFiles for which the specified Box file was not found will be routed to this relationship.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            FILE_ID,
            FETCH_FULL_METADATA
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
        final boolean fetchFullMetadata = context.getProperty(FETCH_FULL_METADATA).asBoolean();

        try {
            final Metadatas metadatas = getFileMetadata(fileId);

            final List<Map<String, Object>> templatesList = new ArrayList<>();
            final Set<String> templateNames = new LinkedHashSet<>();

            if (metadatas.getEntries() == null || metadatas.getEntries().isEmpty()) {
                flowFile = session.putAttribute(flowFile, "box.file.id", fileId);
                flowFile = session.putAttribute(flowFile, "box.metadata.templates.count", "0");
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }

            for (final Metadata metadata : metadatas.getEntries()) {
                final Map<String, Object> templateFields = new HashMap<>();
                final String templateName = metadata.getTemplate();
                final String scope = metadata.getScope();

                if (templateName != null) {
                    templateNames.add(templateName);
                }

                if (fetchFullMetadata && templateName != null && scope != null) {
                    // Fetch full metadata with all custom fields
                    final GetFileMetadataByIdScope metadataScope = "global".equalsIgnoreCase(scope)
                            ? GetFileMetadataByIdScope.GLOBAL
                            : GetFileMetadataByIdScope.ENTERPRISE;
                    final MetadataFull fullMetadata = getFileMetadataById(fileId, metadataScope, templateName);
                    processBoxMetadataInstance(fileId, scope, templateName, fullMetadata, templateFields);
                } else {
                    // Add only basic metadata fields
                    templateFields.put("$parent", metadata.getParent());
                    templateFields.put("$template", templateName);
                    templateFields.put("$scope", scope);
                    templateFields.put("$version", metadata.getVersion());
                }

                templatesList.add(templateFields);
            }

            try {
                try (final OutputStream out = session.write(flowFile);
                     final BoxMetadataJsonArrayWriter writer = BoxMetadataJsonArrayWriter.create(out)) {

                    // Write each metadata template as a separate JSON object in the array
                    for (Map<String, Object> templateFields : templatesList) {
                        writer.write(templateFields);
                    }
                }

                final Map<String, String> recordAttributes = new HashMap<>();
                recordAttributes.put("record.count", String.valueOf(templatesList.size()));
                recordAttributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                recordAttributes.put("box.file.id", fileId);
                recordAttributes.put("box.metadata.templates.names", String.join(",", templateNames));
                recordAttributes.put("box.metadata.templates.count", String.valueOf(templatesList.size()));
                flowFile = session.putAllAttributes(flowFile, recordAttributes);

                session.getProvenanceReporter().receive(flowFile, BoxFileUtils.BOX_URL + fileId);
                session.transfer(flowFile, REL_SUCCESS);
            } catch (final IOException e) {
                getLogger().error("Failed writing metadata templates from file [{}]", fileId, e);
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (final BoxAPIError e) {
            final int statusCode = e.getResponseInfo() != null ? e.getResponseInfo().getStatusCode() : 0;
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(statusCode));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            if (statusCode == 404) {
                getLogger().warn("Box file with ID {} was not found.", fileId);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Couldn't fetch metadata templates from file with id [{}]", fileId, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final Exception e) {
            getLogger().error("Failed to process metadata templates for file [{}]", fileId, e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Returns all metadata for a file.
     *
     * @param fileId The ID of the file.
     * @return The metadata for the file.
     */
    Metadatas getFileMetadata(final String fileId) {
        return boxClient.getFileMetadata().getFileMetadata(fileId);
    }

    /**
     * Returns full metadata for a specific template on a file.
     *
     * @param fileId      The ID of the file.
     * @param scope       The scope of the metadata.
     * @param templateKey The template key of the metadata.
     * @return The full metadata for the file.
     */
    MetadataFull getFileMetadataById(final String fileId, final GetFileMetadataByIdScope scope, final String templateKey) {
        return boxClient.getFileMetadata().getFileMetadataById(fileId, scope, templateKey);
    }
}
