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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

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
import static org.apache.nifi.processors.box.utils.BoxMetadataUtils.processBoxMetadataInstance;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "metadata", "instances", "templates"})
@CapabilityDescription("Retrieves all metadata instances associated with a Box file.")
@SeeAlso({ListBoxFile.class, FetchBoxFile.class, FetchBoxFileInfo.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.id", description = "The ID of the file from which metadata was fetched"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "box.metadata.instances.names", description = "Comma-separated list of instances names"),
        @WritesAttribute(attribute = "box.metadata.instances.count", description = "Number of metadata instances found"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class ListBoxFileMetadataInstances extends AbstractBoxProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the file for which to fetch metadata.")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile containing the metadata instances records will be routed to this relationship upon successful processing.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile will be routed here if there is an error fetching metadata instances from the file.")
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

        try {
            final BoxFile boxFile = getBoxFile(fileId);

            final List<Map<String, Object>> instanceList = new ArrayList<>();
            final Iterable<Metadata> metadataList = boxFile.getAllMetadata();
            final Iterator<Metadata> iterator = metadataList.iterator();
            final Set<String> templateNames = new LinkedHashSet<>();

            if (!iterator.hasNext()) {
                flowFile = session.putAttribute(flowFile, "box.id", fileId);
                flowFile = session.putAttribute(flowFile, "box.metadata.instances.count", "0");
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }

            while (iterator.hasNext()) {
                final Metadata metadata = iterator.next();
                final Map<String, Object> instanceFields = new HashMap<>();

                templateNames.add(metadata.getTemplateName());

                // Add standard metadata fields
                processBoxMetadataInstance(fileId, metadata, instanceFields);
                instanceList.add(instanceFields);
            }

            try {
                try (final OutputStream out = session.write(flowFile);
                     final BoxMetadataJsonArrayWriter writer = BoxMetadataJsonArrayWriter.create(out)) {

                    // Write each metadata template as a separate JSON object in the array
                    for (Map<String, Object> templateFields : instanceList) {
                        writer.write(templateFields);
                    }
                }

                final Map<String, String> recordAttributes = new HashMap<>();
                recordAttributes.put("record.count", String.valueOf(instanceList.size()));
                recordAttributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                recordAttributes.put("box.id", fileId);
                recordAttributes.put("box.metadata.instances.names", String.join(",", templateNames));
                recordAttributes.put("box.metadata.instances.count", String.valueOf(instanceList.size()));
                flowFile = session.putAllAttributes(flowFile, recordAttributes);

                session.getProvenanceReporter().receive(flowFile, BoxFileUtils.BOX_URL + fileId + "/metadata");
                session.transfer(flowFile, REL_SUCCESS);
            } catch (final IOException e) {
                getLogger().error("Failed writing metadata instances from file [{}]", fileId, e);
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            if (e.getResponseCode() == 404) {
                getLogger().warn("Box file with ID {} was not found.", fileId);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Couldn't fetch metadata instances from file with id [{}]", fileId, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final Exception e) {
            getLogger().error("Failed to process metadata instances for file [{}]", fileId, e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
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
