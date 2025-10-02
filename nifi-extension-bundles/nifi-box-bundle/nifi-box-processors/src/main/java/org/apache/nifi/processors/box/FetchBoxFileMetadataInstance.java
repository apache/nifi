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
import java.util.HashMap;
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
@Tags({"box", "storage", "metadata", "instance", "template"})
@CapabilityDescription("Retrieves specific metadata instance associated with a Box file using template key and scope.")
@SeeAlso({ListBoxFileMetadataInstances.class, ListBoxFile.class, FetchBoxFile.class, FetchBoxFileInfo.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.id", description = "The ID of the file from which metadata was fetched"),
        @WritesAttribute(attribute = "box.metadata.template.key", description = "The metadata template key"),
        @WritesAttribute(attribute = "box.metadata.template.scope", description = "The metadata template scope"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type of the FlowFile content"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class FetchBoxFileMetadataInstance extends AbstractBoxProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the file for which to fetch metadata.")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMPLATE_KEY = new PropertyDescriptor.Builder()
            .name("Template Key")
            .description("The metadata template key to retrieve.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMPLATE_SCOPE = new PropertyDescriptor.Builder()
            .name("Template Scope")
            .description("The metadata template scope (e.g., 'enterprise', 'global').")
            .required(true)
            .defaultValue("enterprise")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile containing the metadata instance will be routed to this relationship upon successful processing.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile will be routed here if there is an error fetching metadata instance from the file.")
            .build();

    public static final Relationship REL_FILE_NOT_FOUND = new Relationship.Builder()
            .name("file not found")
            .description("FlowFiles for which the specified Box file was not found will be routed to this relationship.")
            .build();

    public static final Relationship REL_TEMPLATE_NOT_FOUND = new Relationship.Builder()
            .name("template not found")
            .description("FlowFiles for which the specified metadata template was not found will be routed to this relationship.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_FILE_NOT_FOUND,
            REL_TEMPLATE_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            FILE_ID,
            TEMPLATE_KEY,
            TEMPLATE_SCOPE
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
        final String templateScope = context.getProperty(TEMPLATE_SCOPE).evaluateAttributeExpressions(flowFile).getValue();

        try {
            final BoxFile boxFile = getBoxFile(fileId);
            final Metadata metadata = boxFile.getMetadata(templateKey, templateScope);

            final Map<String, Object> instanceFields = new HashMap<>();
            processBoxMetadataInstance(fileId, metadata, instanceFields);

            try {
                try (final OutputStream out = session.write(flowFile);
                     final BoxMetadataJsonArrayWriter writer = BoxMetadataJsonArrayWriter.create(out)) {
                    writer.write(instanceFields);
                }

                final Map<String, String> recordAttributes = new HashMap<>();
                recordAttributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                recordAttributes.put("box.id", fileId);
                recordAttributes.put("box.metadata.template.key", templateKey);
                recordAttributes.put("box.metadata.template.scope", templateScope);
                flowFile = session.putAllAttributes(flowFile, recordAttributes);

                session.getProvenanceReporter().receive(flowFile,
                        String.format("%s/%s/metadata/%s", BoxFileUtils.BOX_URL, fileId, templateKey));
                session.transfer(flowFile, REL_SUCCESS);
            } catch (final IOException e) {
                getLogger().error("Failed writing metadata instance from file [{}] with template key [{}] and scope [{}]",
                        fileId, templateKey, templateScope, e);
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            if (e.getResponseCode() == 404) {
                final String errorBody = e.getResponse();
                if (errorBody != null && errorBody.toLowerCase().contains("instance_not_found")) {
                    getLogger().warn("Box metadata template with key {} and scope {} was not found.", templateKey, templateScope);
                    session.transfer(flowFile, REL_TEMPLATE_NOT_FOUND);
                } else {
                    getLogger().warn("Box file with ID {} was not found.", fileId);
                    session.transfer(flowFile, REL_FILE_NOT_FOUND);
                }
            } else {
                getLogger().error("Couldn't fetch metadata instance from file with id [{}] with template key [{}] and scope [{}]",
                        fileId, templateKey, templateScope, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final Exception e) {
            getLogger().error("Failed to process metadata instance for file [{}] with template key [{}] and scope [{}]",
                    fileId, templateKey, templateScope, e);
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
