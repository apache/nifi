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

import com.box.sdk.BoxAI;
import com.box.sdk.BoxAIExtractMetadataTemplate;
import com.box.sdk.BoxAIExtractStructuredResponse;
import com.box.sdk.BoxAIItem;
import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIResponseException;
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
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "metadata", "ai", "extract"})
@CapabilityDescription("Extracts metadata from a Box file using Box AI and a template. The extracted metadata is written to the FlowFile content as JSON.")
@SeeAlso({ListBoxFileMetadataTemplates.class, ListBoxFile.class, FetchBoxFile.class, UpdateBoxFileMetadataInstance.class})
@WritesAttributes({
        @WritesAttribute(attribute = "box.id", description = "The ID of the file from which metadata was extracted"),
        @WritesAttribute(attribute = "box.ai.template.key", description = "The template key used for extraction"),
        @WritesAttribute(attribute = "box.ai.completion.reason", description = "The completion reason from the AI extraction"),
        @WritesAttribute(attribute = "mime.type", description = "Set to 'application/json' for the JSON content"),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class ExtractStructuredBoxFileMetadata extends AbstractProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("File ID")
            .description("The ID of the file from which to extract metadata.")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMPLATE_KEY = new PropertyDescriptor.Builder()
            .name("Template Key")
            .description("The key of the metadata template to use for extraction.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after metadata has been successfully extracted.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if an error occurs during metadata extraction.")
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
            TEMPLATE_KEY
    );

    private static final String SCOPE = "enterprise";

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
        final String templateKey = context.getProperty(TEMPLATE_KEY).evaluateAttributeExpressions(flowFile).getValue();

        try {
            final BoxAIExtractStructuredResponse result = getBoxAIExtractStructuredResponse(templateKey, fileId);
            flowFile = session.putAllAttributes(flowFile, getAttributes(result, fileId));

            // Write the results directly to the FlowFile content. No record reader is needed.
            try (final OutputStream out = session.write(flowFile)) {
                if (result.getAnswer() != null) {
                    out.write(result.getAnswer().toString().getBytes());
                }
            } catch (final IOException e) {
                getLogger().error("Error writing Box AI metadata extraction answer to FlowFile", e);
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");

            session.getProvenanceReporter().modifyAttributes(flowFile, BoxFileUtils.BOX_URL + fileId);
            session.transfer(flowFile, REL_SUCCESS);

        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            if (e.getResponseCode() == 404) {
                getLogger().warn("Box file with ID {} was not found.", fileId);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Couldn't extract metadata from file with id [{}]", fileId, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final Exception e) {
            getLogger().error("Error processing metadata extraction for Box file [{}]", fileId, e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * This method is responsible for extracting metadata from a Box file using the specified template key.
     * Visible for testing purposes.
     *
     * @param templateKey The key of the metadata template to use for extraction.
     * @param fileId      The ID of the Box file from which to extract metadata.
     * @return The response containing the extracted metadata.
     */
    BoxAIExtractStructuredResponse getBoxAIExtractStructuredResponse(final String templateKey,
                                                                     final String fileId) {
        final BoxAIExtractMetadataTemplate template = new BoxAIExtractMetadataTemplate(templateKey, SCOPE);
        final BoxAIItem fileItem = new BoxAIItem(fileId, BoxAIItem.Type.FILE);

        return BoxAI.extractMetadataStructured(
                boxAPIConnection,
                Collections.singletonList(fileItem),
                template
        );
    }

    private static @NotNull Map<String, String> getAttributes(final BoxAIExtractStructuredResponse result,
                                                              final String fileId) {
        final String completionReason = result.getCompletionReason();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("box.id", fileId);

        if (completionReason != null) {
            attributes.put("box.ai.completion.reason", completionReason);
        }
        return attributes;
    }
}
