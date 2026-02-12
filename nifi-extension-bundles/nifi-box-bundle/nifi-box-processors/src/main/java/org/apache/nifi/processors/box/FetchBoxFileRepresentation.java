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
import com.box.sdkgen.managers.files.GetFileByIdQueryParams;
import com.box.sdkgen.networking.fetchoptions.FetchOptions;
import com.box.sdkgen.networking.fetchoptions.ResponseFormat;
import com.box.sdkgen.networking.fetchresponse.FetchResponse;
import com.box.sdkgen.schemas.filefull.FileFull;
import com.box.sdkgen.schemas.filefull.FileFullRepresentationsEntriesContentField;
import com.box.sdkgen.schemas.filefull.FileFullRepresentationsEntriesField;
import com.box.sdkgen.schemas.filefull.FileFullRepresentationsField;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome;

@Tags({"box", "cloud", "storage", "file", "representation", "content", "download"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Fetches a Box file representation using a representation hint and writes it to the FlowFile content.")
@WritesAttributes({
        @WritesAttribute(attribute = "box.id", description = "The ID of the Box file."),
        @WritesAttribute(attribute = "box.file.name", description = "The name of the Box file."),
        @WritesAttribute(attribute = "box.file.size", description = "The size of the Box file in bytes."),
        @WritesAttribute(attribute = "box.file.created.time", description = "The timestamp when the file was created."),
        @WritesAttribute(attribute = "box.file.modified.time", description = "The timestamp when the file was last modified."),
        @WritesAttribute(attribute = "box.file.representation.type", description = "The representation type that was fetched."),
        @WritesAttribute(attribute = "box.error.message", description = "The error message returned by Box if the operation fails."),
        @WritesAttribute(attribute = "box.error.code", description = "The error code returned by Box if the operation fails.")
})
@SeeAlso({FetchBoxFile.class, ListBoxFile.class})
public class FetchBoxFileRepresentation extends AbstractBoxProcessor implements VerifiableProcessor {

    private static final String BOX_FILE_URI = "https://api.box.com/2.0/files/%s/content?representation=%s";

    static final PropertyDescriptor FILE_ID = new Builder()
            .name("File ID")
            .defaultValue("${box.id}")
            .description("The ID of the Box file to retrieve.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor REPRESENTATION_TYPE = new Builder()
            .name("Representation Type")
            .description("The type of representation to fetch. Common values include 'pdf', 'text', 'jpg', 'png', etc.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully processed will be routed to this relationship.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that encounter errors during processing will be routed to this relationship.")
            .build();

    static final Relationship REL_FILE_NOT_FOUND = new Relationship.Builder()
            .name("file.not.found")
            .description("FlowFiles for which the specified Box file was not found.")
            .build();

    static final Relationship REL_REPRESENTATION_NOT_FOUND = new Relationship.Builder()
            .name("representation.not.found")
            .description("FlowFiles for which the specified Box file's requested representation was not found.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            FILE_ID,
            REPRESENTATION_TYPE
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_FILE_NOT_FOUND,
            REL_REPRESENTATION_NOT_FOUND
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

        final ComponentLog logger = getLogger();
        final String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String representationType = context.getProperty(REPRESENTATION_TYPE).evaluateAttributeExpressions(flowFile).getValue();

        try {
            // Get file info with representations
            final GetFileByIdQueryParams queryParams = new GetFileByIdQueryParams.Builder()
                    .fields(List.of("id", "name", "size", "created_at", "modified_at", "representations"))
                    .build();
            final FileFull fileInfo = boxClient.getFiles().getFileById(fileId, queryParams);

            // Find the matching representation
            final String representationUrl = findRepresentationUrl(fileInfo, representationType);
            if (representationUrl == null) {
                logger.warn("Representation {} is not available for file {}", representationType, fileId);
                flowFile = session.putAttribute(flowFile, "box.error.message", "No matching representation found");
                session.transfer(flowFile, REL_REPRESENTATION_NOT_FOUND);
                return;
            }

            // Download the representation content using Box SDK's network client
            final FetchOptions fetchOptions = new FetchOptions.Builder(representationUrl, "GET")
                    .responseFormat(ResponseFormat.BINARY)
                    .build();
            final FetchResponse response = boxClient.makeRequest(fetchOptions);

            flowFile = session.write(flowFile, outputStream -> {
                try (InputStream is = response.getContent()) {
                    is.transferTo(outputStream);
                } catch (Exception e) {
                    throw new ProcessException("Failed to download representation content", e);
                }
            });

            flowFile = session.putAllAttributes(flowFile, Map.of(
                    "box.id", fileId,
                    "box.file.name", fileInfo.getName(),
                    "box.file.size", String.valueOf(fileInfo.getSize()),
                    "box.file.created.time", fileInfo.getCreatedAt() != null ? fileInfo.getCreatedAt().toString() : "",
                    "box.file.modified.time", fileInfo.getModifiedAt() != null ? fileInfo.getModifiedAt().toString() : "",
                    "box.file.representation.type", representationType
            ));

            session.getProvenanceReporter().fetch(flowFile, BOX_FILE_URI.formatted(fileId, representationType));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final BoxAPIError e) {
            flowFile = session.putAttribute(flowFile, "box.error.message", e.getMessage());
            if (e.getResponseInfo() != null) {
                flowFile = session.putAttribute(flowFile, "box.error.code", String.valueOf(e.getResponseInfo().getStatusCode()));

                if (e.getResponseInfo().getStatusCode() == 404) {
                    logger.warn("Box file with ID {} was not found", fileId);
                    session.transfer(flowFile, REL_FILE_NOT_FOUND);
                    return;
                }
            }
            logger.error("Failed to retrieve Box file representation for file [{}]", fileId, e);
            session.transfer(flowFile, REL_FAILURE);
        } catch (final Exception e) {
            flowFile = session.putAttribute(flowFile, "box.error.message", e.getMessage());
            logger.error("Error while retrieving file [{}]", fileId, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String findRepresentationUrl(final FileFull fileInfo, final String representationType) {
        final FileFullRepresentationsField representations = fileInfo.getRepresentations();
        if (representations == null || representations.getEntries() == null) {
            return null;
        }

        for (FileFullRepresentationsEntriesField entry : representations.getEntries()) {
            final String repType = entry.getRepresentation();
            if (repType != null && repType.toLowerCase().contains(representationType.toLowerCase())) {
                final FileFullRepresentationsEntriesContentField content = entry.getContent();
                if (content != null && content.getUrlTemplate() != null) {
                    // The URL template usually has a {+asset_path} placeholder
                    return content.getUrlTemplate().replace("{+asset_path}", "");
                }
            }
        }
        return null;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context,
                                                 final ComponentLog verificationLogger,
                                                 final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE)
                .asControllerService(BoxClientService.class);
        final BoxClient client = boxClientService.getBoxClient();

        try {
            // Verify the connection by getting the current user
            client.getUsers().getUserMe();
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Box API Connection")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully validated Box connection")
                    .build());
        } catch (final Exception e) {
            verificationLogger.warn("Failed to verify configuration", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Box API Connection")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to validate Box connection: %s", e.getMessage()))
                    .build());
        }

        return results;
    }
}
