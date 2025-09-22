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
import com.box.sdk.BoxFile.Info;
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
        @WritesAttribute(attribute = "box.file.mime.type", description = "The MIME type of the file."),
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

    /*
     * The maximum number of retries for polling the status of the generated representation.
     * Each retry waits for 100 milliseconds before the next attempt, set in the Box SDK.
     * Total wait time is 5 seconds.
     */
    private static final int MAX_RETRIES = 50;

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

        final ComponentLog logger = getLogger();
        final String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String representationType = context.getProperty(REPRESENTATION_TYPE).evaluateAttributeExpressions(flowFile).getValue();

        try {
            final BoxFile boxFile = getBoxFile(fileId);
            final Info fileInfo = boxFile.getInfo();

            flowFile = session.write(flowFile, outputStream ->
                    // Download the file representation, box sdk handles a request to create representation if it doesn't exist
                    boxFile.getRepresentationContent("[" + representationType + "]", "", outputStream, MAX_RETRIES)
            );

            flowFile = session.putAllAttributes(flowFile, Map.of(
                    "box.id", fileId,
                    "box.file.name", fileInfo.getName(),
                    "box.file.size", String.valueOf(fileInfo.getSize()),
                    "box.file.created.time", fileInfo.getCreatedAt().toString(),
                    "box.file.modified.time", fileInfo.getModifiedAt().toString(),
                    "box.file.mime.type", fileInfo.getType(),
                    "box.file.representation.type", representationType
            ));

            session.getProvenanceReporter().fetch(flowFile, BOX_FILE_URI.formatted(fileId, representationType));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final BoxAPIResponseException e) {
            flowFile = session.putAttribute(flowFile, "box.error.message", e.getMessage());
            flowFile = session.putAttribute(flowFile, "box.error.code", String.valueOf(e.getResponseCode()));

            if (e.getResponseCode() == 404) {
                logger.warn("Box file with ID {} was not found or representation {} is not available", fileId, representationType);
                session.transfer(flowFile, REL_FILE_NOT_FOUND);
            } else {
                logger.error("Failed to retrieve Box file representation for file [{}]", fileId, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final BoxAPIException e) {
            flowFile = session.putAttribute(flowFile, "box.error.message", e.getMessage());
            flowFile = session.putAttribute(flowFile, "box.error.code", String.valueOf(e.getResponseCode()));

            // Check if this is the "No matching representations found" error
            if (e.getMessage() != null && e.getMessage().toLowerCase().startsWith("no matching representations found for requested")) {
                logger.warn("Representation {} is not available for file {}: {}", representationType, fileId, e.getMessage());
                session.transfer(flowFile, REL_REPRESENTATION_NOT_FOUND);
            } else {
                logger.error("BoxAPIException while retrieving file [{}]", fileId, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    /**
     * Get BoxFile object for the given fileId. Required for testing purposes to mock BoxFile.
     *
     * @param fileId fileId
     * @return BoxFile object
     */
    protected BoxFile getBoxFile(final String fileId) {
        return new BoxFile(boxAPIConnection, fileId);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context,
                                                 final ComponentLog verificationLogger,
                                                 final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE)
                .asControllerService(BoxClientService.class);
        final BoxAPIConnection boxAPIConnection = boxClientService.getBoxApiConnection();

        try {
            boxAPIConnection.refresh();
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
