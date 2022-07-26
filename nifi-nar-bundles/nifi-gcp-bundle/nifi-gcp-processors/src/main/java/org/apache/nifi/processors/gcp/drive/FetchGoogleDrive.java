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
package org.apache.nifi.processors.gcp.drive;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "drive", "storage", "fetch"})
@CapabilityDescription("Fetches files from a Google Drive Folder. Designed to be used in tandem with ListGoogleDrive.")
@SeeAlso({ListGoogleDrive.class})
@WritesAttributes({
        @WritesAttribute(attribute = FetchGoogleDrive.ERROR_CODE_ATTRIBUTE, description = "The error code returned by Google Drive when the fetch of a file fails"),
        @WritesAttribute(attribute = FetchGoogleDrive.ERROR_MESSAGE_ATTRIBUTE, description = "The error message returned by Google Drive when the fetch of a file fails")
})
public class FetchGoogleDrive extends AbstractProcessor implements GoogleDriveTrait {
    public static final String ERROR_CODE_ATTRIBUTE = "error.code";
    public static final String ERROR_MESSAGE_ATTRIBUTE = "error.message";

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor
            .Builder().name("drive-file-id")
            .displayName("File ID")
            .description("The Drive ID of the File to fetch")
            .required(true)
            .defaultValue("${drive.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming Google Driver File meta-data as NiFi Records."
                    + " If not set, the Processor expects as attributes of a separate flowfile for each File to fetch.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(false)
            .build();

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("A flowfile will be routed here for each successfully fetched File.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("A flowfile will be routed here for each File for which fetch was attempted but failed.")
                    .build();

    public static final Relationship REL_INPUT_FAILURE =
            new Relationship.Builder().name("input_failure")
                    .description("The incoming flowfile will be routed here if it's content could not be processed.")
                    .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE,
            FILE_ID,
            RECORD_READER,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(false, ProxyAwareTransportFactory.PROXY_SPECS)
    ));

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE,
            REL_INPUT_FAILURE
    )));

    private volatile Drive driveService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);

        driveService = createDriveService(
                context,
                new ProxyAwareTransportFactory(proxyConfiguration).create(),
                DriveScopes.DRIVE, DriveScopes.DRIVE_FILE
        );
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (context.getProperty(RECORD_READER).isSet()) {
            RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

            try (InputStream inFlowFile = session.read(flowFile)) {
                final Map<String, String> flowFileAttributes = flowFile.getAttributes();
                final RecordReader reader = recordReaderFactory.createRecordReader(flowFileAttributes, inFlowFile, flowFile.getSize(), getLogger());

                Record record;
                while ((record = reader.nextRecord()) != null) {
                    String fileId = record.getAsString(GoogleDriveFileInfo.ID);
                    FlowFile outFlowFile = session.create(flowFile);
                    try {
                        addAttributes(session, outFlowFile, record);

                        fetchFile(fileId, session, outFlowFile);

                        session.transfer(outFlowFile, REL_SUCCESS);
                    } catch (GoogleJsonResponseException e) {
                        handleErrorResponse(session, fileId, outFlowFile, e);
                    } catch (Exception e) {
                        handleUnexpectedError(session, outFlowFile, fileId, e);
                    }
                }
                session.remove(flowFile);
            } catch (IOException | MalformedRecordException | SchemaNotFoundException e) {
                getLogger().error("Couldn't read file metadata content as records from incoming flowfile", e);

                session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());

                session.transfer(flowFile, REL_INPUT_FAILURE);
            } catch (Exception e) {
                getLogger().error("Unexpected error while processing incoming flowfile", e);

                session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());

                session.transfer(flowFile, REL_INPUT_FAILURE);
            }
        } else {
            String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();
            FlowFile outFlowFile = flowFile;
            try {
                fetchFile(fileId, session, outFlowFile);

                session.transfer(outFlowFile, REL_SUCCESS);
            } catch (GoogleJsonResponseException e) {
                handleErrorResponse(session, fileId, flowFile, e);
            } catch (Exception e) {
                handleUnexpectedError(session, flowFile, fileId, e);
            }
        }
        session.commitAsync();
    }

    private void addAttributes(ProcessSession session, FlowFile outFlowFile, Record record) {
        Map<String, String> attributes = new HashMap<>();

        for (GoogleDriveFlowFileAttribute attribute : GoogleDriveFlowFileAttribute.values()) {
            Optional.ofNullable(attribute.getValue(record))
                    .ifPresent(value -> attributes.put(attribute.getName(), value));
        }

        session.putAllAttributes(outFlowFile, attributes);
    }

    void fetchFile(String fileId, ProcessSession session, FlowFile outFlowFile) throws IOException {
        InputStream driveFileInputStream = driveService
                .files()
                .get(fileId)
                .executeMediaAsInputStream();

        session.importFrom(driveFileInputStream, outFlowFile);
    }

    private void handleErrorResponse(ProcessSession session, String fileId, FlowFile outFlowFile, GoogleJsonResponseException e) {
        getLogger().error("Couldn't fetch file with id '{}'", fileId, e);

        session.putAttribute(outFlowFile, ERROR_CODE_ATTRIBUTE, "" + e.getStatusCode());
        session.putAttribute(outFlowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());

        session.transfer(outFlowFile, REL_FAILURE);
    }

    private void handleUnexpectedError(ProcessSession session, FlowFile flowFile, String fileId, Exception e) {
        getLogger().error("Unexpected error while fetching and processing file with id '{}'", fileId, e);

        session.putAttribute(flowFile, ERROR_CODE_ATTRIBUTE, "N/A");
        session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());

        session.transfer(flowFile, REL_FAILURE);
    }
}
