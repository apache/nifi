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

import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_CODE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP_DESC;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
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

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "drive", "storage", "fetch"})
@CapabilityDescription("Fetches files from a Google Drive Folder. Designed to be used in tandem with ListGoogleDrive. " +
    "Please see Additional Details to set up access to Google Drive.")
@SeeAlso({ListGoogleDrive.class, PutGoogleDrive.class})
@ReadsAttribute(attribute = ID, description = ID_DESC)
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "mime.type", description = MIME_TYPE_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class FetchGoogleDrive extends AbstractProcessor implements GoogleDriveTrait {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor
            .Builder().name("drive-file-id")
            .displayName("File ID")
            .description("The Drive ID of the File to fetch. "
            + "Please see Additional Details to obtain Drive ID.")
            .required(true)
            .defaultValue("${drive.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("A FlowFile will be routed here for each successfully fetched File.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("A FlowFile will be routed here for each File for which fetch was attempted but failed.")
                    .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE,
            FILE_ID,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(false, ProxyAwareTransportFactory.PROXY_SPECS)
    ));

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    private volatile Drive driveService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
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

        final String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();

        final long startNanos = System.nanoTime();
        try {
            flowFile = fetchFile(fileId, session, flowFile);

            final File fileMetadata = fetchFileMetadata(fileId);
            final Map<String, String> attributes = createAttributeMap(fileMetadata);
            flowFile = session.putAllAttributes(flowFile, attributes);

            final String url = DRIVE_URL + fileMetadata.getId();
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(flowFile, url, transferMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (GoogleJsonResponseException e) {
            handleErrorResponse(session, fileId, flowFile, e);
        } catch (Exception e) {
            handleUnexpectedError(session, flowFile, fileId, e);
        }
    }

    private FlowFile fetchFile(String fileId, ProcessSession session, FlowFile flowFile) throws IOException {
        try (final InputStream driveFileInputStream = driveService
                .files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .executeMediaAsInputStream()) {

            return session.importFrom(driveFileInputStream, flowFile);
        }
    }

    private File fetchFileMetadata(String fileId) throws IOException {
        return driveService
                .files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .setFields("id, name, createdTime, mimeType, size")
                .execute();
    }

    private void handleErrorResponse(ProcessSession session, String fileId, FlowFile flowFile, GoogleJsonResponseException e) {
        getLogger().error("Fetching File [{}] failed", fileId, e);

        flowFile = session.putAttribute(flowFile, ERROR_CODE, "" + e.getStatusCode());
        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());

        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private void handleUnexpectedError(ProcessSession session, FlowFile flowFile, String fileId, Exception e) {
        getLogger().error("Fetching File [{}] failed", fileId, e);

        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());

        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }
}
