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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "drive", "storage", "fetch"})
@CapabilityDescription("Fetches files from a Google Drive Folder. Designed to be used in tandem with ListGoogleDrive. " +
    "For how to setup access to Google Drive please see additional details.")
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

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("A flowfile will be routed here for each successfully fetched File.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("A flowfile will be routed here for each File for which fetch was attempted but failed.")
                    .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE,
            FILE_ID,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(false, ProxyAwareTransportFactory.PROXY_SPECS)
    ));

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
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

        String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();

        FlowFile outFlowFile = flowFile;
        try {
            outFlowFile = fetchFile(fileId, session, outFlowFile);

            session.transfer(outFlowFile, REL_SUCCESS);
        } catch (GoogleJsonResponseException e) {
            handleErrorResponse(session, fileId, flowFile, e);
        } catch (Exception e) {
            handleUnexpectedError(session, flowFile, fileId, e);
        }
    }

    FlowFile fetchFile(String fileId, ProcessSession session, FlowFile outFlowFile) throws IOException {
        InputStream driveFileInputStream = driveService
                .files()
                .get(fileId)
                .executeMediaAsInputStream();

        outFlowFile = session.importFrom(driveFileInputStream, outFlowFile);

        return outFlowFile;
    }

    private void handleErrorResponse(ProcessSession session, String fileId, FlowFile outFlowFile, GoogleJsonResponseException e) {
        getLogger().error("Couldn't fetch file with id '{}'", fileId, e);

        outFlowFile = session.putAttribute(outFlowFile, ERROR_CODE_ATTRIBUTE, "" + e.getStatusCode());
        outFlowFile = session.putAttribute(outFlowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());

        session.transfer(outFlowFile, REL_FAILURE);
    }

    private void handleUnexpectedError(ProcessSession session, FlowFile flowFile, String fileId, Exception e) {
        getLogger().error("Unexpected error while fetching and processing file with id '{}'", fileId, e);

        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());

        session.transfer(flowFile, REL_FAILURE);
    }
}
