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

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.util.GoogleUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public interface GoogleDriveTrait {

    String DRIVE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";
    String DRIVE_SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut";
    String DRIVE_URL = "https://drive.google.com/open?id=";
    String APPLICATION_NAME = "NiFi";
    String OLD_CONNECT_TIMEOUT_PROPERTY_NAME = "connect-timeout";
    String OLD_READ_TIMEOUT_PROPERTY_NAME = "read-timeout";

    JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connect Timeout")
            .description("Maximum wait time for connection to Google Drive service.")
            .required(true)
            .defaultValue("20 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Maximum wait time for response from Google Drive service.")
            .required(true)
            .defaultValue("60 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    default Drive createDriveService(ProcessContext context, HttpTransport httpTransport, String... scopes) {
        Drive driveService = new Drive.Builder(
                httpTransport,
                JSON_FACTORY,
                createHttpRequestInitializer(
                        context,
                        Arrays.asList(scopes)
                )
        )
                .setApplicationName(APPLICATION_NAME)
                .build();

        return driveService;
    }

    default HttpRequestInitializer createHttpRequestInitializer(
            final ProcessContext context,
            final Collection<String> scopes
    ) {
        final GoogleCredentials googleCredentials = getGoogleCredentials(context).createScoped(scopes);

        final HttpCredentialsAdapter httpCredentialsAdapter = new HttpCredentialsAdapter(googleCredentials) {
            @Override
            public void initialize(HttpRequest request) throws IOException {
                super.initialize(request);

                final int connectTimeout = context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
                final int readTimeout = context.getProperty(READ_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();

                request.setConnectTimeout(connectTimeout);
                request.setReadTimeout(readTimeout);
            }
        };

        return httpCredentialsAdapter;
    }

    default GoogleCredentials getGoogleCredentials(final ProcessContext context) {
        final GCPCredentialsService gcpCredentialsService = context.getProperty(GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(GCPCredentialsService.class);

        return gcpCredentialsService.getGoogleCredentials();
    }

    default GoogleDriveFileInfo.Builder createGoogleDriveFileInfoBuilder(final File file) {
        return new GoogleDriveFileInfo.Builder()
                .id(file.getId())
                .fileName(file.getName())
                .size(file.getSize() != null ? file.getSize() : 0L)
                .sizeAvailable(file.getSize() != null)
                .createdTime(Optional.ofNullable(file.getCreatedTime()).map(DateTime::getValue).orElse(0L))
                .modifiedTime(Optional.ofNullable(file.getModifiedTime()).map(DateTime::getValue).orElse(0L))
                .mimeType(file.getMimeType());
    }

    default FolderDetails getFolderDetails(final Drive driveService, final String folderId) {
        try {
            final File folder = driveService
                    .files()
                    .get(folderId)
                    .setSupportsAllDrives(true)
                    .setFields("name, driveId")
                    .execute();

            final String sharedDriveId = folder.getDriveId();
            String sharedDriveName = null;
            if (sharedDriveId != null) {
                try {
                    sharedDriveName = driveService
                            .drives()
                            .get(sharedDriveId)
                            .setFields("name")
                            .execute()
                            .getName();
                } catch (HttpResponseException e) {
                    if (e.getStatusCode() != HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                        throw e;
                    }
                    // if the user does not have permission to the Shared Drive root, the service returns HTTP 404 (Not Found)
                    // the Shared Drive name can not be retrieved in this case and will not be added as a FlowFile attribute
                }
            }

            final String folderName;
            if (folderId.equals(sharedDriveId)) {
                // if folderId points to a Shared Drive root, files() returns "Drive" for the name and the result of drives() contains the real name
                folderName = sharedDriveName;
            } else {
                folderName = folder.getName();
            }

            return new FolderDetails(folderId, folderName, sharedDriveId, sharedDriveName);
        } catch (IOException ioe) {
            throw new ProcessException("Error while retrieving folder metadata", ioe);
        }
    }

    class FolderDetails {
        private final String folderId;
        private final String folderName;
        private final String sharedDriveId;
        private final String sharedDriveName;

        FolderDetails(String folderId, String folderName, String sharedDriveId, String sharedDriveName) {
            this.folderId = folderId;
            this.folderName = folderName;
            this.sharedDriveId = sharedDriveId;
            this.sharedDriveName = sharedDriveName;
        }

        public String getFolderId() {
            return folderId;
        }

        public String getFolderName() {
            return folderName;
        }

        public String getSharedDriveId() {
            return sharedDriveId;
        }

        public String getSharedDriveName() {
            return sharedDriveName;
        }
    }
}
