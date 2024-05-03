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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.gcp.util.GoogleUtils;

import java.util.Arrays;
import java.util.Collection;

public interface GoogleDriveTrait {

    String DRIVE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";
    String DRIVE_URL = "https://drive.google.com/open?id=" ;
    String APPLICATION_NAME = "NiFi";

    JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    default Drive createDriveService(ProcessContext context, HttpTransport httpTransport, String... scopes) {
        Drive driveService = new Drive.Builder(
                httpTransport,
                JSON_FACTORY,
                getHttpCredentialsAdapter(
                        context,
                        Arrays.asList(scopes)
                )
        )
                .setApplicationName(APPLICATION_NAME)
                .build();

        return driveService;
    }

    default HttpCredentialsAdapter getHttpCredentialsAdapter(
            final ProcessContext context,
            final Collection<String> scopes
    ) {
        GoogleCredentials googleCredentials = getGoogleCredentials(context);

        HttpCredentialsAdapter httpCredentialsAdapter = new HttpCredentialsAdapter(googleCredentials.createScoped(scopes));

        return httpCredentialsAdapter;
    }

    default GoogleCredentials getGoogleCredentials(final ProcessContext context) {
        final GCPCredentialsService gcpCredentialsService = context.getProperty(GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(GCPCredentialsService.class);

        return gcpCredentialsService.getGoogleCredentials();
    }

    default Map<String, String> createAttributeMap(File file) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(GoogleDriveAttributes.ID, file.getId());
        attributes.put(GoogleDriveAttributes.FILENAME, file.getName());
        attributes.put(GoogleDriveAttributes.MIME_TYPE, file.getMimeType());
        attributes.put(GoogleDriveAttributes.TIMESTAMP, String.valueOf(file.getCreatedTime()));
        attributes.put(GoogleDriveAttributes.SIZE, String.valueOf(file.getSize()));
        return attributes;
    }
}
