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
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.util.GoogleUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface GoogleDriveTrait {

    String DRIVE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";
    String DRIVE_SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut";
    String DRIVE_URL = "https://drive.google.com/open?id=";
    String APPLICATION_NAME = "NiFi";

    JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connect-timeout")
            .displayName("Connect Timeout")
            .description("Maximum wait time for connection to Google Drive service.")
            .required(true)
            .defaultValue("20 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("read-timeout")
            .displayName("Read Timeout")
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

                final int connectTimeout = context.getProperty(CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
                final int readTimeout = context.getProperty(READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

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

    default Map<String, String> createAttributeMap(File file) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(GoogleDriveAttributes.ID, file.getId());
        attributes.put(GoogleDriveAttributes.FILENAME, file.getName());
        attributes.put(GoogleDriveAttributes.MIME_TYPE, file.getMimeType());
        attributes.put(GoogleDriveAttributes.TIMESTAMP, String.valueOf(file.getCreatedTime()));
        attributes.put(GoogleDriveAttributes.SIZE, String.valueOf(file.getSize() != null ? file.getSize() : 0L));
        attributes.put(GoogleDriveAttributes.SIZE_AVAILABLE, String.valueOf(file.getSize() != null));
        return attributes;
    }
}
