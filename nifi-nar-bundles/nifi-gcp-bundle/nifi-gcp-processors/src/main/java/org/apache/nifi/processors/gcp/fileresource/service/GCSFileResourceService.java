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
package org.apache.nifi.processors.gcp.fileresource.service;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.util.GoogleUtils;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.gcp.storage.StorageAttributes.BUCKET_ATTR;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.BUCKET_DESC;
import static org.apache.nifi.processors.gcp.storage.StorageAttributes.KEY_DESC;

@Tags({"file", "resource", "gcs"})
@CapabilityDescription("Provides a Google Compute Storage (GCS) file resource for other components.")
public class GCSFileResourceService extends AbstractControllerService implements FileResourceService {

    public static final PropertyDescriptor BUCKET = new PropertyDescriptor
            .Builder().name("gcs-bucket")
            .displayName("Bucket")
            .description(BUCKET_DESC)
            .required(true)
            .defaultValue("${" + BUCKET_ATTR + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor
            .Builder().name("gcs-key")
            .displayName("Name")
            .description(KEY_DESC)
            .required(true)
            .defaultValue("${" + CoreAttributes.FILENAME.key() + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Arrays.asList(
            BUCKET,
            KEY,
            GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE
    );

    private volatile PropertyContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.context = context;
    }

    @OnDisabled
    public void onDisabled() {
        this.context = null;
    }

    @Override
    public FileResource getFileResource(Map<String, String> attributes) {
        final GCPCredentialsService gcpCredentialsService = context.getProperty(GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE).asControllerService(GCPCredentialsService.class);
        final Storage storage = getCloudService(gcpCredentialsService.getGoogleCredentials());

        try {
            return fetchBlob(storage, attributes);
        } catch (final StorageException | IOException e) {
            throw new ProcessException("Failed to fetch GCS Object", e);
        }
    }

    protected Storage getCloudService(GoogleCredentials credentials) {
        final StorageOptions storageOptions = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build();

        return storageOptions.getService();
    }

    /**
     * Fetching blob from the provided bucket.
     * @param storage gcs storage
     * @param attributes configuration attributes
     * @return fetched blob as FileResource
     * @throws IOException exception caused by missing parameters
     */
    private FileResource fetchBlob(Storage storage, Map<String, String> attributes) throws IOException {
        final String bucketName = context.getProperty(BUCKET).evaluateAttributeExpressions(attributes).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(attributes).getValue();

        final BlobId blobId = BlobId.of(bucketName, key);
        if (blobId.getName() == null || blobId.getName().isEmpty()) {
            throw new IllegalArgumentException("Name is required");
        }

        final Blob blob = storage.get(blobId);
        if (blob == null) {
            throw new StorageException(404, "Blob " + blobId + " not found");
        }

        final ReadChannel reader = storage.reader(blob.getBlobId());
        return new FileResource(Channels.newInputStream(reader), blob.getSize());
    }
}
