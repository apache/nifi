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
package org.apache.nifi.processors.azure.storage;

import com.azure.storage.blob.BlobClient;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;

import java.util.Map;

@Tags({"azure", "microsoft", "cloud", "storage", "blob"})
@SeeAlso({ListAzureBlobStorage_v12.class, FetchAzureBlobStorage_v12.class, PutAzureBlobStorage_v12.class,
        CopyAzureBlobStorage_v12.class, DeleteAzureBlobStorage_v12.class, GetAzureBlobStorageMetadata_v12.class})
@CapabilityDescription("Retrieves tags from the specified blob from Azure Blob Storage. The processor uses Azure Blob Storage client library v12.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({@WritesAttribute(attribute = "azure.tag.<tag-key>", description = "The value of the retrieved tag")})
public class GetAzureBlobStorageTags_v12 extends AbstractGetAzureBlobStoragePropertiesProcessor_v12 {

    private static final String ATTRIBUTE_PREFIX = "azure.tag.%s";

    @Override
    protected String getAttributePrefix() {
        return ATTRIBUTE_PREFIX;
    }

    @Override
    protected Map<String, String> fetchProperties(final BlobClient blobClient) {
        return blobClient.getTags();
    }
}
