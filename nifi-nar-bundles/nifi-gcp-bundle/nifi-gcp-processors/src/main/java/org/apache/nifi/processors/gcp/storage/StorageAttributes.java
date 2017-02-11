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
package org.apache.nifi.processors.gcp.storage;


/**
 * Common attributes being written and accessed through Google Cloud Storage.
 */
public class StorageAttributes {
    private StorageAttributes() {}

    public static final String BUCKET_ATTR = "gcs.bucket";
    public static final String BUCKET_DESC = "Bucket of the object.";

    public static final String KEY_ATTR = "gcs.key";
    public static final String KEY_DESC = "Name of the object.";

    public static final String SIZE_ATTR = "gcs.size";
    public static final String SIZE_DESC = "Size of the object.";

    public static final String CACHE_CONTROL_ATTR = "gcs.cache.control";
    public static final String CACHE_CONTROL_DESC = "Data cache control of the object.";

    public static final String COMPONENT_COUNT_ATTR = "gcs.component.count";
    public static final String COMPONENT_COUNT_DESC = "The number of components which make up the object.";

    public static final String CONTENT_DISPOSITION_ATTR = "gcs.content.disposition";
    public static final String CONTENT_DISPOSITION_DESC = "The data content disposition of the object.";

    public static final String CONTENT_ENCODING_ATTR = "gcs.content.encoding";
    public static final String CONTENT_ENCODING_DESC = "The content encoding of the object.";

    public static final String CONTENT_LANGUAGE_ATTR = "gcs.content.language";
    public static final String CONTENT_LANGUAGE_DESC = "The content language of the object.";

    public static final String CRC32C_ATTR = "gcs.crc32c";
    public static final String CRC32C_DESC = "The CRC32C checksum of object's data, encoded in base64 in " +
                                                    "big-endian order.";

    public static final String CREATE_TIME_ATTR = "gcs.create.time";
    public static final String CREATE_TIME_DESC = "The creation time of the object (milliseconds)";

    public static final String DELETE_TIME_ATTR = "gcs.delete.time";
    public static final String DELETE_TIME_DESC = "The deletion time of the object (milliseconds)";

    public static final String UPDATE_TIME_ATTR = "gcs.update.time";
    public static final String UPDATE_TIME_DESC = "The last modification time of the object (milliseconds)";

    public static final String ENCRYPTION_ALGORITHM_ATTR = "gcs.encryption.algorithm";
    public static final String ENCRYPTION_ALGORITHM_DESC = "The algorithm used to encrypt the object.";

    public static final String ENCRYPTION_SHA256_ATTR = "gcs.encryption.sha256";
    public static final String ENCRYPTION_SHA256_DESC = "The SHA256 hash of the key used to encrypt the object";

    public static final String ETAG_ATTR = "gcs.etag";
    public static final String ETAG_DESC = "The HTTP 1.1 Entity tag for the object.";

    public static final String GENERATED_ID_ATTR = "gcs.generated.id";
    public static final String GENERATED_ID_DESC = "The service-generated for the object";

    public static final String GENERATION_ATTR = "gcs.generation";
    public static final String GENERATION_DESC = "The data generation of the object.";

    public static final String MD5_ATTR = "gcs.md5";
    public static final String MD5_DESC = "The MD5 hash of the object's data encoded in base64.";

    public static final String MEDIA_LINK_ATTR = "gcs.media.link";
    public static final String MEDIA_LINK_DESC = "The media download link to the object.";

    public static final String METAGENERATION_ATTR = "gcs.metageneration";
    public static final String METAGENERATION_DESC = "The metageneration of the object.";

    public static final String OWNER_ATTR = "gcs.owner";
    public static final String OWNER_DESC = "The owner (uploader) of the object.";

    public static final String OWNER_TYPE_ATTR = "gcs.owner.type";
    public static final String OWNER_TYPE_DESC = "The ACL entity type of the uploader of the object.";

    public static final String URI_ATTR = "gcs.uri";
    public static final String URI_DESC = "The URI of the object as a string.";
}
