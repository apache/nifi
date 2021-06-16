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
package org.apache.nifi.registry.client.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Jersey implementation of BucketClient.
 */
public class JerseyBucketClient extends AbstractJerseyClient implements BucketClient {

    private final WebTarget bucketsTarget;


    public JerseyBucketClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyBucketClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.bucketsTarget = baseTarget.path("/buckets");
    }

    @Override
    public Bucket create(final Bucket bucket) throws NiFiRegistryException, IOException {
        if (bucket == null) {
            throw new IllegalArgumentException("Bucket cannot be null");
        }

        return executeAction("Error creating bucket", () -> {
            return getRequestBuilder(bucketsTarget)
                    .post(
                            Entity.entity(bucket, MediaType.APPLICATION_JSON),
                            Bucket.class
                    );
        });

    }

    @Override
    public Bucket get(final String bucketId) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket ID cannot be blank");
        }

        return executeAction("Error retrieving bucket", () -> {
            final WebTarget target = bucketsTarget
                    .path("/{bucketId}")
                    .resolveTemplate("bucketId", bucketId);

            return getRequestBuilder(target).get(Bucket.class);
        });

    }

    @Override
    public Bucket update(final Bucket bucket) throws NiFiRegistryException, IOException {
        if (bucket == null) {
            throw new IllegalArgumentException("Bucket cannot be null");
        }

        if (StringUtils.isBlank(bucket.getIdentifier())) {
            throw new IllegalArgumentException("Bucket Identifier must be provided");
        }

        return executeAction("Error updating bucket", () -> {
            final WebTarget target = bucketsTarget
                    .path("/{bucketId}")
                    .resolveTemplate("bucketId", bucket.getIdentifier());

            return getRequestBuilder(target)
                    .put(
                            Entity.entity(bucket, MediaType.APPLICATION_JSON),
                            Bucket.class
                    );

        });
    }

    @Override
    public Bucket delete(final String bucketId) throws NiFiRegistryException, IOException {
        return delete(bucketId, null);
    }

    @Override
    public Bucket delete(final String bucketId, final RevisionInfo revision) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket ID cannot be blank");
        }

        return executeAction("Error deleting bucket", () -> {
            WebTarget target = bucketsTarget
                    .path("/{bucketId}")
                    .resolveTemplate("bucketId", bucketId);

            target = addRevisionQueryParams(target, revision);

            return getRequestBuilder(target).delete(Bucket.class);
        });
    }

    @Override
    public Fields getFields() throws NiFiRegistryException, IOException {
        return executeAction("Error retrieving bucket field info", () -> {
            final WebTarget target = bucketsTarget
                    .path("/fields");

            return getRequestBuilder(target).get(Fields.class);
        });
    }

    @Override
    public List<Bucket> getAll() throws NiFiRegistryException, IOException {
        return executeAction("Error retrieving all buckets", () -> {
            final Bucket[] buckets = getRequestBuilder(bucketsTarget).get(Bucket[].class);
            return buckets == null ? Collections.emptyList() : Arrays.asList(buckets);
        });
    }

}
