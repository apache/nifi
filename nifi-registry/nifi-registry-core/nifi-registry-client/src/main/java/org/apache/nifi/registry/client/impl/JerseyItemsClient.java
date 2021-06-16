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
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.client.ItemsClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.field.Fields;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Jersey implementation of ItemsClient.
 */
public class JerseyItemsClient extends AbstractJerseyClient implements ItemsClient {

    private final WebTarget itemsTarget;

    public JerseyItemsClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyItemsClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.itemsTarget = baseTarget.path("/items");
    }



    @Override
    public List<BucketItem> getAll() throws NiFiRegistryException, IOException {
        return executeAction("", () -> {
            WebTarget target = itemsTarget;
            final BucketItem[] bucketItems = getRequestBuilder(target).get(BucketItem[].class);
            return bucketItems == null ? Collections.emptyList() : Arrays.asList(bucketItems);
        });
    }

    @Override
    public List<BucketItem> getByBucket(final String bucketId)
            throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Bucket Identifier cannot be blank");
        }

        return executeAction("", () -> {
            WebTarget target = itemsTarget
                    .path("/{bucketId}")
                    .resolveTemplate("bucketId", bucketId);

            final BucketItem[] bucketItems = getRequestBuilder(target).get(BucketItem[].class);
            return bucketItems == null ? Collections.emptyList() : Arrays.asList(bucketItems);
        });
    }

    @Override
    public Fields getFields() throws NiFiRegistryException, IOException {
        return executeAction("", () -> {
            final WebTarget target = itemsTarget.path("/fields");
            return getRequestBuilder(target).get(Fields.class);

        });
    }

}
