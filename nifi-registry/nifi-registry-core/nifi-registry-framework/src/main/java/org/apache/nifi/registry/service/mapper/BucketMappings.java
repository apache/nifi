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
package org.apache.nifi.registry.service.mapper;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.db.entity.BucketEntity;

import java.util.Date;

/**
 * Mappings between bucket DB entities and data model.
 */
public class BucketMappings {

    public static BucketEntity map(final Bucket bucket) {
        final BucketEntity bucketEntity = new BucketEntity();
        bucketEntity.setId(bucket.getIdentifier());
        bucketEntity.setName(bucket.getName());
        bucketEntity.setDescription(bucket.getDescription());
        bucketEntity.setCreated(new Date(bucket.getCreatedTimestamp()));
        bucketEntity.setAllowExtensionBundleRedeploy(bucket.isAllowBundleRedeploy());
        bucketEntity.setAllowPublicRead(bucket.isAllowPublicRead());
        return bucketEntity;
    }

    public static Bucket map(final BucketEntity bucketEntity) {
        final Bucket bucket = new Bucket();
        bucket.setIdentifier(bucketEntity.getId());
        bucket.setName(bucketEntity.getName());
        bucket.setDescription(bucketEntity.getDescription());
        bucket.setCreatedTimestamp(bucketEntity.getCreated().getTime());
        bucket.setAllowBundleRedeploy(bucketEntity.isAllowExtensionBundleRedeploy());
        bucket.setAllowPublicRead(bucketEntity.isAllowPublicRead());
        return bucket;
    }

}
