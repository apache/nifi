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
package org.apache.nifi.registry.web.security;

import org.apache.nifi.registry.authorization.Permissions;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.security.authorization.AuthorizableLookup;
import org.apache.nifi.registry.security.authorization.resource.Authorizable;
import org.apache.nifi.registry.service.AuthorizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * This is a class that Resource classes can utilized to populate fields
 * on model objects returned by the {@link org.apache.nifi.registry.service.RegistryService}
 * before returning them to a client.
 *
 * The fields cannot be populated by the RegistryService because they require
 * the {@link AuthorizationService}, which RegistryService does not depend on.
 */
@Service
public class PermissionsService {

    private AuthorizationService authorizationService;
    private AuthorizableLookup authorizableLookup;

    @Autowired
    public PermissionsService(AuthorizationService authorizationService, AuthorizableLookup authorizableLookup) {
        this.authorizationService = authorizationService;
        this.authorizableLookup = authorizableLookup;
    }

    public void populateBucketPermissions(final Iterable<Bucket> buckets) {
        Permissions topLevelBucketPermissions = authorizationService.getPermissionsForResource(authorizableLookup.getBucketsAuthorizable());
        buckets.forEach(b -> populateBucketPermissions(b, topLevelBucketPermissions));
    }

    public void populateBucketPermissions(final Bucket bucket) {
        populateBucketPermissions(bucket, null);
    }

    public void populateItemPermissions(final Iterable<? extends BucketItem> bucketItems) {
        Permissions topLevelBucketPermissions = authorizationService.getPermissionsForResource(authorizableLookup.getBucketsAuthorizable());
        bucketItems.forEach(i -> populateItemPermissions(i, topLevelBucketPermissions));
    }

    public void populateItemPermissions(final BucketItem bucketItem) {
        populateItemPermissions(bucketItem, null);
    }

    private void populateBucketPermissions(final Bucket bucket, final Permissions knownPermissions) {
        if (bucket == null) {
            return;
        }

        Permissions bucketPermissions = createPermissionsForBucketId(bucket.getIdentifier(), knownPermissions);
        bucket.setPermissions(bucketPermissions);
    }

    private void populateItemPermissions(final BucketItem bucketItem, final Permissions knownPermissions) {
        if (bucketItem == null) {
            return;
        }

        Permissions bucketItemPermissions = createPermissionsForBucketId(bucketItem.getBucketIdentifier(), knownPermissions);
        bucketItem.setPermissions(bucketItemPermissions);
    }

    private Permissions createPermissionsForBucketId(String bucketId, final Permissions knownPermissions) {
        Authorizable bucketResource = authorizableLookup.getBucketAuthorizable(bucketId);

        Permissions permissions = knownPermissions == null
                ? authorizationService.getPermissionsForResource(bucketResource)
                : authorizationService.getPermissionsForResource(bucketResource, knownPermissions);

        return permissions;
    }

}
