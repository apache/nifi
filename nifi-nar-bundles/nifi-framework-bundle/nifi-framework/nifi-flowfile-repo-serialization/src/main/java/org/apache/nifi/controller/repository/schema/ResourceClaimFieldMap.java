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

package org.apache.nifi.controller.repository.schema;

import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;

public class ResourceClaimFieldMap implements Record {
    private final ResourceClaim resourceClaim;
    private final RecordSchema schema;

    public ResourceClaimFieldMap(final ResourceClaim resourceClaim, final RecordSchema schema) {
        this.resourceClaim = resourceClaim;
        this.schema = schema;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        switch (fieldName) {
            case ContentClaimSchema.CLAIM_CONTAINER:
                return resourceClaim.getContainer();
            case ContentClaimSchema.CLAIM_SECTION:
                return resourceClaim.getSection();
            case ContentClaimSchema.CLAIM_IDENTIFIER:
                return resourceClaim.getId();
            case ContentClaimSchema.LOSS_TOLERANT:
                return resourceClaim.isLossTolerant();
        }

        return null;
    }

    public static ResourceClaim getResourceClaim(final Record record, final ResourceClaimManager claimManager) {
        final String container = (String) record.getFieldValue(ContentClaimSchema.CLAIM_CONTAINER);
        final String section = (String) record.getFieldValue(ContentClaimSchema.CLAIM_SECTION);
        final String identifier = (String) record.getFieldValue(ContentClaimSchema.CLAIM_IDENTIFIER);
        final Boolean lossTolerant = (Boolean) record.getFieldValue(ContentClaimSchema.LOSS_TOLERANT);

        // Make sure that we preserve the existing ResourceClaim, if there is already one held by the Resource Claim Manager
        // because we need to honor its determination of whether or not the claim is writable. If the Resource Claim Manager
        // does not have a copy of this Resource Claim, then we can go ahead and just create one and assume that it is not
        // writable (because if it were writable, then the Resource Claim Manager would know about it).
        ResourceClaim resourceClaim = claimManager.getResourceClaim(container, section, identifier);
        if (resourceClaim == null) {
            resourceClaim = claimManager.newResourceClaim(container, section, identifier, lossTolerant, false);
        }

        return resourceClaim;
    }

    @Override
    public int hashCode() {
        return 41 + 91 * resourceClaim.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        if (obj.getClass() != ResourceClaimFieldMap.class) {
            return false;
        }

        final ResourceClaimFieldMap other = (ResourceClaimFieldMap) obj;
        return resourceClaim.equals(other.resourceClaim);
    }
}
