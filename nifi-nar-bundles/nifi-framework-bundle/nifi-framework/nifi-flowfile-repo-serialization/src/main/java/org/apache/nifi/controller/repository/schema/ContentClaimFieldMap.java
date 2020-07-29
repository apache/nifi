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

import java.util.List;

import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;

public class ContentClaimFieldMap implements Record {
    private final ContentClaim contentClaim;
    private final long contentClaimOffset;
    private final ResourceClaimFieldMap resourceClaimFieldMap;
    private final RecordSchema schema;

    public ContentClaimFieldMap(final ContentClaim contentClaim, final long contentClaimOffset, final RecordSchema schema) {
        this.contentClaim = contentClaim;
        this.contentClaimOffset = contentClaimOffset;
        this.schema = schema;

        final List<RecordField> resourceClaimFields = schema.getField(ContentClaimSchema.RESOURCE_CLAIM).getSubFields();
        final RecordSchema resourceClaimSchema = new RecordSchema(resourceClaimFields);
        this.resourceClaimFieldMap = new ResourceClaimFieldMap(contentClaim.getResourceClaim(), resourceClaimSchema);
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        switch (fieldName) {
            case ContentClaimSchema.RESOURCE_CLAIM:
                return resourceClaimFieldMap;
            case ContentClaimSchema.CONTENT_CLAIM_LENGTH:
                return contentClaim.getLength();
            case ContentClaimSchema.CONTENT_CLAIM_OFFSET:
                return contentClaimOffset;
            case ContentClaimSchema.RESOURCE_CLAIM_OFFSET:
                return contentClaim.getOffset();
            default:
                return null;
        }
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public int hashCode() {
        return (int) (31 + contentClaimOffset + 21 * resourceClaimFieldMap.hashCode());
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        ContentClaimFieldMap other = (ContentClaimFieldMap) obj;
        if (contentClaimOffset != other.contentClaimOffset) {
            return false;
        }

        if (resourceClaimFieldMap == null) {
            if (other.resourceClaimFieldMap != null) {
                return false;
            }
        } else if (!resourceClaimFieldMap.equals(other.resourceClaimFieldMap)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "ContentClaimFieldMap[" + contentClaim + "]";
    }

    public static ContentClaim getContentClaim(final Record claimRecord, final ResourceClaimManager resourceClaimManager) {
        final Record resourceClaimRecord = (Record) claimRecord.getFieldValue(ContentClaimSchema.RESOURCE_CLAIM);
        final String container = (String) resourceClaimRecord.getFieldValue(ContentClaimSchema.CLAIM_CONTAINER);
        final String section = (String) resourceClaimRecord.getFieldValue(ContentClaimSchema.CLAIM_SECTION);
        final String identifier = (String) resourceClaimRecord.getFieldValue(ContentClaimSchema.CLAIM_IDENTIFIER);
        final Boolean lossTolerant = (Boolean) resourceClaimRecord.getFieldValue(ContentClaimSchema.LOSS_TOLERANT);

        final Long length = (Long) claimRecord.getFieldValue(ContentClaimSchema.CONTENT_CLAIM_LENGTH);
        final Long resourceOffset = (Long) claimRecord.getFieldValue(ContentClaimSchema.RESOURCE_CLAIM_OFFSET);

        // Make sure that we preserve the existing ResourceClaim, if there is already one held by the Resource Claim Manager
        // because we need to honor its determination of whether or not the claim is writable. If the Resource Claim Manager
        // does not have a copy of this Resource Claim, then we can go ahead and just create one and assume that it is not
        // writable (because if it were writable, then the Resource Claim Manager would know about it).
        ResourceClaim resourceClaim = resourceClaimManager.getResourceClaim(container, section, identifier);
        if (resourceClaim == null) {
            resourceClaim = resourceClaimManager.newResourceClaim(container, section, identifier, lossTolerant, false);
        }

        final StandardContentClaim contentClaim = new StandardContentClaim(resourceClaim, resourceOffset);
        contentClaim.setLength(length);

        return contentClaim;
    }

    public static Long getContentClaimOffset(final Record claimRecord) {
        return (Long) claimRecord.getFieldValue(ContentClaimSchema.CONTENT_CLAIM_OFFSET);
    }
}
