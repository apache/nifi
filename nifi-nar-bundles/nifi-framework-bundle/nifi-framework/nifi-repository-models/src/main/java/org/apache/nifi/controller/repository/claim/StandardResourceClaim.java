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
package org.apache.nifi.controller.repository.claim;

public class StandardResourceClaim implements ResourceClaim, Comparable<ResourceClaim> {
    private final ResourceClaimManager claimManager;
    private final String id;
    private final String container;
    private final String section;
    private final boolean lossTolerant;
    private final int hashCode;
    private volatile boolean writable = true;

    public StandardResourceClaim(final ResourceClaimManager claimManager, final String container, final String section, final String id, final boolean lossTolerant) {
        this.claimManager = claimManager;
        this.container = container.intern();
        this.section = section.intern();
        this.id = id;
        this.lossTolerant = lossTolerant;

        hashCode = 17 + 19 * id.hashCode() + 19 * container.hashCode() + 19 * section.hashCode();
    }

    @Override
    public boolean isLossTolerant() {
        return lossTolerant;
    }

    /**
     * @return the unique identifier for this claim
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * @return the container identifier in which this claim is held
     */
    @Override
    public String getContainer() {
        return container;
    }

    /**
     * @return the section within a given container the claim is held
     */
    @Override
    public String getSection() {
        return section;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (other == null) {
            return false;
        }
        if (hashCode != other.hashCode()) {
            // We check hash code before instanceof because instanceof is fairly expensive and for
            // StandardResourceClaim, calling hashCode() simply returns a pre-calculated value.
            return false;
        }

        if (!(other instanceof ResourceClaim)) {
            return false;
        }
        final ResourceClaim otherClaim = (ResourceClaim) other;
        return id.equals(otherClaim.getId()) && container.equals(otherClaim.getContainer()) && section.equals(otherClaim.getSection());
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "StandardResourceClaim[id=" + id + ", container=" + container + ", section=" + section + "]";
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    /**
     * Freeze the Resource Claim so that it can now longer be written to
     */
    void freeze() {
        this.writable = false;
    }

    @Override
    public boolean isInUse() {
        // Note that it is critical here that we always check isWritable() BEFORE checking
        // the claimant count. This is due to the fact that if the claim is in fact writable, the claimant count
        // could increase. So if we first check claimant count and that is 0, and then we check isWritable, it may be
        // that the claimant count has changed to 1 before checking isWritable.
        // However, if isWritable() is false, then the only way that the claimant count can increase is if a FlowFile referencing
        // the Resource Claim is cloned. In this case, though, the claimant count has not become 0.
        // Said another way, if isWritable() == false, then the claimant count can never increase from 0.
        return isWritable() || claimManager.getClaimantCount(this) > 0;
    }
}
