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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * A ContentClaim is a reference to a given flow file's content. Multiple flow
 * files may reference the same content by both having the same content
 * claim.</p>
 *
 * <p>
 * Must be thread safe</p>
 *
 * @author none
 */
public final class StandardContentClaim implements ContentClaim, Comparable<ContentClaim> {

    private final String id;
    private final String container;
    private final String section;
    private final boolean lossTolerant;
    private final AtomicInteger claimantCount = new AtomicInteger(0);
    private final int hashCode;

    /**
     * Constructs a content claim
     *
     * @param container
     * @param section
     * @param id
     * @param lossTolerant
     */
    StandardContentClaim(final String container, final String section, final String id, final boolean lossTolerant) {
        this.container = container.intern();
        this.section = section.intern();
        this.id = id;
        this.lossTolerant = lossTolerant;

        hashCode = (int) (17 + 19 * (id.hashCode()) + 19 * container.hashCode() + 19 * section.hashCode());
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

    int getClaimantCount() {
        return claimantCount.get();
    }

    int decrementClaimantCount() {
        return claimantCount.decrementAndGet();
    }

    int incrementClaimantCount() {
        return claimantCount.incrementAndGet();
    }

    /**
     * Provides the natural ordering for ContentClaim objects. By default they
     * are sorted by their id, then container, then section
     *
     * @param other
     * @return x such that x <=1 if this is less than other;
     * x=0 if this.equals(other);
     * x >= 1 if this is greater than other
     */
    @Override
    public int compareTo(final ContentClaim other) {
        final int idComparison = id.compareTo(other.getId());
        if (idComparison != 0) {
            return idComparison;
        }

        final int containerComparison = container.compareTo(other.getContainer());
        if (containerComparison != 0) {
            return containerComparison;
        }

        return section.compareTo(other.getSection());
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
            // StandardContentClaim, calling hashCode() simply returns a pre-calculated value.
            return false;
        }

        if (!(other instanceof ContentClaim)) {
            return false;
        }
        final ContentClaim otherClaim = (ContentClaim) other;
        return id.equals(otherClaim.getId()) && container.equals(otherClaim.getContainer()) && section.equals(otherClaim.getSection());
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "ContentClaim[id=" + id + ", container=" + container + ", section=" + section + "]";
    }
}
