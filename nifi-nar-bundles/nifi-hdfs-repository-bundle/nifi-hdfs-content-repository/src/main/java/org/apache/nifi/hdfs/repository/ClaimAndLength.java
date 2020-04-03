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
package org.apache.nifi.hdfs.repository;

import org.apache.nifi.controller.repository.claim.ResourceClaim;

public class ClaimAndLength {

    private final ResourceClaim claim;
    private final long length;

    public ClaimAndLength(ResourceClaim claim, long length) {
        this.claim = claim;
        this.length = length;
    }

    public ResourceClaim getClaim() {
        return claim;
    }
    public long getLength() {
        return length;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (claim == null ? 0 : claim.hashCode());
        return result;
    }

    /**
     * Equality is determined purely by the ResourceClaim's equality
     *
     * @param obj the object to compare against
     * @return -1, 0, or +1 according to the contract of Object.equals
     */
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

        final ClaimAndLength other = (ClaimAndLength) obj;
        return claim.equals(other.getClaim());
    }

}
