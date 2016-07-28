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

/**
 * <p>
 * A reference to a section of a {@link ResourceClaim}, which may or may not encompass
 * the entire ResourceClaim. Multiple FlowFiles may reference the same content by both
 * having the same content claim.
 * </p>
 *
 * <p>
 * Must be thread safe
 * </p>
 */
public interface ContentClaim extends Comparable<ContentClaim> {

    /**
     * @return the ResourceClaim that this ContentClaim references
     */
    ResourceClaim getResourceClaim();

    /**
     * @return the offset into the ResourceClaim where the content for this
     * claim begins
     */
    long getOffset();

    /**
     * @return the length of this ContentClaim
     */
    long getLength();
}
