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
package org.apache.nifi.controller.repository;

import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;

/**
 * <code>FlowFileRecord</code> is a sub-interface of <code>FlowFile</code> and
 * is used to provide additional information about FlowFiles that provide
 * valuable information to the framework but should be hidden from components
 */
public interface FlowFileRecord extends FlowFile {

    /**
     * Returns the time (in millis since epoch) at which this FlowFile should no
     * longer be penalized.
     *
     * @return
     */
    long getPenaltyExpirationMillis();

    /**
     * Returns the {@link ContentClaim} that holds the FlowFile's content
     *
     * @return
     */
    ContentClaim getContentClaim();

    /**
     * Returns the byte offset into the {@link ContentClaim} at which the
     * FlowFile's content occurs. This mechanism allows multiple FlowFiles to
     * have the same ContentClaim, which can be significantly more efficient for
     * some implementations of
     * {@link nifi.controller.repository.ContentRepository ContentRepository}
     *
     * @return
     */
    long getContentClaimOffset();
}
