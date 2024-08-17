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
package org.apache.nifi.web.api.streaming;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * Range of bytes requested as described in RFC 9110 Section 14.1.2 with optional first and last positions
 */
public class ByteRange {
    private final Long firstPosition;

    private final Long lastPosition;

    public ByteRange(final Long firstPosition, final Long lastPosition) {
        if (firstPosition == null) {
            Objects.requireNonNull(lastPosition, "Last Position required");
        }
        this.firstPosition = firstPosition;
        this.lastPosition = lastPosition;
    }

    /**
     * Get first position in byte range which can be empty indicating the last position must be specified
     *
     * @return First position starting with 0 or empty
     */
    public OptionalLong getFirstPosition() {
        return firstPosition == null ? OptionalLong.empty() : OptionalLong.of(firstPosition);
    }

    /**
     * Get last position in byte range which can empty indicating the first position must be specified
     *
     * @return Last position starting with 0 or empty
     */
    public OptionalLong getLastPosition() {
        return lastPosition == null ? OptionalLong.empty() : OptionalLong.of(lastPosition);
    }
}
