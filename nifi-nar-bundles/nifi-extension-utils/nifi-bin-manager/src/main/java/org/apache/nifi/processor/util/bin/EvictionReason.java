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

package org.apache.nifi.processor.util.bin;

public enum EvictionReason {
    MAX_BYTES_THRESHOLD_REACHED("Maximum number of bytes (Max Group Size) reached"),

    MAX_ENTRIES_THRESHOLD_REACHED("Maximum number of entries reached"),

    MIN_THRESHOLDS_REACHED("Minimum number of bytes (Min Group Size) and minimum number of entries reached"),

    TIMEOUT("Max Bin Age reached"),

    BIN_MANAGER_FULL("The oldest Bin was removed because incoming FlowFile could not be placed in an existing Bin, and the Maximum Number of Bins was reached"),

    UNSET("No reason was determined");

    private final String explanation;
    EvictionReason(final String explanation) {
        this.explanation = explanation;
    }

    public String getExplanation() {
        return explanation;
    }

    public String toString() {
        return explanation;
    }
}
