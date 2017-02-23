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
package org.apache.nifi.flowfile.attributes;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.util.HashMap;
import java.util.Map;

/**
 * This enum class contains flow file attribute keys commonly used among Split processors.
 */
public enum FragmentAttributes implements FlowFileAttributeKey {

    /**
     * The number of bytes from the original FlowFile that were copied to this FlowFile,
     * including header, if applicable, which is duplicated in each split FlowFile.
     */
    FRAGMENT_SIZE("fragment.size"),
    /**
     * All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute.
     */
    FRAGMENT_ID("fragment.identifier"),
    /**
     * A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile.
     */
    FRAGMENT_INDEX("fragment.index"),
    /**
     * The number of split FlowFiles generated from the parent FlowFile.
     */
    FRAGMENT_COUNT("fragment.count"),
    /**
     * The filename of the parent FlowFile.
     */
    SEGMENT_ORIGINAL_FILENAME("segment.original.filename");

    private final String key;

    FragmentAttributes(final String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }

    public static FlowFile copyAttributesToOriginal(final ProcessSession processSession, final FlowFile originalFlowFile,
                                                    final String fragmentId, final int fragmentCount) {
        final Map<String, String> attributesToOriginal = new HashMap<>();
        if (fragmentId != null && fragmentId.length() > 0) {
            attributesToOriginal.put(FRAGMENT_ID.key(), fragmentId);
        }
        attributesToOriginal.put(FRAGMENT_COUNT.key(), String.valueOf(fragmentCount));
        return processSession.putAllAttributes(originalFlowFile, attributesToOriginal);
    }

}
