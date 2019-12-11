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
package org.apache.nifi.provenance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to capture the method used to acquire FlowFiles
 */
public enum FlowFileAcquisitionMethod {
    /**
     * FlowFile was created by passively listening for input from an external source. Examples may include subscribing to a message queue or listening for HTTP POSTs
     */
    PASSIVE_RECEIVE,

    /**
     * FlowFile was created by actively querying an external source where some explicit selection criteria are used (e.g. not full-take).
     * Examples may include querying databases
     */
    ACTIVE_QUERY,

    /**
     * FlowFile was created by actively polling an external source for data. Examples may include HTTP GETs or FTP pulls
     */
    ACTIVE_PULL,

    /**
     * The method of acquisition isn't known
     */
    UNSPECIFIED;

    private static final List<String> METHOD_NAMES;
    private static final Map<FlowFileAcquisitionMethod, Integer> METHOD_TO_IDENTIFIER;

    static {
        METHOD_NAMES = new ArrayList<>();
        METHOD_TO_IDENTIFIER = new HashMap<>();

        int count = 0;
        for (final FlowFileAcquisitionMethod method : values()) {
            METHOD_NAMES.add(method.name());
            METHOD_TO_IDENTIFIER.put(method, count++);
        }
    }

    /**
     * This is used in the serialization of provenance events containing this enumeration to be tolerant of ordinal changes
     *
     * @return an unmodifiable collection of all of the flowfile acquisition names
     */
    public static List<String> flowFileAcquisitionMethodNames() {
        return Collections.unmodifiableList(METHOD_NAMES);
    }

    /**
     * This is used in the serialization of provenance events containing this enumeration to be tolerant of ordinal changes
     *
     * @return a map of flowfile acquisition name to their respective ordinals
     */
    public static Map<FlowFileAcquisitionMethod, Integer> flowFileAcquisitionMap() {
        return Collections.unmodifiableMap(METHOD_TO_IDENTIFIER);
    }
}
