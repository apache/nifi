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

package org.apache.nifi.controller.state;

import org.apache.nifi.web.api.dto.StateEntryDTO;

import java.text.Collator;
import java.util.Comparator;
import java.util.Locale;

public class SortedStateUtils {

    /**
     * The maximum number of state entries to return to a client
     */
    public static final int MAX_COMPONENT_STATE_ENTRIES = 500;

    /**
     * Gets a comparator for comparing state entry keys.
     *
     * @return comparator for comparing state entry keys
     */
    public static Comparator<String> getKeyComparator() {
        final Collator collator = Collator.getInstance(Locale.US);
        return new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return collator.compare(s1, s2);
            }
        };
    }

    /**
     * Gets a comparator for comparing state entry keys.
     *
     * @return comparator for comparing state entry keys
     */
    public static Comparator<StateEntryDTO> getEntryDtoComparator() {
        final Collator collator = Collator.getInstance(Locale.US);
        return new Comparator<StateEntryDTO>() {
            @Override
            public int compare(StateEntryDTO o1, StateEntryDTO o2) {
                return collator.compare(o1.getKey(), o2.getKey());
            }
        };
    }
}
