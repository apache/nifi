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
package org.apache.nifi.prioritizer;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import java.util.Comparator;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * This prioritizer checks each FlowFile for a "priority" attribute and lets that attribute determine the priority.
 * <p>
 * 1. If one FlowFile has a "priority" attribute and the other does not, then the one with the attribute wins.
 * 2. If only one's FlowFile "priority" attribute is an integer, then that FlowFile wins.
 * 3. If the "priority" attributes of both are an integer, then the FlowFile with the lowest number wins.
 * 4. If the "priority" attributes of both are not an integer, they're compared lexicographically and the lowest wins.
 */
public class PriorityAttributePrioritizer implements FlowFilePrioritizer {

    private static final Predicate<String> isInteger = Pattern.compile("-?\\d+").asMatchPredicate();

    private static final Comparator<String> priorityAttributeComparator = Comparator.nullsLast(
            Comparator.comparing(
                    PriorityAttributePrioritizer::parseLongOrNull,
                    Comparator.nullsLast(Long::compare)
            ).thenComparing(Comparator.naturalOrder())
    );

    private static final Comparator<FlowFile> composedComparator = Comparator.nullsLast(
            Comparator.comparing(
                    flowFile -> flowFile.getAttribute(CoreAttributes.PRIORITY.key()),
                    priorityAttributeComparator
            )
    );

    private static Long parseLongOrNull(String attribute) {
        final String trimmedAttribute = attribute.trim();

        if (isInteger.test(trimmedAttribute)) {
            try {
                return Long.parseLong(trimmedAttribute);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    @Override
    public int compare(FlowFile o1, FlowFile o2) {
        return composedComparator.compare(o1, o2);
    }
}
