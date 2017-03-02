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

import java.util.regex.Pattern;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

/**
 * This prioritizer checks each FlowFile for a "priority" attribute and lets
 * that attribute determine the priority.
 *
 * 1. if neither FlowFile has a "priority" attribute then order will be
 * FirstInFirstOut 2. if one FlowFile has a "priority" attribute and the other
 * does not, then the one with the attribute wins 3. if one or both "priority"
 * attributes is an integer, then the lowest number wins 4. the "priority"
 * attributes are compared lexicographically and the lowest wins
 */
public class PriorityAttributePrioritizer implements FlowFilePrioritizer {

    private static final Pattern intPattern = Pattern.compile("-?\\d+");

    @Override
    public int compare(FlowFile o1, FlowFile o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o2 == null) {
            return -1;
        } else if (o1 == null) {
            return 1;
        }

        String o1Priority = o1.getAttribute(CoreAttributes.PRIORITY.key());
        String o2Priority = o2.getAttribute(CoreAttributes.PRIORITY.key());
        if (o1Priority == null && o2Priority == null) {
            return -1; // this is not 0 to match FirstInFirstOut
        } else if (o2Priority == null) {
            return -1;
        } else if (o1Priority == null) {
            return 1;
        }

        // priority exists on both FlowFiles
        if (intPattern.matcher(o1Priority.trim()).matches()) {
            if (intPattern.matcher(o2Priority.trim()).matches()) {
                try {
                    // both o1Priority and o2Priority are numbers
                    long o1num = Long.parseLong(o1Priority.trim());
                    long o2num = Long.parseLong(o2Priority.trim());
                    return o1num < o2num ? -1 : (o1num > o2num ? 1 : 0);
                } catch (NumberFormatException e) {
                    // not a long after regex matched
                    return 0;
                }
            } else {
                // o1Priority is a number, o2Priority is not, o1 wins
                return -1;
            }
        } else {
            if (intPattern.matcher(o2Priority.trim()).matches()) {
                // o2Priority is a number, o1Priority is not, o2 wins
                return 1;
            } else {
                // neither o1Priority nor o2Priority are numbers
                return o1Priority.compareTo(o2Priority);
            }
        }
    }

}
