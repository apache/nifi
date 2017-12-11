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

package org.apache.nifi.processors.standard.merge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

public class KeepUniqueAttributeStrategy implements AttributeStrategy {

    @Override
    public Map<String, String> getMergedAttributes(final List<FlowFile> flowFiles) {
        final Map<String, String> newAttributes = new HashMap<>();
        final Set<String> conflicting = new HashSet<>();

        for (final FlowFile flowFile : flowFiles) {
            for (final Map.Entry<String, String> attributeEntry : flowFile.getAttributes().entrySet()) {
                final String name = attributeEntry.getKey();
                final String value = attributeEntry.getValue();

                final String existingValue = newAttributes.get(name);
                if (existingValue != null && !existingValue.equals(value)) {
                    conflicting.add(name);
                } else {
                    newAttributes.put(name, value);
                }
            }
        }

        for (final String attributeToRemove : conflicting) {
            newAttributes.remove(attributeToRemove);
        }

        // Never copy the UUID from the parents - which could happen if we don't remove it and there is only 1 parent.
        newAttributes.remove(CoreAttributes.UUID.key());
        return newAttributes;
    }
}