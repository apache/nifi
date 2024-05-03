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
package org.apache.nifi.processors.gcp.drive;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public interface OutputChecker {
    TestRunner getTestRunner();

    default void checkAttributes(Relationship relationship, Set<Map<String, String>> expectedAttributes) {
        getTestRunner().assertTransferCount(relationship, expectedAttributes.size());
        List<MockFlowFile> flowFiles = getTestRunner().getFlowFilesForRelationship(relationship);

        Set<String> checkedAttributeNames = getCheckedAttributeNames();

        Set<Map<String, String>> actualAttributes = flowFiles.stream()
                .map(flowFile -> flowFile.getAttributes().entrySet().stream()
                        .filter(attributeNameAndValue -> checkedAttributeNames.contains(attributeNameAndValue.getKey()))
                        .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))

                )
                .collect(Collectors.toSet());

        assertEquals(expectedAttributes, actualAttributes);
    }

    default Set<String> getCheckedAttributeNames() {
        Set<String> checkedAttributeNames = Arrays.stream(GoogleDriveFlowFileAttribute.values())
                .map(GoogleDriveFlowFileAttribute::getName)
                .collect(Collectors.toSet());

        return checkedAttributeNames;
    }

    default void checkContent(Relationship relationship, List<String> expectedContent) {
        getTestRunner().assertTransferCount(relationship, expectedContent.size());
        List<MockFlowFile> flowFiles = getTestRunner().getFlowFilesForRelationship(relationship);

        List<String> actualContent = flowFiles.stream()
                .map(flowFile -> flowFile.getContent())
                .collect(Collectors.toList());

        assertEquals(expectedContent, actualContent);
    }
}
