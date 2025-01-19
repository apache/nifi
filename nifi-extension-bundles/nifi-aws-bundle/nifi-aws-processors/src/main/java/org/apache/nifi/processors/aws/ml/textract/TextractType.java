/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.aws.ml.textract;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public enum TextractType {
    DOCUMENT_ANALYSIS("Document Analysis"),
    DOCUMENT_TEXT_DETECTION("Document Text Detection"),
    EXPENSE_ANALYSIS("Expense Analysis");

    public static final Set<String> TEXTRACT_TYPES = Arrays.stream(values()).map(TextractType::getType)
            .collect(collectingAndThen(toSet(), Collections::unmodifiableSet));

    public final String type;

    TextractType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static TextractType fromString(String value) {
        return Arrays.stream(values())
                .filter(type -> type.getType().equalsIgnoreCase(value))
                .findAny()
                .orElseThrow(() -> new UnsupportedOperationException("Unsupported textract type."));
    }
}
