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

package org.apache.nifi.controller.repository.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.repository.schema.ComplexRecordField;
import org.apache.nifi.repository.schema.FieldType;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.Repetition;
import org.apache.nifi.repository.schema.SimpleRecordField;

public class ContentClaimSchema {

    // resource claim fields
    public static final String CLAIM_CONTAINER = "Container";
    public static final String CLAIM_SECTION = "Section";
    public static final String CLAIM_IDENTIFIER = "Identifier";
    public static final String LOSS_TOLERANT = "Loss Tolerant";
    public static final String RESOURCE_CLAIM = "Resource Claim";

    // content claim fields
    public static final String RESOURCE_CLAIM_OFFSET = "Resource Claim Offset"; // offset into resource claim where the content claim begins
    public static final String CONTENT_CLAIM_OFFSET = "Content Claim Offset"; // offset into the content claim where the flowfile begins
    public static final String CONTENT_CLAIM_LENGTH = "Content Claim Length";

    public static final RecordSchema CONTENT_CLAIM_SCHEMA_V1;
    public static final RecordSchema RESOURCE_CLAIM_SCHEMA_V1;

    static {
        final List<RecordField> resourceClaimFields = new ArrayList<>();
        resourceClaimFields.add(new SimpleRecordField(CLAIM_CONTAINER, FieldType.STRING, Repetition.EXACTLY_ONE));
        resourceClaimFields.add(new SimpleRecordField(CLAIM_SECTION, FieldType.STRING, Repetition.EXACTLY_ONE));
        resourceClaimFields.add(new SimpleRecordField(CLAIM_IDENTIFIER, FieldType.STRING, Repetition.EXACTLY_ONE));
        resourceClaimFields.add(new SimpleRecordField(LOSS_TOLERANT, FieldType.BOOLEAN, Repetition.EXACTLY_ONE));
        RESOURCE_CLAIM_SCHEMA_V1 = new RecordSchema(Collections.unmodifiableList(resourceClaimFields));

        final List<RecordField> contentClaimFields = new ArrayList<>();
        contentClaimFields.add(new ComplexRecordField(RESOURCE_CLAIM, Repetition.EXACTLY_ONE, resourceClaimFields));
        contentClaimFields.add(new SimpleRecordField(RESOURCE_CLAIM_OFFSET, FieldType.LONG, Repetition.EXACTLY_ONE));
        contentClaimFields.add(new SimpleRecordField(CONTENT_CLAIM_OFFSET, FieldType.LONG, Repetition.EXACTLY_ONE));
        contentClaimFields.add(new SimpleRecordField(CONTENT_CLAIM_LENGTH, FieldType.LONG, Repetition.EXACTLY_ONE));
        CONTENT_CLAIM_SCHEMA_V1 = new RecordSchema(Collections.unmodifiableList(contentClaimFields));
    }
}
