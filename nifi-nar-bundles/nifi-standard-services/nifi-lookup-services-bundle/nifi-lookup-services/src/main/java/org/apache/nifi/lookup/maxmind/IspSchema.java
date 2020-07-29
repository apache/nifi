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

package org.apache.nifi.lookup.maxmind;

import java.util.Arrays;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class IspSchema {
    static final RecordField NAME = new RecordField("name", RecordFieldType.STRING.getDataType());
    static final RecordField ORG = new RecordField("organization", RecordFieldType.STRING.getDataType());
    static final RecordField ASN = new RecordField("asn", RecordFieldType.INT.getDataType());
    static final RecordField ASN_ORG = new RecordField("asnOrganization", RecordFieldType.STRING.getDataType());

    static final RecordSchema ISP_SCHEMA = new SimpleRecordSchema(Arrays.asList(NAME, ORG, ASN, ASN_ORG));
}
