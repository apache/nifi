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

public class ContainerSchema {

    static final RecordField GEO = new RecordField("geo", RecordFieldType.RECORD.getRecordDataType(CitySchema.GEO_SCHEMA));
    static final RecordField ISP = new RecordField("isp", RecordFieldType.RECORD.getRecordDataType(IspSchema.ISP_SCHEMA));
    static final RecordField DOMAIN_NAME = new RecordField("domainName", RecordFieldType.STRING.getDataType());
    static final RecordField CONNECTION_TYPE = new RecordField("connectionType", RecordFieldType.STRING.getDataType());
    static final RecordField ANONYMOUS_IP = new RecordField("anonymousIp", RecordFieldType.RECORD.getRecordDataType(AnonymousIpSchema.ANONYMOUS_IP_SCHEMA));

    static final RecordSchema CONTAINER_SCHEMA = new SimpleRecordSchema(Arrays.asList(GEO, ISP, DOMAIN_NAME, CONNECTION_TYPE, ANONYMOUS_IP));

}
