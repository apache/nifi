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

public class AnonymousIpSchema {
    static final RecordField ANONYMOUS = new RecordField("anonymous", RecordFieldType.BOOLEAN.getDataType());
    static final RecordField ANONYMOUS_VPN = new RecordField("anonymousVpn", RecordFieldType.BOOLEAN.getDataType());
    static final RecordField HOSTING_PROVIDER = new RecordField("hostingProvider", RecordFieldType.BOOLEAN.getDataType());
    static final RecordField PUBLIC_PROXY = new RecordField("publicProxy", RecordFieldType.BOOLEAN.getDataType());
    static final RecordField TOR_EXIT_NODE = new RecordField("torExitNode", RecordFieldType.BOOLEAN.getDataType());

    static final RecordSchema ANONYMOUS_IP_SCHEMA = new SimpleRecordSchema(Arrays.asList(ANONYMOUS, ANONYMOUS_VPN, HOSTING_PROVIDER, PUBLIC_PROXY, TOR_EXIT_NODE));
}
