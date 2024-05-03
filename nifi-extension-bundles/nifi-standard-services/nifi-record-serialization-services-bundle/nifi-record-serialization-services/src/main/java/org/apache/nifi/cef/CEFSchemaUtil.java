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
package org.apache.nifi.cef;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class CEFSchemaUtil {

    private static final List<RecordField> HEADER_FIELDS = new ArrayList<>();

    static {
        // Reference states that severity might be represented by integer values (0-10) and string values (like High) too
        HEADER_FIELDS.add(new RecordField("version", RecordFieldType.INT.getDataType()));
        HEADER_FIELDS.add(new RecordField("deviceVendor", RecordFieldType.STRING.getDataType()));
        HEADER_FIELDS.add(new RecordField("deviceProduct", RecordFieldType.STRING.getDataType()));
        HEADER_FIELDS.add(new RecordField("deviceVersion", RecordFieldType.STRING.getDataType()));
        HEADER_FIELDS.add(new RecordField("deviceEventClassId", RecordFieldType.STRING.getDataType()));
        HEADER_FIELDS.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        HEADER_FIELDS.add(new RecordField("severity", RecordFieldType.STRING.getDataType()));
    }

    // Fields known by the CEF Extension Dictionary in version 23
    private static final Set<String> EXTENSIONS_STRING = new HashSet<>(Arrays.asList("act", "app", "c6a1Label", "c6a2Label", "c6a3Label",
            "c6a4Label", "cfp1Label", "cfp2Label", "cfp3Label", "cfp4Label", "cn1Label", "cn2Label", "cn3Label", "cs1", "cs1Label",
            "cs2", "cs2Label", "cs3", "cs3Label", "cs4", "cs4Label", "cs5", "cs5Label", "cs6", "cs6Label", "destinationDnsDomain",
            "destinationServiceName", "destinationTranslatedAddress", "destinationTranslatedPort", "deviceCustomDate1Label", "deviceCustomDate2Label",
            "deviceDnsDomain", "deviceExternalId", "deviceFacility", "deviceInboundInterface", "deviceNtDomain", "deviceOutboundInterface",
            "deviceProcessName", "dhost", "dntdom", "dpriv", "dproc", "dtz", "duid", "duser", "dvchost", "externalId", "fileHash", "field", "filePath",
            "filePermission", "fileType", "flexDate1Label", "flexNumber1Label", "flexNumber2Label", "flexString1", "flexString1Label",
            "flexString2", "flexString2Label", "fname", "msg", "oldFileHash", "oldField", "oldFileName", "oldFilePath", "oldFilePermission",
            "oldFileType", "outcome", "proto", "reason", "request", "requestClientApplication", "requestContext", "requestCookies", "requestMethod",
            "shost", "sntdom", "sourceDnsDomain", "sourceServiceName", "spriv", "sproc", "suid", "suser", "agentDnsDomain", "agentNtDomain",
            "agentTranslatedZoneExternalID", "agentTranslatedZoneURI", "ahost", "aid", "at", "atz", "av", "cat", "customerExternalID", "customerURI",
            "destinationTranslatedZoneExternalID", "destinationTranslatedZoneURI", "destinationZoneExternalID", "destinationZoneURI",
            "deviceTranslatedZoneExternalID", "deviceTranslatedZoneURI", "deviceZoneExternalID", "deviceZoneURI", "rawEvent", "sourceTranslatedZoneExternalID",
            "sourceTranslatedZoneURI", "sourceZoneExternalID", "sourceZoneURI"));
    private static final Set<String> EXTENSIONS_INTEGER = new HashSet<>(Arrays.asList("cnt", "deviceDirection", "dpid", "dpt", "dvcpid", "fsize", "in",
            "oldFileSize", "out", "sourceTranslatedPort", "spid", "spt", "type"));
    private static final Set<String> EXTENSIONS_LONG = new HashSet<>(Arrays.asList("cn1", "cn2", "cn3", "flexNumber1", "flexNumber2", "eventId"));
    private static final Set<String> EXTENSIONS_FLOAT = new HashSet<>(Arrays.asList("cfp1", "cfp2", "cfp3", "cfp4"));
    private static final Set<String> EXTENSIONS_DOUBLE = new HashSet<>(Arrays.asList("dlat", "dlong", "slat", "slong"));
    private static final Set<String> EXTENSIONS_INET_ADDRESS = new HashSet<>(Arrays.asList("agt"));
    private static final Set<String> EXTENSIONS_IPV4_ADDRESS = new HashSet<>(Arrays.asList("deviceTranslatedAddress", "dst", "dvc", "sourceTranslatedAddress", "src",
            "agentTranslatedAddress"));
    private static final Set<String> EXTENSIONS_IPV6_ADDRESS = new HashSet<>(Arrays.asList("c6a1", "c6a2", "c6a3", "c6a4"));
    private static final Set<String> EXTENSIONS_MAC_ADDRESS = new HashSet<>(Arrays.asList("dmac", "dvcmac", "smac", "amac"));
    private static final Set<String> EXTENSIONS_TIMESTAMP = new HashSet<>(Arrays.asList("deviceCustomDate1", "deviceCustomDate2", "end", "fileCreatedTime",
            "fileModificationTime", "flexDate1", "oldFileCreateTime", "oldFileModificationTime", "rt", "start", "art"));

    private static final Map<Set<String>, DataType> EXTENSION_TYPE_MAPPING = new HashMap<>();

    static {
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_STRING, RecordFieldType.STRING.getDataType());
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_INTEGER, RecordFieldType.INT.getDataType());
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_LONG, RecordFieldType.LONG.getDataType());
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_FLOAT, RecordFieldType.FLOAT.getDataType());
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_DOUBLE, RecordFieldType.DOUBLE.getDataType());
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_INET_ADDRESS, RecordFieldType.STRING.getDataType());
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_IPV4_ADDRESS, RecordFieldType.STRING.getDataType());
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_IPV6_ADDRESS, RecordFieldType.STRING.getDataType());
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_MAC_ADDRESS, RecordFieldType.STRING.getDataType());
        EXTENSION_TYPE_MAPPING.put(EXTENSIONS_TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType());
    }

    static List<RecordField> getHeaderFields() {
        return HEADER_FIELDS;
    }

    static Map<Set<String>, DataType> getExtensionTypeMapping() {
        return EXTENSION_TYPE_MAPPING;
    }

    private CEFSchemaUtil() {
        // Util class, not to be instantiated
    }
}
