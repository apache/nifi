/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.network.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.processors.network.parser.util.TemplateJSON;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class Netflowv5Template {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final List<TemplateJSON> headerList;
    private static final List<TemplateJSON> recordList;
    private static final ObjectNode template;
    public static final int templateID = 5;

    public static final ObjectNode getTemplate() {
        return template;
    }

    static {
        headerList = new ArrayList<>();
        recordList = new ArrayList<>();
        template = mapper.createObjectNode();

        // Header
        headerList.add(new TemplateJSON("version", "int", 0, 2, "", 1));
        headerList.add(new TemplateJSON("count", "int", 2, 2, "", 2));
        headerList.add(new TemplateJSON("sys_uptime", "long", 4, 4, "", 3));
        headerList.add(new TemplateJSON("unix_secs", "long", 8, 4, "", 4));
        headerList.add(new TemplateJSON("unix_nsecs", "long", 12, 4, "", 5));
        headerList.add(new TemplateJSON("flow_sequence", "long", 16, 4, "", 6));
        headerList.add(new TemplateJSON("engine_type", "short", 20, 1, "", 7));
        headerList.add(new TemplateJSON("engine_id", "short", 21, 1, "", 8));
        headerList.add(new TemplateJSON("sampling_interval", "int", 22, 2, "", 9));
        // Record
        recordList.add(new TemplateJSON("srcaddr", "long", 0, 4, "", 1));
        recordList.add(new TemplateJSON("dstaddr", "long", 4, 8, "", 2));
        recordList.add(new TemplateJSON("nexthop", "long", 8, 4, "", 3));
        recordList.add(new TemplateJSON("input", "int", 12, 2, "", 4));
        recordList.add(new TemplateJSON("output", "int", 14, 2, "", 5));
        recordList.add(new TemplateJSON("dPkts", "long", 16, 4, "", 6));
        recordList.add(new TemplateJSON("dOctets", "long", 20, 4, "", 7));
        recordList.add(new TemplateJSON("first", "long", 24, 4, "", 8));
        recordList.add(new TemplateJSON("last", "long", 28, 4, "", 9));
        recordList.add(new TemplateJSON("srcport", "int", 32, 2, "", 10));
        recordList.add(new TemplateJSON("dstport", "int", 34, 2, "", 11));
        recordList.add(new TemplateJSON("pad1", "short", 36, 1, "", 12));
        recordList.add(new TemplateJSON("tcp_flags", "short", 37, 1, "", 13));
        recordList.add(new TemplateJSON("prot", "short", 38, 1, "", 14));
        recordList.add(new TemplateJSON("tos", "short", 39, 1, "", 15));
        recordList.add(new TemplateJSON("src_as", "int", 40, 2, "", 16));
        recordList.add(new TemplateJSON("dst_as", "int", 42, 2, "", 17));
        recordList.add(new TemplateJSON("src_mask", "short", 44, 1, "", 18));
        recordList.add(new TemplateJSON("dst_mask", "short", 45, 1, "", 19));
        recordList.add(new TemplateJSON("pad2", "int", 46, 0, "", 20));
        try {
            template.set("Template", generateSnapshot());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public final static ObjectNode generateSnapshot() throws JsonProcessingException {
        final ObjectNode entries = mapper.createObjectNode();
        entries.set("id", mapper.valueToTree(templateID));
        entries.set("fields", mapper.valueToTree(generateV5Template()));
        return entries;
    }

    private final static ObjectNode generateV5Template() throws JsonProcessingException {
        final ObjectNode fields = mapper.createObjectNode();
        fields.set("header", mapper.valueToTree(headerList));
        fields.set("record", mapper.valueToTree(recordList));
        return fields;
    }
}
