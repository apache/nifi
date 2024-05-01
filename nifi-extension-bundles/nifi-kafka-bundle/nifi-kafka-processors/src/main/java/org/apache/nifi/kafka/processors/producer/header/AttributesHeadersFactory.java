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
package org.apache.nifi.kafka.processors.producer.header;

import org.apache.nifi.kafka.service.api.header.RecordHeader;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Derive Kafka headers from NiFi FlowFile attributes.
 */
public class AttributesHeadersFactory implements HeadersFactory {
    private final Pattern attributeNameRegex;
    private final Charset messageHeaderCharset;

    public AttributesHeadersFactory(final Pattern attributeNameRegex, final Charset messageHeaderCharset) {
        this.attributeNameRegex = attributeNameRegex;
        this.messageHeaderCharset = messageHeaderCharset;
    }

    public List<RecordHeader> getHeaders(final Map<String, String> attributes) throws IOException {
        final List<RecordHeader> headers = new ArrayList<>();
        if (attributeNameRegex != null) {
            for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                if (attributeNameRegex.matcher(entry.getKey()).matches()) {
                    headers.add(new RecordHeader(entry.getKey(), entry.getValue().getBytes(messageHeaderCharset)));
                }
            }
        }
        return headers;
    }
}
