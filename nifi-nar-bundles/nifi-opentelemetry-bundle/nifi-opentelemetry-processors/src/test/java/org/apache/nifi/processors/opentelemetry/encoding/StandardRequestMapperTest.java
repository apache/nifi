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
package org.apache.nifi.processors.opentelemetry.encoding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandardRequestMapperTest {

    private static final byte[] SPAN_ID = new byte[]{-18, -31, -101, 126, -61, -63, -79, 116};

    private static final String SPAN_ID_ENCODED = "EEE19B7EC3C1B174";

    private static final byte[] TRACE_ID = new byte[]{91, -114, -1, -9, -104, 3, -127, 3, -46, 105, -74, 51, -127, 63, -58, 12};

    private static final String TRACE_ID_ENCODED = "5B8EFFF798038103D269B633813FC60C";

    private static final byte[] PARENT_SPAN_ID = new byte[]{-18, -31, -101, 126, -61, -63, -79, 115};

    private static final String PARENT_SPAN_ID_ENCODED = "EEE19B7EC3C1B173";

    private static final Span.SpanKind SPAN_KIND = Span.SpanKind.SPAN_KIND_SERVER;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final StandardRequestMapper requestMapper = new StandardRequestMapper();

    @Test
    void testWriteValueTraces() throws IOException {
        final ExportTraceServiceRequest request = getTraceRequest();
        final byte[] serialized = getSerialized(request);

        final JsonNode rootNode = objectMapper.readTree(serialized);
        final JsonNode resourceSpansNode = rootNode.get("resourceSpans");
        assertNotNull(resourceSpansNode);
        final JsonNode firstResourceSpan = resourceSpansNode.get(0);
        assertNotNull(firstResourceSpan);
        final JsonNode scopeSpansNode = firstResourceSpan.get("scopeSpans");
        assertNotNull(scopeSpansNode);
        final JsonNode firstScopeSpan = scopeSpansNode.get(0);
        assertNotNull(firstScopeSpan);
        final JsonNode spansNode = firstScopeSpan.get("spans");
        assertNotNull(spansNode);

        final JsonNode firstSpan = spansNode.get(0);
        final String firstSpanId = firstSpan.get("spanId").asText();
        assertEquals(SPAN_ID_ENCODED, firstSpanId);

        final String firstTraceId = firstSpan.get("traceId").asText();
        assertEquals(TRACE_ID_ENCODED, firstTraceId);

        final String firstParentSpanId = firstSpan.get("parentSpanId").asText();
        assertEquals(PARENT_SPAN_ID_ENCODED, firstParentSpanId);

        final int firstSpanKind = firstSpan.get("kind").asInt();
        assertEquals(SPAN_KIND.ordinal(), firstSpanKind);
    }

    @Test
    void testReadValueTraces() throws IOException {
        final ExportTraceServiceRequest request = getTraceRequest();
        final byte[] serialized = getSerialized(request);
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized);

        final ExportTraceServiceRequest deserialized = requestMapper.readValue(inputStream, ExportTraceServiceRequest.class);

        assertNotNull(deserialized);
        final ResourceSpans resourceSpans = deserialized.getResourceSpans(0);
        final ScopeSpans scopeSpans = resourceSpans.getScopeSpans(0);
        final Span span = scopeSpans.getSpans(0);

        assertArrayEquals(SPAN_ID, span.getSpanId().toByteArray());
        assertArrayEquals(TRACE_ID, span.getTraceId().toByteArray());
        assertArrayEquals(PARENT_SPAN_ID, span.getParentSpanId().toByteArray());
    }

    private byte[] getSerialized(final Message message) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        requestMapper.writeValue(outputStream, message);
        return outputStream.toByteArray();
    }

    private ExportTraceServiceRequest getTraceRequest() {
        final Span span = Span.newBuilder()
                .setSpanId(ByteString.copyFrom(SPAN_ID))
                .setTraceId(ByteString.copyFrom(TRACE_ID))
                .setParentSpanId(ByteString.copyFrom(PARENT_SPAN_ID))
                .setKind(SPAN_KIND)
                .build();
        final ScopeSpans scopeSpans = ScopeSpans.newBuilder().addSpans(span).build();
        final ResourceSpans resourceSpans = ResourceSpans.newBuilder().addScopeSpans(scopeSpans).build();
        return ExportTraceServiceRequest.newBuilder().addResourceSpans(resourceSpans).build();
    }
}
