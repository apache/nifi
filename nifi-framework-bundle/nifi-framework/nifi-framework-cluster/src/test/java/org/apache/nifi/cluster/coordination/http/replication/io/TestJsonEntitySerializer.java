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

package org.apache.nifi.cluster.coordination.http.replication.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;

import com.fasterxml.jackson.databind.MapperFeature;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.util.TimeAdapter;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationModule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJsonEntitySerializer {

    @Test
    public void testSerializeProcessor() throws IOException {
        final ObjectMapper jsonCodec = new ObjectMapper();
        jsonCodec.registerModule(new JakartaXmlBindAnnotationModule());
        jsonCodec.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);

        // Test that we can properly serialize a ProcessorEntity because it has many nested levels, including a Map
        final JsonEntitySerializer serializer = new JsonEntitySerializer(jsonCodec);
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            final ProcessorConfigDTO configDto = new ProcessorConfigDTO();
            configDto.setProperties(Collections.singletonMap("key", "value"));
            final ProcessorDTO processorDto = new ProcessorDTO();
            processorDto.setConfig(configDto);

            final ProcessorEntity processor = new ProcessorEntity();
            processor.setId("123");
            processor.setComponent(processorDto);

            serializer.serialize(processor, baos);

            final String serialized = new String(baos.toByteArray(), StandardCharsets.UTF_8);
            assertEquals("{\"id\":\"123\",\"component\":{\"config\":{\"properties\":{\"key\":\"value\"}}}}", serialized);
        }
    }

    @Test
    public void testBulletinEntity() throws Exception {
        final ObjectMapper jsonCodec = new ObjectMapper();
        jsonCodec.registerModule(new JakartaXmlBindAnnotationModule());
        jsonCodec.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        jsonCodec.setConfig(jsonCodec.getSerializationConfig().with(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY));

        final Date timestamp = new Date();
        final TimeAdapter adapter = new TimeAdapter();
        final String formattedTimestamp = adapter.marshal(timestamp);

        // Test that we can properly serialize a Bulletin because it contains a timestmap,
        // which uses a JAXB annotation to specify how to marshal it.
        final JsonEntitySerializer serializer = new JsonEntitySerializer(jsonCodec);

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            final BulletinDTO bulletinDto = new BulletinDTO();
            bulletinDto.setCategory("test");
            bulletinDto.setLevel("INFO");
            bulletinDto.setTimestamp(timestamp);

            final BulletinEntity bulletin = new BulletinEntity();
            bulletin.setBulletin(bulletinDto);
            serializer.serialize(bulletin, baos);

            final String serialized = new String(baos.toByteArray(), StandardCharsets.UTF_8);
            assertEquals("{\"bulletin\":{\"category\":\"test\",\"level\":\"INFO\",\"timestamp\":\"" + formattedTimestamp + "\"}}", serialized);
        }
    }
}
