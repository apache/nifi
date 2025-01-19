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

package org.apache.nifi.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.GeoEnrichTestUtils.getFullCityResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGeoEnrichIPRecord {

    private TestRunner runner;
    private DatabaseReader reader;

    @BeforeEach
    public void setup() throws Exception {
        reader = mock(DatabaseReader.class);
        final CityResponse cityResponse = getFullCityResponse();
        when(reader.city(InetAddress.getByName("1.2.3.4"))).thenReturn(cityResponse);
        runner = TestRunners.newTestRunner(new TestableGeoEnrichIPRecord());
        ControllerService reader = new JsonTreeReader();
        ControllerService writer = new JsonRecordSetWriter();
        ControllerService registry = new MockSchemaRegistry();
        runner.addControllerService("reader", reader);
        runner.addControllerService("writer", writer);
        runner.addControllerService("registry", registry);


        try (InputStream is = getClass().getResourceAsStream("/avro/record_schema.avsc")) {
            String raw = IOUtils.toString(is, StandardCharsets.UTF_8);
            RecordSchema parsed = AvroTypeUtil.createSchema(new Schema.Parser().parse(raw));
            ((MockSchemaRegistry) registry).addSchema("record", parsed);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.setProperty(GeoEnrichIPRecord.IP_RECORD_PATH, "/ip_address");
        runner.setProperty(GeoEnrichIPRecord.READER, "reader");
        runner.setProperty(GeoEnrichIPRecord.WRITER, "writer");
        runner.enableControllerService(registry);
        runner.enableControllerService(reader);
        runner.enableControllerService(writer);

        runner.setProperty(GeoEnrichIPRecord.GEO_CITY, "/geo/city");
        runner.setProperty(GeoEnrichIPRecord.GEO_COUNTRY, "/geo/country");
        runner.setProperty(GeoEnrichIPRecord.GEO_COUNTRY_ISO, "/geo/country_iso");
        runner.setProperty(GeoEnrichIPRecord.GEO_POSTAL_CODE, "/geo/country_postal");
        runner.setProperty(GeoEnrichIPRecord.GEO_LATITUDE, "/geo/lat");
        runner.setProperty(GeoEnrichIPRecord.GEO_LONGITUDE, "/geo/lon");
        runner.setProperty(AbstractEnrichIP.LOG_LEVEL, "WARN");
        runner.assertValid();
    }

    private void commonTest(String path, int not, int found, int original) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "record");
        runner.enqueue(getClass().getResourceAsStream(path), attrs);
        runner.run();

        runner.assertTransferCount(GeoEnrichIPRecord.REL_NOT_FOUND, not);
        runner.assertTransferCount(GeoEnrichIPRecord.REL_FOUND, found);
        runner.assertTransferCount(GeoEnrichIPRecord.REL_ORIGINAL, original);
    }

    @Test
    public void testSplitOutput() {
        runner.setProperty(GeoEnrichIPRecord.SPLIT_FOUND_NOT_FOUND, "true");
        commonTest("/json/two_records_for_split.json", 1, 1, 1);
    }

    @Test
    public void testEnrichSendToNotFound() {
        commonTest("/json/one_record_no_geo.json", 1, 0, 0);
    }

    @Test
    public void testEnrichSendToFound() throws Exception {
        commonTest("/json/one_record.json", 0, 1, 0);

        MockFlowFile ff = runner.getFlowFilesForRelationship(GeoEnrichIPRecord.REL_FOUND).getFirst();
        byte[] raw = runner.getContentAsByteArray(ff);
        String content = new String(raw);
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> result = mapper.readValue(content, List.class);

        assertNotNull(result);
        assertEquals(1, result.size());

        Map<String, Object> element = result.getFirst();
        Map<String, Object> geo = (Map<String, Object>) element.get("geo");

        assertNotNull(geo);
        assertNotNull(geo.get("city"));
        assertNotNull(geo.get("country"));
        assertNotNull(geo.get("country_iso"));
        assertNotNull(geo.get("country_postal"));
        assertNotNull(geo.get("lat"));
        assertNotNull(geo.get("lon"));
    }

    class TestableGeoEnrichIPRecord extends GeoEnrichIPRecord {
        TestableGeoEnrichIPRecord() { }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return List.of(
                    READER,
                    WRITER,
                    IP_RECORD_PATH,
                    SPLIT_FOUND_NOT_FOUND,
                    GEO_CITY,
                    GEO_LATITUDE,
                    GEO_LONGITUDE,
                    GEO_COUNTRY,
                    GEO_COUNTRY_ISO,
                    GEO_POSTAL_CODE,
                    LOG_LEVEL
            );
        }
        @Override
        @OnScheduled
        public void onScheduled(ProcessContext context) {
            databaseReaderRef.set(reader);
            readerFactory = context.getProperty(READER).asControllerService(RecordReaderFactory.class);
            writerFactory = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
            splitOutput = context.getProperty(SPLIT_FOUND_NOT_FOUND).asBoolean();
        }
        @Override
        protected void loadDatabaseFile() {
            //  Do nothing, the mock database reader is used
        }
    }
}
