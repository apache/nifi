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
package org.apache.nifi.registry.service.extension.docs;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.registry.db.entity.ExtensionEntity;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.serialization.ExtensionSerializer;
import org.apache.nifi.registry.serialization.Serializer;
import org.apache.nifi.registry.serialization.jackson.ObjectMapperProvider;
import org.apache.nifi.registry.service.mapper.ExtensionMappings;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;

public class TestHtmlExtensionDocWriter {

    private ExtensionDocWriter docWriter;
    private Serializer<Extension> extensionSerializer;

    @Before
    public void setup() {
        docWriter = new HtmlExtensionDocWriter();
        extensionSerializer = new ExtensionSerializer();
    }

    @Test
    public void testWriteDocsForConsumeKafkaRecord() throws IOException {
        final File rawExtensionJson = new File("src/test/resources/extensions/ConsumeKafkaRecord_1_0.json");
        final String serializedExtension = getSerializedExtension(rawExtensionJson);

        final ExtensionEntity entity = new ExtensionEntity();
        entity.setContent(serializedExtension);
        entity.setBucketId(UUID.randomUUID().toString());
        entity.setBucketName("My Bucket");
        entity.setGroupId("org.apache.nifi");
        entity.setArtifactId("nifi-kakfa-bundle");
        entity.setVersion("1.9.1");
        entity.setSystemApiVersion("1.9.1");
        entity.setBundleId(UUID.randomUUID().toString());
        entity.setBundleType(BundleType.NIFI_NAR);
        entity.setDisplayName("ConsumeKafkaRecord_1_0");

        final ExtensionMetadata metadata = ExtensionMappings.mapToMetadata(entity, extensionSerializer);
        assertNotNull(entity);

        final Extension extension = ExtensionMappings.map(entity, extensionSerializer);
        assertNotNull(extension);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        docWriter.write(metadata, extension, out);

        final String docsResult = new String(out.toByteArray(), StandardCharsets.UTF_8);
        assertNotNull(docsResult);

        XmlValidator.assertXmlValid(docsResult);
        XmlValidator.assertContains(docsResult, entity.getDisplayName());
    }

    private String getSerializedExtension(final File rawExtensionJson) throws IOException {
        final ByteArrayOutputStream serializedExtension = new ByteArrayOutputStream();
        try (final InputStream inputStream = new FileInputStream(rawExtensionJson)) {
            final String rawJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            final Extension tempExtension = ObjectMapperProvider.getMapper().readValue(rawJson, Extension.class);
            extensionSerializer.serialize(tempExtension, serializedExtension);
        }

        return serializedExtension.toString("UTF-8");
    }
}
