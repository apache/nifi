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
package org.apache.nifi.registry.bundle.extract.nar.docs;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import org.apache.nifi.registry.bundle.extract.BundleException;
import org.apache.nifi.registry.extension.component.manifest.ExtensionManifest;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of {@link ExtensionManifestParser} that uses Jackson XML to unmarshall the extension-manifest.xml content.
 */
public class JacksonExtensionManifestParser implements ExtensionManifestParser {

    private final ObjectMapper mapper;

    public JacksonExtensionManifestParser() {
        this.mapper = new XmlMapper();
        this.mapper.registerModule(new JaxbAnnotationModule());
        this.mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public ExtensionManifest parse(InputStream inputStream) {
        try {
            return mapper.readValue(inputStream, ExtensionManifest.class);
        } catch (IOException e) {
            throw new BundleException("Unable to parse extension manifest due to: " + e.getMessage(), e);
        }
    }
}
