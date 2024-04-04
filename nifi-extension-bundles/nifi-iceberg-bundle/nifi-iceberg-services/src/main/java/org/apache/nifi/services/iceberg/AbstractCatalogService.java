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
package org.apache.nifi.services.iceberg;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.w3c.dom.Document;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Abstract class holding common properties and methods for Catalog Service implementations.
 */
public abstract class AbstractCatalogService extends AbstractControllerService implements IcebergCatalogService {

    protected Map<IcebergCatalogProperty, String> catalogProperties = new HashMap<>();

    protected List<String> configFilePaths;

    static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hadoop-config-resources")
            .displayName("Hadoop Configuration Resources")
            .description("A file, or comma separated list of files, which contain the Hadoop configuration (core-site.xml, etc.). Without this, default configuration will be used.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dynamicallyModifiesClasspath(true)
            .build();

    protected List<Document> parseConfigFilePaths(String configFilePaths) {
        List<Document> documentList = new ArrayList<>();
        for (final String configFile : createFilePathList(configFilePaths)) {
            File file = new File(configFile.trim());
            try (final InputStream fis = new FileInputStream(file);
                 final InputStream in = new BufferedInputStream(fis)) {
                final StandardDocumentProvider documentProvider = new StandardDocumentProvider();
                documentList.add(documentProvider.parse(in));
            } catch (IOException e) {
                throw new ProcessException("Failed to load config files", e);
            }
        }
        return documentList;
    }

    protected List<String> createFilePathList(String configFilePaths) {
        List<String> filePathList = new ArrayList<>();
        if (configFilePaths != null && !configFilePaths.trim().isEmpty()) {
            for (final String configFile : configFilePaths.split(",")) {
                filePathList.add(configFile.trim());
            }
        }
        return filePathList;
    }

    @Override
    public Map<IcebergCatalogProperty, String> getCatalogProperties() {
        return catalogProperties;
    }

    @Override
    public List<String> getConfigFilePaths() {
        return configFilePaths;
    }
}
