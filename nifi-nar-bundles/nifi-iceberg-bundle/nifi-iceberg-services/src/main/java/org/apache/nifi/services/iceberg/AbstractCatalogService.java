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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;


/**
 * Abstract class holding common properties and methods for Catalog Service implementations.
 */
public abstract class AbstractCatalogService extends AbstractControllerService implements IcebergCatalogService {

    protected Configuration configuration = new Configuration();

    static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hadoop-config-resources")
            .displayName("Hadoop Configuration Resources")
            .description("A file, or comma separated list of files, which contain the Hadoop configuration (core-site.xml, etc.). Without this, default configuration will be used.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dynamicallyModifiesClasspath(true)
            .build();

    /**
     * Loads configuration files from the provided paths.
     *
     * @param configFiles list of config file paths separated with comma
     * @return merged configuration
     */
    protected Configuration getConfigurationFromFiles(String configFiles) {
        final Configuration conf = new Configuration();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                conf.addResource(new Path(configFile.trim()));
            }
        }
        return conf;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }
}
