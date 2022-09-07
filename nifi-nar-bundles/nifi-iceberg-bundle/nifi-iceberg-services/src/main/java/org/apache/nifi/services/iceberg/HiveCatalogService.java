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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tags({"iceberg", "catalog", "service", "metastore", "hive"})
@CapabilityDescription("Catalog service that connects to a Hive metastore to keep track of Iceberg tables.")
public class HiveCatalogService extends AbstractCatalogService {

    static final PropertyDescriptor METASTORE_URI = new PropertyDescriptor.Builder()
            .name("hive-metastore-uri")
            .displayName("Hive Metastore URI")
            .description("The URI location(s) for the Hive metastore; note that this is not the location of the Hive Server. The default port for the Hive metastore is 9043.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .defaultValue("thrift://localhost:9083")
            .build();

    static final PropertyDescriptor WAREHOUSE_LOCATION = new PropertyDescriptor.Builder()
            .name("warehouse-location")
            .displayName("Default Warehouse Location")
            .description("Location of default database for the warehouse. This field sets or overrides the 'hive.metastore.warehouse.dir' configuration property.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            METASTORE_URI,
            WAREHOUSE_LOCATION,
            HADOOP_CONFIGURATION_RESOURCES
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private HiveCatalog catalog;


    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final String metastoreUri = context.getProperty(METASTORE_URI).evaluateAttributeExpressions().getValue();
        final String warehouseLocation = context.getProperty(WAREHOUSE_LOCATION).evaluateAttributeExpressions().getValue();

        catalog = new HiveCatalog();

        if (context.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet()) {
            final String configFiles = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();

            final Configuration hadoopConfig = getConfigurationFromFiles(configFiles);
            catalog.setConf(hadoopConfig);
        }

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
        properties.put(CatalogProperties.URI, metastoreUri);

        catalog.initialize("hive-catalog", properties);
    }

    @Override
    public Catalog getCatalog() {
        return catalog;
    }
}
