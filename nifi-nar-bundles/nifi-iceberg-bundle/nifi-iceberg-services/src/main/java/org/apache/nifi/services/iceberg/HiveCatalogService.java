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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Tags({"iceberg", "catalog", "service", "metastore", "hive"})
@CapabilityDescription("Catalog service that connects to a Hive metastore to keep track of Iceberg tables.")
public class HiveCatalogService extends AbstractCatalogService {

    static final PropertyDescriptor METASTORE_URI = new PropertyDescriptor.Builder()
            .name("hive-metastore-uri")
            .displayName("Hive Metastore URI")
            .description("The URI location(s) for the Hive metastore; note that this is not the location of the Hive Server. The default port for the Hive metastore is 9043.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .build();

    static final PropertyDescriptor WAREHOUSE_LOCATION = new PropertyDescriptor.Builder()
            .name("warehouse-location")
            .displayName("Default Warehouse Location")
            .description("Location of default database for the warehouse. This field sets or overrides the 'hive.metastore.warehouse.dir' configuration property.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
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

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {

        final List<ValidationResult> problems = new ArrayList<>();
        boolean configMetastoreUriPresent = false;
        boolean configWarehouseLocationPresent = false;

        final String propertyMetastoreUri = validationContext.getProperty(METASTORE_URI).evaluateAttributeExpressions().getValue();
        final String propertyWarehouseLocation = validationContext.getProperty(WAREHOUSE_LOCATION).evaluateAttributeExpressions().getValue();

        // Load the configurations for validation only if any config resource is provided and if either the metastore URI or the warehouse location property is missing
        if (validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet() && (propertyMetastoreUri == null || propertyWarehouseLocation == null)) {
            final String configFiles = validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            final List<Document> documents = parseConfigFilePaths(configFiles);

            for (Document document : documents) {
                final NodeList nameNodeList = document.getElementsByTagName("name");

                for (int i = 0; i < nameNodeList.getLength(); i++) {
                    final String nodeValue = nameNodeList.item(i).getFirstChild().getNodeValue();

                    if (nodeValue.equals(IcebergCatalogProperty.METASTORE_URI.getHadoopPropertyName())) {
                        configMetastoreUriPresent = true;
                    }

                    if (nodeValue.equals(IcebergCatalogProperty.WAREHOUSE_LOCATION.getHadoopPropertyName())) {
                        configWarehouseLocationPresent = true;
                    }

                    if (configMetastoreUriPresent && configWarehouseLocationPresent) {
                        break;
                    }
                }
            }
        }

        if (!configMetastoreUriPresent && propertyMetastoreUri == null) {
            problems.add(new ValidationResult.Builder()
                    .subject("Hive Metastore URI")
                    .valid(false)
                    .explanation("cannot find hive metastore uri, please provide it in the 'Hive Metastore URI' property" +
                            " or provide a configuration file which contains 'hive.metastore.uris' value.")
                    .build());
        }

        if (!configWarehouseLocationPresent && propertyWarehouseLocation == null) {
            problems.add(new ValidationResult.Builder()
                    .subject("Default Warehouse Location")
                    .valid(false)
                    .explanation("cannot find default warehouse location, please provide it in the 'Default Warehouse Location' property" +
                            " or provide a configuration file which contains 'hive.metastore.warehouse.dir' value.")
                    .build());
        }

        return problems;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        if (context.getProperty(METASTORE_URI).isSet()) {
            catalogProperties.put(IcebergCatalogProperty.METASTORE_URI, context.getProperty(METASTORE_URI).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(WAREHOUSE_LOCATION).isSet()) {
            catalogProperties.put(IcebergCatalogProperty.WAREHOUSE_LOCATION, context.getProperty(WAREHOUSE_LOCATION).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet()) {
            configFilePaths = createFilePathList(context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue());
        }
    }

    @Override
    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.HIVE;
    }
}
