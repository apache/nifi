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
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.FILE_IO_IMPLEMENTATION;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.CLIENT_POOL_SERVICE;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.CLIENT_POOL_SIZE;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.WAREHOUSE_LOCATION;

@Tags({"iceberg", "catalog", "service", "jdbc"})
@CapabilityDescription("Catalog service using relational database to manage Iceberg tables through JDBC.")
public class JdbcCatalogService extends AbstractCatalogService {

    public static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("Catalog Name")
            .description("Name of the Iceberg catalog.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("jdbc-catalog")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("Connection Pool")
            .description("Specifies the JDBC Connection Pool to use in order to communicate with the Iceberg catalog.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor CONNECTION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("Connection Pool Size")
            .description("Specifies the size of the JDBC Connection Pool.")
            .defaultValue("2")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILE_IO_IMPL = new PropertyDescriptor.Builder()
            .name("File IO Implementation")
            .description("Specifies the implementation of FileIO interface to be used. " +
                    "The provided implementation have to include the class and full package name.")
            .defaultValue("org.apache.iceberg.hadoop.HadoopFileIO")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            CATALOG_NAME, CONNECTION_POOL, CONNECTION_POOL_SIZE, FILE_IO_IMPL, WAREHOUSE_PATH, HADOOP_CONFIGURATION_RESOURCES);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        if (context.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet()) {
            configFilePaths = createFilePathList(context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue());
        }

        catalogProperties.put(IcebergCatalogProperty.CATALOG_NAME, context.getProperty(CATALOG_NAME).evaluateAttributeExpressions().getValue());
        catalogProperties.put(CLIENT_POOL_SERVICE, context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class));
        catalogProperties.put(CLIENT_POOL_SIZE, context.getProperty(CONNECTION_POOL_SIZE).evaluateAttributeExpressions().getValue());
        catalogProperties.put(FILE_IO_IMPLEMENTATION, context.getProperty(FILE_IO_IMPL).evaluateAttributeExpressions().getValue());
        catalogProperties.put(WAREHOUSE_LOCATION, context.getProperty(WAREHOUSE_PATH).evaluateAttributeExpressions().getValue());
    }

    @Override
    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.JDBC;
    }

}
