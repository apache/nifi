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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.WAREHOUSE_LOCATION;

@Tags({"iceberg", "catalog", "service", "hadoop", "hdfs"})
@CapabilityDescription("Catalog service that can use HDFS or similar file systems that support atomic rename.")
public class HadoopCatalogService extends AbstractCatalogService {

    static final PropertyDescriptor WAREHOUSE_PATH = new PropertyDescriptor.Builder()
            .name("warehouse-path")
            .displayName("Warehouse Path")
            .description("Path to the location of the warehouse.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            WAREHOUSE_PATH,
            HADOOP_CONFIGURATION_RESOURCES
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        if (context.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet()) {
            configFilePaths = createFilePathList(context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue());
        }

        catalogProperties.put(WAREHOUSE_LOCATION, context.getProperty(WAREHOUSE_PATH).evaluateAttributeExpressions().getValue());
    }

    @Override
    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.HADOOP;
    }

}
