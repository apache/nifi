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
package org.apache.nifi.processors.oraclecdc.controller.impl;

import java.net.MalformedURLException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.nifi.oraclecdcservice.api.OracleClassLoaderService;

@Tags({ "dbcp", "jdbc", "database", "connection", "pooling", "store" })
@CapabilityDescription("Provides a shareable classloader for the Oracle CDC connection pool")

public class StandardOracleClassLoaderService extends AbstractControllerService implements OracleClassLoaderService {

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("database-driver-locations").displayName("Database Driver Location(s)")
            .description(
                    "Comma-separated list of files/folders and/or URLs containing the driver JAR and its dependencies (if any). For example '/var/tmp/mariadb-java-client-1.1.7.jar'")
            .defaultValue(null).required(true)
            .addValidator(
                    StandardValidators.createListValidator(true, true, StandardValidators.createURLorFileValidator()))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

    private static final List<PropertyDescriptor> properties;

    protected ClassLoader driverClassLoader;
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DB_DRIVER_LOCATION);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().name(propertyDescriptorName).required(false)
                .addValidator(StandardValidators
                        .createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).dynamic(true).build();
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String locationString = context.getProperty(DB_DRIVER_LOCATION).getValue();
        final String drv = "oracle.jdbc.OracleDriver";
        if (locationString != null && locationString.length() > 0) {
            try {
                // Split and trim the entries
                final ClassLoader classLoader = ClassLoaderUtils.getCustomClassLoader(locationString,
                        this.getClass().getClassLoader(), (dir, name) -> name != null && name.endsWith(".jar"));

                // Workaround which allows to use URLClassLoader for JDBC driver
                // loading.
                // (Because the DriverManager will refuse to use a driver not
                // loaded by the system ClassLoader.)
                final Class<?> clazz = Class.forName(drv, true, classLoader);
                if (clazz == null) {
                    throw new InitializationException("Can't load Database Driver " + drv);
                }
                final Driver driver = (Driver) clazz.newInstance();
                DriverManager.registerDriver(new DriverShim(driver));

                this.driverClassLoader = classLoader;
            } catch (final MalformedURLException e) {
                throw new InitializationException("Invalid Database Driver Jar Url", e);
            } catch (final Exception e) {
                throw new InitializationException("Can't load Database Driver", e);
            }
        } else {
            // That will ensure that you are using the ClassLoader for you NAR.
            this.driverClassLoader = Thread.currentThread().getContextClassLoader();
        }
    }

    /**
     * using Thread.currentThread().getContextClassLoader(); will ensure that
     * you are using the ClassLoader for you NAR.
     *
     * @throws InitializationException
     *             if there is a problem obtaining the ClassLoader
     */
    @Override
    public ClassLoader getClassLoader() {
        return this.driverClassLoader;
    }

}
