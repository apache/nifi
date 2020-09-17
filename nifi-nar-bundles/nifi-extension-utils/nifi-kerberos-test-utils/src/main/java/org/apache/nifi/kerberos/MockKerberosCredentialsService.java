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
package org.apache.nifi.kerberos;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.ArrayList;
import java.util.List;

public class MockKerberosCredentialsService extends AbstractControllerService implements KerberosCredentialsService {

    public static String DEFAULT_KEYTAB = "src/test/resources/fake.keytab";
    public static String DEFAULT_PRINCIPAL = "test@REALM.COM";

    private volatile String keytab = DEFAULT_KEYTAB;
    private volatile String principal = DEFAULT_PRINCIPAL;

    public static final PropertyDescriptor PRINCIPAL = new PropertyDescriptor.Builder()
            .name("Kerberos Principal")
            .description("Kerberos principal to authenticate as. Requires nifi.kerberos.krb5.file to be set in your nifi.properties")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    public static final PropertyDescriptor KEYTAB = new PropertyDescriptor.Builder()
            .name("Kerberos Keytab")
            .description("Kerberos keytab associated with the principal. Requires nifi.kerberos.krb5.file to be set in your nifi.properties")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    public MockKerberosCredentialsService() {
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        keytab = context.getProperty(KEYTAB).getValue();
        principal = context.getProperty(PRINCIPAL).getValue();
    }

    @Override
    public String getKeytab() {
        return keytab;
    }

    @Override
    public String getPrincipal() {
        return principal;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(2);
        properties.add(KEYTAB);
        properties.add(PRINCIPAL);
        return properties;
    }

    @Override
    public String getIdentifier() {
        return "kcs";
    }
}