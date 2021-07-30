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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;

import java.util.Collections;
import java.util.List;

@CapabilityDescription("Provides a mechanism for creating a KerberosUser from a principal and password that other " +
        "components are able to use in order to perform authentication using Kerberos.")
@Tags({"Kerberos", "Password", "Principal", "Credentials", "Authentication", "Security"})
public class KerberosPasswordUserService extends AbstractKerberosUserService {

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Kerberos Password")
            .description("Kerberos password associated with the principal.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    private volatile String password;

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Collections.singletonList(PASSWORD);
    }

    @Override
    protected void setAdditionalConfiguredValues(final ConfigurationContext context) {
        this.password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
    }

    @Override
    public KerberosUser createKerberosUser() {
        return new KerberosPasswordUser(getPrincipal(), password);
    }

}
