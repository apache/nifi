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

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.security.krb.KerberosTicketCacheUser;
import org.apache.nifi.security.krb.KerberosUser;

import java.util.Collections;
import java.util.List;

@CapabilityDescription("Provides a mechanism for creating a KerberosUser from a principal and ticket cache that other components " +
        "are able to use in order to perform authentication using Kerberos. By encapsulating this information into a Controller Service " +
        "and allowing other components to make use of it an administrator is able to choose which users are allowed to use which ticket " +
        "caches and principals. This provides a more robust security model for multi-tenant use cases.")
@Tags({"Kerberos", "Ticket", "Cache", "Principal", "Credentials", "Authentication", "Security"})
@Restricted(restrictions = {
        @Restriction(requiredPermission = RequiredPermission.ACCESS_TICKET_CACHE,
                explanation = "Allows user to define a ticket cache and principal that can then be used by other components.")
})
public class KerberosTicketCacheUserService extends AbstractKerberosUserService implements SelfContainedKerberosUserService {

    static final PropertyDescriptor TICKET_CACHE_FILE = new PropertyDescriptor.Builder()
            .name("Kerberos Ticket Cache File")
            .description("Kerberos ticket cache associated with the principal.")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    private volatile String ticketCacheFile;

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Collections.singletonList(TICKET_CACHE_FILE);
    }

    @Override
    protected void setAdditionalConfiguredValues(final ConfigurationContext context) {
        this.ticketCacheFile = context.getProperty(TICKET_CACHE_FILE).evaluateAttributeExpressions().getValue();
    }

    @Override
    public KerberosUser createKerberosUser() {
        return new KerberosTicketCacheUser(getPrincipal(), ticketCacheFile);
    }

}
