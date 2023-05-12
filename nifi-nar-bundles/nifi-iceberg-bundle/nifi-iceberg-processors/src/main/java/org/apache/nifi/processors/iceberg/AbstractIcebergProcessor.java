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
package org.apache.nifi.processors.iceberg;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static org.apache.nifi.hadoop.SecurityUtil.getUgiForKerberosUser;

/**
 * Base Iceberg processor class.
 */
public abstract class AbstractIcebergProcessor extends AbstractProcessor {

    public static final PropertyDescriptor CATALOG = new PropertyDescriptor.Builder()
            .name("catalog-service")
            .displayName("Catalog Service")
            .description("Specifies the Controller Service to use for handling references to table’s metadata files.")
            .identifiesControllerService(IcebergCatalogService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos.")
            .identifiesControllerService(KerberosUserService.class)
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the operation failed and retrying the operation will also fail, such as an invalid data or schema.")
            .build();

    private volatile KerberosUser kerberosUser;
    private volatile UserGroupInformation ugi;

    @OnScheduled
    public final void onScheduled(final ProcessContext context) {
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosUserService != null) {
            this.kerberosUser = kerberosUserService.createKerberosUser();
            try {
                this.ugi = getUgiForKerberosUser(catalogService.getConfiguration(), kerberosUser);
            } catch (IOException e) {
                throw new ProcessException("Kerberos Authentication failed", e);
            }
        }
    }

    @OnStopped
    public final void onStopped() {
        if (kerberosUser != null) {
            try {
                kerberosUser.logout();
            } catch (KerberosLoginException e) {
                getLogger().error("Error logging out kerberos user", e);
            } finally {
                kerberosUser = null;
                ugi = null;
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (kerberosUser == null) {
            doOnTrigger(context, session, flowFile);
        } else {
            try {
                getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
                    doOnTrigger(context, session, flowFile);
                    return null;
                });

            } catch (Exception e) {
                getLogger().error("Privileged action failed with kerberos user " + kerberosUser, e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
            }
        }
    }

    private UserGroupInformation getUgi() {
        try {
            kerberosUser.checkTGTAndRelogin();
        } catch (KerberosLoginException e) {
            throw new ProcessException("Unable to re-login with kerberos credentials for " + kerberosUser.getPrincipal(), e);
        }
        return ugi;
    }

    protected abstract void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException;
}
