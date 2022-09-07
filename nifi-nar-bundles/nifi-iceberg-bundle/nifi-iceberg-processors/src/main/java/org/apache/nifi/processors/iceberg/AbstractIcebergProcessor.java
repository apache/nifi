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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;

import java.security.PrivilegedExceptionAction;

/**
 * Base Iceberg processor class.
 */
public abstract class AbstractIcebergProcessor extends AbstractProcessor {

    static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos.")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();

    private volatile KerberosUser kerberosUser;

    @OnScheduled
    public final void onScheduled(final ProcessContext context) {
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        if (kerberosUserService != null) {
            this.kerberosUser = kerberosUserService.createKerberosUser();
        }
    }

    @OnStopped
    public final void closeClient() {
        if (kerberosUser != null) {
            try {
                kerberosUser.logout();
                kerberosUser = null;
            } catch (final KerberosLoginException e) {
                getLogger().debug("Error logging out keytab user", e);
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final KerberosUser kerberosUser = getKerberosUser();
        if (kerberosUser == null) {
            doOnTrigger(context, session);
        } else {
            // wrap doOnTrigger in a privileged action
            final PrivilegedExceptionAction<Void> action = () -> {
                doOnTrigger(context, session);
                return null;
            };

            // execute the privileged action as the given keytab user
            final KerberosAction kerberosAction = new KerberosAction<>(kerberosUser, action, getLogger());
            try {
                kerberosAction.execute();
            } catch (ProcessException e) {
                context.yield();
                throw e;
            }
        }
    }

    protected abstract void doOnTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;

    protected final KerberosUser getKerberosUser() {
        return kerberosUser;
    }
}
