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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.hadoop.HdfsResources;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.nifi.hadoop.SecurityUtil.checkTGTAndRelogin;
import static org.apache.nifi.hadoop.SecurityUtil.getUgiForKerberosUser;
import static org.apache.nifi.hadoop.SecurityUtil.isSecurityEnabled;
import static org.apache.nifi.hadoop.SecurityUtil.loginSimple;
import static org.apache.nifi.processors.iceberg.PutIceberg.REL_FAILURE;

/**
 * Base Iceberg processor class.
 */
@RequiresInstanceClassLoading(cloneAncestorResources = true)
public abstract class AbstractIcebergProcessor extends AbstractProcessor implements ClassloaderIsolationKeyProvider {

    static final PropertyDescriptor CATALOG = new PropertyDescriptor.Builder()
            .name("catalog-service")
            .displayName("Catalog Service")
            .description("Specifies the Controller Service to use for handling references to table’s metadata files.")
            .identifiesControllerService(IcebergCatalogService.class)
            .required(true)
            .build();

    static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos.")
            .identifiesControllerService(KerberosUserService.class)
            .build();

    private static final Object RESOURCES_LOCK = new Object();
    private static final HdfsResources EMPTY_HDFS_RESOURCES = new HdfsResources(null, null, null, null);
    private final AtomicReference<HdfsResources> hdfsResources = new AtomicReference<>();

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        final Configuration config = catalogService.getConfiguration();

        UserGroupInformation ugi;
        KerberosUser kerberosUser;

        // -- use RESOURCE_LOCK to guarantee UserGroupInformation is accessed by only a single thread at a time
        synchronized (RESOURCES_LOCK) {
            if (kerberosUserService != null && isSecurityEnabled(config)) {
                kerberosUser = kerberosUserService.createKerberosUser();
                ugi = getUgiForKerberosUser(config, kerberosUser);
            } else {
                config.set(IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, "true");
                config.set(HADOOP_SECURITY_AUTHENTICATION, "simple");
                ugi = loginSimple(config);
                kerberosUser = null;
            }
        }

        hdfsResources.set(new HdfsResources(config, null, ugi, kerberosUser));
    }

    @OnStopped
    public final void onStopped() {
        final KerberosUser kerberosUser = getKerberosUser();
        if (kerberosUser != null) {
            try {
                kerberosUser.logout();
            } catch (KerberosLoginException e) {
                getLogger().error("Error logging out kerberos user", e);
            } finally {
                hdfsResources.set(EMPTY_HDFS_RESOURCES);
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (getKerberosUser() == null) {
            doOnTrigger(context, session, flowFile);
        } else {
            try {
                getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
                    doOnTrigger(context, session, flowFile);
                    return null;
                });

            } catch (Exception e) {
                getLogger().error("Privileged action failed with kerberos user " + getKerberosUser(), e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
            }
        }
    }

    private UserGroupInformation getUgi() {
        checkTGTAndRelogin(getLogger(), getKerberosUser());
        return hdfsResources.get().getUserGroupInformation();
    }

    @Override
    public String getClassloaderIsolationKey(PropertyContext context) {
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        if (kerberosUserService != null) {
            return kerberosUserService.getIdentifier();
        }

        return null;
    }

    private KerberosUser getKerberosUser() {
        return hdfsResources.get().getKerberosUser();
    }

    protected abstract void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException;
}
