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
package org.apache.nifi.processors.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import static org.apache.nifi.hadoop.SecurityUtil.getUgiForKerberosUser;

/**
 * Base Hudi processor class.
 */
@RequiresInstanceClassLoading(cloneAncestorResources = true)
public abstract class AbstractHudiProcessor extends AbstractProcessor implements ClassloaderIsolationKeyProvider {

    static final PropertyDescriptor HUDI_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hudi-config-resources")
            .displayName("Hudi Configuration Resources")
            .description("A file, or comma separated list of files, which contain the Hudi configurations. Without this, default configuration will be used.")
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hadoop-config-resources")
            .displayName("Hadoop Configuration Resources")
            .description("A file, or comma separated list of files, which contain the Hadoop configuration (core-site.xml, etc.). Without this, default configuration will be used.")
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
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
    public void onScheduled(final ProcessContext context) {
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosUserService != null) {
            this.kerberosUser = kerberosUserService.createKerberosUser();
            try {
                this.ugi = getUgiForKerberosUser(getConfigurationFromFiles(getConfigLocations(context)), kerberosUser);
            } catch (IOException e) {
                throw new ProcessException("Kerberos Authentication failed", e);
            }
        }
    }

    @OnStopped
    public void onStopped() {
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

    @Override
    public String getClassloaderIsolationKey(PropertyContext context) {
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        if (kerberosUserService != null) {
            return kerberosUserService.getIdentifier();
        }
        return null;
    }

    private UserGroupInformation getUgi() {
        try {
            kerberosUser.checkTGTAndRelogin();
        } catch (KerberosLoginException e) {
            throw new ProcessException("Unable to re-login with kerberos credentials for " + kerberosUser.getPrincipal(), e);
        }
        return ugi;
    }

    protected List<String> getConfigLocations(PropertyContext context) {
        final ResourceReferences configResources = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().asResources();
        final List<String> locations = configResources.asLocations();
        return locations;
    }

    /**
     * Loads configuration files from the provided paths.
     *
     * @param configFilePaths list of config file paths separated with comma
     * @return merged configuration
     */
    protected Configuration getConfigurationFromFiles(List<String> configFilePaths) {
        final Configuration conf = new Configuration();
        if (configFilePaths != null) {
            for (final String configFile : configFilePaths) {
                conf.addResource(new Path(configFile.trim()));
            }
        }
        return conf;
    }

    protected abstract void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException;
}
