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

package org.apache.nifi.controller.leader.election;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import com.google.common.collect.Lists;

public class CuratorACLProviderFactory {

    public static final String SASL_AUTH_SCHEME = "sasl";

    public ACLProvider create(ZooKeeperClientConfig config){
        return StringUtils.equalsIgnoreCase(config.getAuthType(),SASL_AUTH_SCHEME) ? new SaslACLProvider(config) : new DefaultACLProvider();
    }

    private class SaslACLProvider implements ACLProvider{

        private final List<ACL> acls;

        private SaslACLProvider(ZooKeeperClientConfig config) {

            if(!StringUtils.isEmpty(config.getAuthPrincipal())) {

                final String realm = config.getAuthPrincipal().substring(config.getAuthPrincipal().indexOf('@') + 1, config.getAuthPrincipal().length());
                final String[] user = config.getAuthPrincipal().substring(0, config.getAuthPrincipal().indexOf('@')).split("/");
                final String host = user.length == 2 ? user[1] : null;
                final String instance = user[0];
                final StringBuilder principal = new StringBuilder(instance);

                if (!config.getRemoveHostFromPrincipal().equalsIgnoreCase("true")) {
                    principal.append("/");
                    principal.append(host);
                }

                if (!config.getRemoveRealmFromPrincipal().equalsIgnoreCase("true")) {
                    principal.append("@");
                    principal.append(realm);
                }

                this.acls = Lists.newArrayList(new ACL(ZooDefs.Perms.ALL, new Id(SASL_AUTH_SCHEME, principal.toString())));
                this.acls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);

            }else{
                throw new IllegalArgumentException("No Kerberos Principal configured for use with SASL Authentication Scheme");
            }
        }

        @Override
        public List<ACL> getDefaultAcl() {
            return acls;
        }

        @Override
        public List<ACL> getAclForPath(String s) {
            return acls;
        }
    }

}
