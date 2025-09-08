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

package org.apache.nifi.framework.cluster.zookeeper;

import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.X509Util;

public class SecureClientZooKeeperFactory implements ZookeeperFactory {

    public static final String NETTY_CLIENT_CNXN_SOCKET = org.apache.zookeeper.ClientCnxnSocketNetty.class.getName();

    private ZKClientConfig zkSecureClientConfig;

    public SecureClientZooKeeperFactory(final ZooKeeperClientConfig zkConfig) {
        this.zkSecureClientConfig = new ZKClientConfig();

        // Netty is required for the secure client config.
        final String cnxnSocket = zkConfig.getConnectionSocket();
        if (!NETTY_CLIENT_CNXN_SOCKET.equals(cnxnSocket)) {
            throw new IllegalArgumentException(String.format("connection factory set to '%s', %s required", String.valueOf(cnxnSocket), NETTY_CLIENT_CNXN_SOCKET));
        }
        zkSecureClientConfig.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET, cnxnSocket);

        // This should never happen but won't get checked elsewhere.
        final boolean clientSecure = zkConfig.isClientSecure();
        if (!clientSecure) {
            throw new IllegalStateException(String.format("%s set to '%b', expected true", ZKClientConfig.SECURE_CLIENT, clientSecure));
        }
        zkSecureClientConfig.setProperty(ZKClientConfig.SECURE_CLIENT, String.valueOf(clientSecure));

        final X509Util clientX509util = new ClientX509Util();
        zkSecureClientConfig.setProperty(clientX509util.getSslKeystoreLocationProperty(), zkConfig.getKeyStore());
        zkSecureClientConfig.setProperty(clientX509util.getSslKeystoreTypeProperty(), zkConfig.getKeyStoreType());
        zkSecureClientConfig.setProperty(clientX509util.getSslKeystorePasswdProperty(), zkConfig.getKeyStorePassword());
        zkSecureClientConfig.setProperty(clientX509util.getSslTruststoreLocationProperty(), zkConfig.getTrustStore());
        zkSecureClientConfig.setProperty(clientX509util.getSslTruststoreTypeProperty(), zkConfig.getTrustStoreType());
        zkSecureClientConfig.setProperty(clientX509util.getSslTruststorePasswdProperty(), zkConfig.getTrustStorePassword());
    }

    @Override
    public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception {
        return new ZooKeeperAdmin(connectString, sessionTimeout, watcher, zkSecureClientConfig);
    }
}
