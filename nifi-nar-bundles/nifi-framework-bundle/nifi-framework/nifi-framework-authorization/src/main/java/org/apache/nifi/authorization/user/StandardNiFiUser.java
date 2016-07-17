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
package org.apache.nifi.authorization.user;

import java.util.Objects;

/**
 * An implementation of NiFiUser.
 */
public class StandardNiFiUser implements NiFiUser {

    public static final StandardNiFiUser ANONYMOUS = new StandardNiFiUser("anonymous");

    private final String identity;
    private final NiFiUser chain;
    private final String clientAddress;

    public StandardNiFiUser(String identity) {
        this(identity, null, null);
    }

    public StandardNiFiUser(String identity, String clientAddress) {
        this(identity, null, clientAddress);
    }

    public StandardNiFiUser(String identity, NiFiUser chain) {
        this(identity, chain, null);
    }

    public StandardNiFiUser(String identity, NiFiUser chain, String clientAddress) {
        this.identity = identity;
        this.chain = chain;
        this.clientAddress = clientAddress;
    }


    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public NiFiUser getChain() {
        return chain;
    }

    @Override
    public boolean isAnonymous() {
        return this == ANONYMOUS;
    }

    @Override
    public String getClientAddress() {
        return clientAddress;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof NiFiUser)) {
            return false;
        }

        final NiFiUser other = (NiFiUser) obj;
        return Objects.equals(this.identity, other.getIdentity());
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.identity);
        return hash;
    }

    @Override
    public String toString() {
        return String.format("identity[%s]", getIdentity());
    }
}
