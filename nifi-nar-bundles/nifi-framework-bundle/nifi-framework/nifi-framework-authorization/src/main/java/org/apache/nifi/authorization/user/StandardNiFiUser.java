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

import java.io.Serializable;
import java.util.Objects;

/**
 * An implementation of NiFiUser.
 */
public class StandardNiFiUser implements NiFiUser, Serializable {
    private static final long serialVersionUID = -5503790026187817496L;

    public static final StandardNiFiUser ANONYMOUS = new StandardNiFiUser("anonymous");

    private final String identity;
    private final String userName;
    private final NiFiUser chain;

    public StandardNiFiUser(String identity) {
        this(identity, identity, null);
    }

    public StandardNiFiUser(String identity, NiFiUser chain) {
        this(identity, identity, chain);
    }

    public StandardNiFiUser(String identity, String userName, NiFiUser chain) {
        this.identity = identity;
        this.userName = userName;
        this.chain = chain;
    }


    @Override
    public String getIdentity() {
        return identity;
    }

    @Override
    public String getUserName() {
        return userName;
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
        return String.format("identity[%s], userName[%s]", getIdentity(), getUserName(), ", ");
    }
}
