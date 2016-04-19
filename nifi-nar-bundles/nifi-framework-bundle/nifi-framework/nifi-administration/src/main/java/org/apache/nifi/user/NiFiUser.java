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
package org.apache.nifi.user;

import java.io.Serializable;
import java.util.Objects;

/**
 * An NiFiUser.
 */
public class NiFiUser implements Serializable {

    public static final NiFiUser ANONYMOUS = new NiFiUser("anonymous");

    private String identity;
    private String userName;

    private NiFiUser chain;

    public NiFiUser(String identity) {
        this(identity, identity, null);
    }

    public NiFiUser(String identity, String userName) {
        this(identity, userName, null);
    }

    public NiFiUser(String identity, NiFiUser chain) {
        this(identity, identity, chain);
    }

    public NiFiUser(String identity, String userName, NiFiUser chain) {
        this.identity = identity;
        this.userName = userName;
        this.chain = chain;
    }

    /* getters / setters */

    public String getIdentity() {
        return identity;
    }

    public String getUserName() {
        return userName;
    }

    public NiFiUser getChain() {
        return chain;
    }

    public boolean isAnonymous() {
        return this == ANONYMOUS;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final NiFiUser other = (NiFiUser) obj;
        if (!Objects.equals(this.identity, other.identity)) {
            return false;
        }
        return true;
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
