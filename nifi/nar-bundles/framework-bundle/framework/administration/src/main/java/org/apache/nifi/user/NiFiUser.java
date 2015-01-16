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
import java.util.Date;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.apache.nifi.authorization.Authority;
import org.apache.commons.lang3.StringUtils;

/**
 * An NiFiUser.
 */
public class NiFiUser implements Serializable {

    public static final String ANONYMOUS_USER_DN = "anonymous";

    private String id;
    private String dn;
    private String userName;
    private String userGroup;
    private String justification;

    private Date creation;
    private Date lastVerified;
    private Date lastAccessed;

    private AccountStatus status;
    private EnumSet<Authority> authorities;
    
    private NiFiUser chain;

    /* getters / setters */
    public Date getCreation() {
        return creation;
    }

    public void setCreation(Date creation) {
        this.creation = creation;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserGroup() {
        return userGroup;
    }

    public void setUserGroup(String userGroup) {
        this.userGroup = userGroup;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getJustification() {
        return justification;
    }

    public void setJustification(String justification) {
        this.justification = justification;
    }

    public AccountStatus getStatus() {
        return status;
    }

    public void setStatus(AccountStatus status) {
        this.status = status;
    }

    public Date getLastVerified() {
        return lastVerified;
    }

    public void setLastVerified(Date lastVerified) {
        this.lastVerified = lastVerified;
    }

    public Date getLastAccessed() {
        return lastAccessed;
    }

    public void setLastAccessed(Date lastAccessed) {
        this.lastAccessed = lastAccessed;
    }

    public NiFiUser getChain() {
        return chain;
    }

    public void setChain(NiFiUser chain) {
        this.chain = chain;
    }

    public Set<Authority> getAuthorities() {
        if (authorities == null) {
            authorities = EnumSet.noneOf(Authority.class);
        }
        return authorities;
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
        if (!Objects.equals(this.dn, other.dn)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.dn);
        return hash;
    }

    @Override
    public String toString() {
        return String.format("dn[%s], userName[%s], justification[%s], authorities[%s]", getDn(), getUserName(), getJustification(), StringUtils.join(getAuthorities(), ", "));
    }

}
