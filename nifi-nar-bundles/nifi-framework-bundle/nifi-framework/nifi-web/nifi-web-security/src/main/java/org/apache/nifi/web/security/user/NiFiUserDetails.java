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
package org.apache.nifi.web.security.user;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.user.NiFiUser;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * User details for a NiFi user.
 */
public class NiFiUserDetails implements UserDetails {

    private final NiFiUser user;

    /**
     * Creates a new NiFiUserDetails.
     *
     * @param user user
     */
    public NiFiUserDetails(NiFiUser user) {
        this.user = user;
    }

    /**
     * Get the user for this UserDetails.
     *
     * @return user
     */
    public NiFiUser getNiFiUser() {
        return user;
    }

    /**
     * Returns the authorities that this NiFi user has.
     *
     * @return authorities
     */
    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        final Set<Authority> authorities = user.getAuthorities();
        final Set<GrantedAuthority> grantedAuthorities = new HashSet<>(authorities.size());
        for (final Authority authority : authorities) {
            grantedAuthorities.add(new SimpleGrantedAuthority(authority.toString()));
        }
        return grantedAuthorities;
    }

    @Override
    public String getPassword() {
        return StringUtils.EMPTY;
    }

    @Override
    public String getUsername() {
        return user.getIdentity();
    }

    @Override
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    public boolean isAccountNonLocked() {
        return true;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

}
