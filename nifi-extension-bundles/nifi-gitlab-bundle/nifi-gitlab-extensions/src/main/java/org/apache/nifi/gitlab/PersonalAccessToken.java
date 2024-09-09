/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.gitlab;

import java.util.List;

/**
 * Response from retrieving a personal access token from the GitLab API.
 */
public class PersonalAccessToken {

    private long id;
    private long user_id;

    private String name;
    private String revoked;
    private String created_at;
    private String expires_at;
    private String last_user_at;

    private List<String> scopes;

    private boolean active;

    public long getId() {
        return id;
    }

    public void setId(final long id) {
        this.id = id;
    }

    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(final long user_id) {
        this.user_id = user_id;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getRevoked() {
        return revoked;
    }

    public void setRevoked(final String revoked) {
        this.revoked = revoked;
    }

    public String getCreated_at() {
        return created_at;
    }

    public void setCreated_at(final String created_at) {
        this.created_at = created_at;
    }

    public String getExpires_at() {
        return expires_at;
    }

    public void setExpires_at(final String expires_at) {
        this.expires_at = expires_at;
    }

    public String getLast_user_at() {
        return last_user_at;
    }

    public void setLast_user_at(final String last_user_at) {
        this.last_user_at = last_user_at;
    }

    public List<String> getScopes() {
        return scopes;
    }

    public void setScopes(final List<String> scopes) {
        this.scopes = scopes;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(final boolean active) {
        this.active = active;
    }
}
