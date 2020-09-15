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
package org.apache.nifi.idp;

import java.util.Date;

public class IdpCredential {

    private int id;
    private String identity;
    private IdpType type;
    private byte[] credential;
    private Date created;

    public IdpCredential() {

    }

    public IdpCredential(int id, String identity, IdpType type, byte[] credential) {
       this(id, identity, type, credential, new Date());
    }

    public IdpCredential(int id, String identity, IdpType type, byte[] credential, Date created) {
        this.id = id;
        this.identity = identity;
        this.type = type;
        this.credential = credential;
        this.created = created;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public IdpType getType() {
        return type;
    }

    public void setType(IdpType type) {
        this.type = type;
    }

    public byte[] getCredential() {
        return credential;
    }

    public void setCredential(byte[] credential) {
        this.credential = credential;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }
}
