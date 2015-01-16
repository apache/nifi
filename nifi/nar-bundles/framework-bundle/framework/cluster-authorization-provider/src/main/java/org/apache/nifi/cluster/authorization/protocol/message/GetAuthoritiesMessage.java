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
package org.apache.nifi.cluster.authorization.protocol.message;

import java.util.HashSet;
import java.util.Set;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.nifi.authorization.Authority;

/**
 * @author unattributed
 */
@XmlRootElement(name = "getAuthoritiesMessage")
public class GetAuthoritiesMessage extends ProtocolMessage {

    private String dn;

    private Set<Authority> response = new HashSet<>();

    public GetAuthoritiesMessage() {
    }

    @Override
    public MessageType getType() {
        return MessageType.GET_AUTHORITIES;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    public Set<Authority> getResponse() {
        return response;
    }

    public void setResponse(Set<Authority> response) {
        this.response = response;
    }

}
