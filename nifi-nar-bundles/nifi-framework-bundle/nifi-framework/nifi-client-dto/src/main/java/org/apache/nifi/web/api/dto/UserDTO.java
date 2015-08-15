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
package org.apache.nifi.web.api.dto;

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Date;
import java.util.Set;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;

/**
 * A user of this NiFi.
 */
@XmlType(name = "user")
public class UserDTO {

    private String id;
    private String dn;
    private String userName;
    private String userGroup;
    private String justification;
    private Date creation;
    private String status;

    private Date lastVerified;
    private Date lastAccessed;
    private Set<String> authorities;

    /**
     * @return user id
     */
    @ApiModelProperty(
            value = "The id of the user."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return users authorities
     */
    @ApiModelProperty(
            value = "The users authorities."
    )
    public Set<String> getAuthorities() {
        return authorities;
    }

    public void setAuthorities(Set<String> authorities) {
        this.authorities = authorities;
    }

    /**
     * @return creation time for this users account
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @ApiModelProperty(
            value = "The timestamp when the user was created."
    )
    public Date getCreation() {
        return creation;
    }

    public void setCreation(Date creation) {
        this.creation = creation;
    }

    /**
     * @return users DN
     */
    @ApiModelProperty(
            value = "The dn of the user."
    )
    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    /**
     * @return users name. If the name could not be extracted from the DN, this value will be the entire DN
     */
    @ApiModelProperty(
            value = "The username. If it could not be extracted from the DN, this value will be the entire DN."
    )
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * @return user group
     */
    @ApiModelProperty(
            value = "The group this user belongs to."
    )
    public String getUserGroup() {
        return userGroup;
    }

    public void setUserGroup(String userGroup) {
        this.userGroup = userGroup;
    }

    /**
     * @return users account justification
     */
    @ApiModelProperty(
            value = "The justification for the user account."
    )
    public String getJustification() {
        return justification;
    }

    public void setJustification(String justification) {
        this.justification = justification;
    }

    /**
     * @return time that the user last accessed the system
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @ApiModelProperty(
            value = "The timestamp the user last accessed the system."
    )
    public Date getLastAccessed() {
        return lastAccessed;
    }

    public void setLastAccessed(Date lastAccessed) {
        this.lastAccessed = lastAccessed;
    }

    /**
     * @return time that the users credentials were last verified
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @ApiModelProperty(
            value = "The timestamp the user authorities were verified."
    )
    public Date getLastVerified() {
        return lastVerified;
    }

    public void setLastVerified(Date lastVerified) {
        this.lastVerified = lastVerified;
    }

    /**
     * @return status of the users account
     */
    @ApiModelProperty(
            value = "The user status."
    )
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

}
