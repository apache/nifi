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
package org.apache.nifi.web.api.entity;

import java.util.Collection;
import java.util.Date;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * A serialized representation of this class can be placed in the entity body of
 * a request or response to or from the API. This particular entity holds a
 * reference to a collection of UserDTO.
 */
@XmlRootElement(name = "usersEntity")
public class UsersEntity extends Entity {

    private Collection<UserDTO> users;
    private Date generated;

    /**
     * The collection of UserDTOs that are being serialized.
     *
     * @return The UserDTO object
     */
    public Collection<UserDTO> getUsers() {
        return users;
    }

    public void setUsers(Collection<UserDTO> users) {
        this.users = users;
    }

    /**
     * @return When this content was generated
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }

}
