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
package org.apache.nifi.authorization;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A group that users can belong to.
 */
public class Group {

    private final String identifier;

    private final String name;

    private final Set<String> users;

    /**
     * Constructs a new group with the given identifier and name.
     *
     * @param identifier a unique identifier for the group
     * @param name the name of the group
     */
    public Group(final String identifier, final String name) {
        this(identifier, name, null);
    }

    /**
     * Constructs a new group with the given identifier, name, and users.
     *
     * @param identifier a unique identifier for the group
     * @param name the name of the group
     * @param users the list of user identifiers that belong to this group
     */
    public Group(final String identifier, final String name, final Set<String> users) {
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Identifier can not be null or empty");
        }

        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Name can not be null or empty");
        }

        this.identifier = identifier;
        this.name = name;
        this.users = (users == null ? Collections.unmodifiableSet(new HashSet<>()) : Collections.unmodifiableSet(users));
    }

    /**
     * @return the identifier of the group
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * @return the name of the group
     */
    public String getName() {
        return name;
    }

    /**
     * @return the list of user identifiers that belong to this group
     */
    public Set<String> getUsers() {
        return users;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final Group other = (Group) obj;
        if (!Objects.equals(this.identifier, other.identifier)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.identifier);
        return hash;
    }

    @Override
    public String toString() {
        return String.format("identifier[%s], name[%s]", getIdentifier(), getName(), ", ");
    }

}
