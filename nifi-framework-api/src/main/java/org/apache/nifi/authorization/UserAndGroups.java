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

import java.util.Set;

/**
 * A holder object to provide atomic access to a user and their groups.
 */
public interface UserAndGroups {

    /**
     * Retrieves the user, or null if the user is unknown
     *
     * @return the user with the given identity
     */
    User getUser();

    /**
     * Retrieves the groups for the user, or null if the user is unknown or has no groups.
     *
     * @return the set of groups for the given user identity
     */
    Set<Group> getGroups();

}
