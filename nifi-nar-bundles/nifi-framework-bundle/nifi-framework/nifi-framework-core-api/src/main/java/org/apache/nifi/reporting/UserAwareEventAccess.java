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
package org.apache.nifi.reporting;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.status.ProcessGroupStatus;

public interface UserAwareEventAccess extends EventAccess {
    /**
     * Returns the status for components in the specified group. This request is
     * made by the specified user so the results will be filtered accordingly.
     *
     * @param groupId group id
     * @param user user making request
     * @return the component status
     */
    ProcessGroupStatus getGroupStatus(String groupId, NiFiUser user, int recursiveStatusDepth);


    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is made by the specified user so the
     * results will be filtered accordingly.
     *
     * @param groupId group id
     * @param statusReport report
     * @param user user making request
     * @return the component status
     */
    ProcessGroupStatus getGroupStatus(String groupId, RepositoryStatusReport statusReport, NiFiUser user);

    /**
     * Returns the status for components in the specified group. This request is
     * made by the specified user so the results will be filtered accordingly.
     *
     * @param groupId group id
     * @param user user making request
     * @return the component status
     */
    ProcessGroupStatus getGroupStatus(String groupId, NiFiUser user);

    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is made by the specified user so the
     * results will be filtered accordingly.
     *
     * @param groupId group id
     * @param statusReport report
     * @param user user making request
     * @param recursiveStatusDepth the number of levels deep we should recurse and still include the the processors' statuses, the groups' statuses, etc. in the returned ProcessGroupStatus
     * @return the component status
     */
    ProcessGroupStatus getGroupStatus(String groupId, RepositoryStatusReport statusReport, NiFiUser user, int recursiveStatusDepth);
}
