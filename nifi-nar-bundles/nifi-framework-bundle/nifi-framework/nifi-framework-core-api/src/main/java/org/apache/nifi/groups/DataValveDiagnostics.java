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

package org.apache.nifi.groups;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DataValveDiagnostics {

    /**
     * @return the set of Process Groups for which data is currently flowing in
     */
    Set<ProcessGroup> getGroupsWithDataFlowingIn();

    /**
     * @return the set of Process Groups for which data is currently flowing out
     */
    Set<ProcessGroup> getGroupsWithDataFlowingOut();

    /**
     * @return a Mapping of reason that data cannot flow into a Process Group to Process Groups that are affected by the reason
     */
    Map<String, List<ProcessGroup>> getReasonForInputNotAllowed();

    /**
     * @return a Mapping of reason that data cannot flow out of a Process Group to Process Groups that are affected by the reason
     */
    Map<String, List<ProcessGroup>> getReasonForOutputNotAllowed();

}
