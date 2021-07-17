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
package org.apache.nifi.web.search.query;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.groups.ProcessGroup;

/**
 * Service responsible to translating incoming user and contextual information.
 */
public interface SearchQueryParser {

    /**
     * Parses the incoming and contextual data and returns with a query object.
     *
     * @param searchLiteral The original search string provided by the user.
     * @param user The requesting user.
     * @param rootGroup The root process group of the flow.
     * @param activeGroup The process group was active for the user during the time of query.
     *
     * @return Returns a query object containing all the details needed for the search.
     */
    SearchQuery parse(String searchLiteral, NiFiUser user, ProcessGroup rootGroup, ProcessGroup activeGroup);
}
