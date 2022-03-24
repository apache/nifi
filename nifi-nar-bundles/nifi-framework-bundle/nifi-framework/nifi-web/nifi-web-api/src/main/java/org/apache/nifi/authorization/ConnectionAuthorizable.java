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

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.groups.ProcessGroup;

/**
 * Authorizable for a Connection and its Group, Source, and Destination.
 */
public interface ConnectionAuthorizable {
    /**
     * Returns the authorizable for this connection. Non null
     *
     * @return authorizable
     */
    Authorizable getAuthorizable();

    /**
     * Returns the source.
     *
     * @return source
     */
    Connectable getSource();

    /**
     * Returns the data authorizable for the source of the connection.
     *
     * @return source data authorizable
     */
    Authorizable getSourceData();

    /**
     * Returns the destination.
     *
     * @return destination
     */
    Connectable getDestination();

    /**
     * Returns the data authorizable for the destination of the connection.
     *
     * @return destination data authorizable
     */
    Authorizable getDestinationData();

    /**
     * Returns the parent process group.
     *
     * @return parent
     */
    ProcessGroup getParentGroup();
}
