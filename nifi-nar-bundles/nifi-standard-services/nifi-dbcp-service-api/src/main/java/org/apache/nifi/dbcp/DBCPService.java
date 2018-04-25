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
package org.apache.nifi.dbcp;

import java.sql.Connection;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Definition for Database Connection Pooling Service.
 *
 */
@Tags({"dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service. Connections can be asked from pool and returned after usage.")
public interface DBCPService extends ControllerService {
    Connection getConnection() throws ProcessException;

    /**
     * Allows a Map of attributes to be passed to the DBCPService for use in configuration, etc.
     * An implementation will want to override getConnection() to return getConnection(Collections.emptyMap()),
     * and override this method (possibly with its existing getConnection() implementation).
     * @param attributes a Map of attributes to be passed to the DBCPService. The use of these
     *                   attributes is implementation-specific, and the source of the attributes
     *                   is processor-specific
     * @return a Connection from the specifed/configured connection pool(s)
     * @throws ProcessException if an error occurs while getting a connection
     */
    default Connection getConnection(Map<String,String> attributes) throws ProcessException {
        // default implementation (for backwards compatibility) is to call getConnection()
        // without attributes
        return getConnection();
    }
}
