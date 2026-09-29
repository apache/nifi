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
package org.apache.nifi.dbcp.api;

import org.apache.nifi.controller.ControllerService;

/**
 * Supplies database credentials on-demand for Controller Services that establish JDBC connections.
 */
public interface DatabasePasswordProvider extends ControllerService {

    /**
     * Returns the JDBC property used to supply the credential when establishing a database connection.
     *
     * @return credential property used for connection attempts
     */
    default DatabaseCredentialProperty getDatabaseCredentialProperty() {
        return DatabaseCredentialProperty.PASSWORD;
    }

    /**
     * Returns credential characters to be used when establishing a database connection.
     *
     * @param requestContext context for the database credential request
     * @return credential characters
     */
    char[] getPassword(DatabasePasswordRequestContext requestContext);
}
