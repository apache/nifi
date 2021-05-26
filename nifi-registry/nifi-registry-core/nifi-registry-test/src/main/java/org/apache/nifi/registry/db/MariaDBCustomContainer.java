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
package org.apache.nifi.registry.db;

import org.testcontainers.containers.MariaDBContainer;

/**
 * Custom container to override the JDBC URL and add additional query parameters.
 *
 * NOTE: At the time of implementing this, Flyway does not support some versions of MariaDB because the driver returns an unexpected DB name:
 *
 * https://github.com/flyway/flyway/issues/2339
 *
 * The work around is to add the useMysqlMetadata=true to the URL.
 */
public class MariaDBCustomContainer extends MariaDBContainer {

    public MariaDBCustomContainer(String dockerImageName) {
        super(dockerImageName);
    }

    @Override
    public String getJdbcUrl() {
        return super.getJdbcUrl() + "?useMysqlMetadata=true";
    }
}
