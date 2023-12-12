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
package org.apache.nifi.questdb;

import java.io.Closeable;

/**
 * Provides access to database via distributing clients. Also responsible to ensure the health of the database connection
 * and database if possible.
 */
public interface DatabaseManager extends Closeable {
    /**
     * @return A client to execute queries against the managed database instance.
     */
    Client acquireClient();

    /**
     * Starts maintenance of the database. Necessary initialization step for proper use.
     */
    void init();

    /**
     * Finishes maintenance of the database. After calling, manager does not guarantee any connection with the database.
     */
    void close();
}
