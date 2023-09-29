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

package org.apache.nifi.sql;

import java.io.IOException;

/**
 * A Source of data for a database
 */
public interface ResettableDataSource {

    /**
     * @return the schema that the rows returned by the {@link RowStream} will adhere to
     */
    NiFiTableSchema getSchema();

    /**
     * Resets the data source and establishes a {@link RowStream} that can be used for iterating over data
     *
     * @return the newly established RowStream
     * @throws IOException if unable to obtain a RowStream due to IO failures
     */
    RowStream reset() throws IOException;
}
