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
 * An interface that is responsible for returning rows of data
 */
public interface RowStream extends AutoCloseable {

    /**
     * Returns the next row of data as an object array, or <code>null</code> if the stream is out of data.
     * Note that the objects returned MUST adhere to the schema of the {@link ResettableDataSource} that created this RowStream.
     *
     * @return the next row of data, or <code>null</code> if there is no more data
     * @throws IOException if unable to obtain the next row of data due to I/O failure
     */
    Object[] nextRow() throws IOException;

}
