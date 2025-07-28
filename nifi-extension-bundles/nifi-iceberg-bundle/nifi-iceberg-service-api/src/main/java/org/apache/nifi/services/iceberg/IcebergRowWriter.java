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
package org.apache.nifi.services.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;

import java.io.Closeable;
import java.io.IOException;

/**
 * Iceberg Row Writer abstraction based on org.apache.iceberg.io.TaskWriter avoid iceberg-io dependency
 */
public interface IcebergRowWriter extends Closeable {
    /**
     * Write Row to Data Files
     *
     * @param row Row Object to be written
     * @throws IOException Thrown on write failures
     */
    void write(Record row) throws IOException;

    /**
     * Close Writer and delete completed files
     *
     * @throws IOException Thrown on delete or close failures
     */
    void abort() throws IOException;

    /**
     * Close the writer and get completed Iceberg Data Files containing rows written
     *
     * @return Array of Iceberg Data Files
     * @throws IOException Thrown on close failures
     */
    DataFile[] dataFiles() throws IOException;
}
