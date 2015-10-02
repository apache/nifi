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
package org.apache.nifi.hbase.io;

import org.apache.nifi.hbase.scan.ResultCell;

import java.io.IOException;
import java.io.OutputStream;

public interface RowSerializer {

    /**
     * Serializes the given row and cells to the provided OutputStream
     *
     * @param rowKey the row's key
     * @param cells the cells to serialize
     * @param out the OutputStream to serialize to
     * @throws IOException if unable to serialize the row
     */
    void serialize(byte[] rowKey, ResultCell[] cells, OutputStream out) throws IOException;
}
