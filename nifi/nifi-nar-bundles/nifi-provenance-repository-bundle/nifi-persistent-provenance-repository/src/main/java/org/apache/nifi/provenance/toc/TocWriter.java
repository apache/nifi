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
package org.apache.nifi.provenance.toc;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Writes a .toc file
 */
public interface TocWriter extends Closeable {

    /**
     * Adds the given block offset as the next Block Offset in the Table of Contents
     * @param offset
     * @throws IOException
     */
    void addBlockOffset(long offset) throws IOException;
    
    /**
     * Returns the index of the current Block
     * @return
     */
    int getCurrentBlockIndex();
    
    /**
     * Returns the file that is currently being written to
     * @return
     */
    File getFile();

    /**
     * Synchronizes the data with the underlying storage device
     * @throws IOException
     */
    void sync() throws IOException;
}
