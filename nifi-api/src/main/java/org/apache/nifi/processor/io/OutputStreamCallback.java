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
package org.apache.nifi.processor.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public interface OutputStreamCallback {

    /**
     * Provides a managed output stream for use. The input stream is
     * automatically opened and closed though it is ok to close the stream
     * manually - and quite important if any streams wrapping these streams open
     * resources which should be cleared.
     *
     * @param out the stream to write bytes to
     * @throws IOException if issues writing to output stream
     */
    void process(OutputStream out) throws IOException;

}
