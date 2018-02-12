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
package org.apache.nifi.toolkit.cli.api;

import java.io.IOException;
import java.io.PrintStream;

/**
 * A result that can be written to a PrintStream.
 *
 * @param <T> the type of result
 */
public interface WritableResult<T> extends Result<T> {

    /**
     * Writes this result to the given output stream.
     *
     * @param output the output stream
     * @throws IOException if an error occurs writing the result
     */
    void write(PrintStream output) throws IOException;
}
