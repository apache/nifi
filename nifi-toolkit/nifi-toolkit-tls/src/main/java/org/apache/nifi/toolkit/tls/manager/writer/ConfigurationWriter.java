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

package org.apache.nifi.toolkit.tls.manager.writer;

import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;

import java.io.IOException;

/**
 * Class that can write out configuration information on the given object
 *
 * @param <T> the type of Object to write information about
 */
public interface ConfigurationWriter<T> {
    /**
     * Writes configuration information about the given object
     *
     * @param t the object
     * @param outputStreamFactory an OutputStreamFactory
     * @throws IOException if there is an IO problem
     */
    void write(T t, OutputStreamFactory outputStreamFactory) throws IOException;
}
