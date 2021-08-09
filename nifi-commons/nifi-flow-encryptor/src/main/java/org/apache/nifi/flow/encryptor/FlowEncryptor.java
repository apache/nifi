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
package org.apache.nifi.flow.encryptor;

import org.apache.nifi.encrypt.PropertyEncryptor;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Flow Encryptor for reading a Flow Configuration and writing a new Flow Configuration using a new password
 */
public interface FlowEncryptor {
    /**
     * Process Flow Configuration Stream
     *
     * @param inputStream Flow Configuration Input Stream
     * @param outputStream Flow Configuration Output Stream encrypted using new password
     * @param inputEncryptor Property Encryptor for Input Configuration
     * @param outputEncryptor Property Encryptor for Output Configuration
     */
    void processFlow(InputStream inputStream, OutputStream outputStream, PropertyEncryptor inputEncryptor, PropertyEncryptor outputEncryptor);
}
