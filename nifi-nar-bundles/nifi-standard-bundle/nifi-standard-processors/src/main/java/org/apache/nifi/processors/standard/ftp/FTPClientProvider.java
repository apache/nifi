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
package org.apache.nifi.processors.standard.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.nifi.context.PropertyContext;

import java.util.Map;

/**
 * FTP Client Provider for abstracting initial connection configuration of FTP Client instances
 */
public interface FTPClientProvider {
    /**
     * Get configured FTP Client using context properties and attributes
     *
     * @param context Property Context
     * @param attributes FlowFile attributes for property expression evaluation
     * @return Configured FTP Client
     */
    FTPClient getClient(PropertyContext context, final Map<String, String> attributes);
}
