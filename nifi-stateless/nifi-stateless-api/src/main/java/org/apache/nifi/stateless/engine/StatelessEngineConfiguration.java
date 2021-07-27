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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.stateless.config.ExtensionClientDefinition;
import org.apache.nifi.stateless.config.SslContextDefinition;

import java.io.File;
import java.util.List;
import java.util.Optional;

public interface StatelessEngineConfiguration {
    File getWorkingDirectory();

    File getNarDirectory();

    File getExtensionsDirectory();

    File getKrb5File();

    /**
     * @return the directory to use for storing FlowFile Content, or an empty optional if content is to be stored in memory
     */
    Optional<File> getContentRepositoryDirectory();

    SslContextDefinition getSslContext();

    String getSensitivePropsKey();

    List<ExtensionClientDefinition> getExtensionClients();
}
