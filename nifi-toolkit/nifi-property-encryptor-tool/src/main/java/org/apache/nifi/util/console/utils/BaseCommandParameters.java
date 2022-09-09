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
package org.apache.nifi.util.console.utils;

import picocli.CommandLine;

import java.nio.file.Path;

public class BaseCommandParameters {
    protected static final String DECRYPT = "decrypt";
    protected static final String ENCRYPT = "encrypt";
    protected static final String MIGRATE = "migrate";
    protected static final String RUN_LOG_MESSAGE = "The property encryptor is running to [{}] configuration files in [{}]";

    @CommandLine.Parameters(description="The base directory of NiFi/NiFi Registry/MiNiFi which contains the 'conf' directory (eg. /var/lib/nifi)")
    protected Path baseDirectory;

    @CommandLine.Parameters(description="A plaintext passphrase (12 character minimum) used to derive a root key (the 'bootstrap' key) and subsequently encrypt files")
    protected String rootPassphrase;
}
