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
package org.apache.nifi.toolkit.encryptconfig

import org.apache.nifi.properties.ConfigEncryptionTool
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class LegacyMode extends ConfigEncryptionTool implements ToolMode {

    private static final Logger logger = LoggerFactory.getLogger(LegacyMode.class)

    private static final MODE_NAME = "legacy"

    @Override
    String getModeName() {
        return MODE_NAME
    }

    @Override
    String getModeDescription() {
        return "A backwards-compatible mode for protecting sensitive values in NiFi config files by encrypting them with a master key."
    }

    @Override
    void run(String[] args) {

        // does --nifi-registy exist?


        logger.info("Invoking legacy command line tool.")
        ConfigEncryptionTool.main(args)
    }

    boolean matchesArgs(String[] args) {
        try {
            def parseResult = parse(args)
            return parseResult != null
        } catch (Throwable t) {
            return false
        }
    }
}
