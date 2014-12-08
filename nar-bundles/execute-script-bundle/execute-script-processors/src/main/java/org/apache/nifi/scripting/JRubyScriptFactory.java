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
package org.apache.nifi.scripting;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public enum JRubyScriptFactory {

    INSTANCE;

    private static final String PRELOADS = "include Java\n"
            + "\n"
            + "java_import 'org.apache.nifi.components.PropertyDescriptor'\n"
            + "java_import 'org.apache.nifi.components.Validator'\n"
            + "java_import 'org.apache.nifi.processor.util.StandardValidators'\n"
            + "java_import 'org.apache.nifi.processor.Relationship'\n"
            + "java_import 'org.apache.nifi.logging.ProcessorLog'\n"
            + "java_import 'org.apache.nifi.scripting.ReaderScript'\n"
            + "java_import 'org.apache.nifi.scripting.WriterScript'\n"
            + "java_import 'org.apache.nifi.scripting.ConverterScript'\n"
            + "\n";

    public String getScript(File scriptFile) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(PRELOADS)
                .append(FileUtils.readFileToString(scriptFile, "UTF-8"));
        return sb.toString();
    }
}
