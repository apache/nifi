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

public enum JythonScriptFactory {

    INSTANCE;

    private final static String PRELOADS = "from org.python.core.util import FileUtil\n"
            + "from org.apache.nifi.components import PropertyDescriptor\n"
            + "from org.apache.nifi.components import Validator\n"
            + "from org.apache.nifi.processor.util import StandardValidators\n"
            + "from org.apache.nifi.processor import Relationship\n"
            + "from org.apache.nifi.logging import ProcessorLog\n"
            + "from org.apache.nifi.scripting import ReaderScript\n"
            + "from org.apache.nifi.scripting import WriterScript\n"
            + "from org.apache.nifi.scripting import ConverterScript\n";

    public String getScript(File scriptFile) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(PRELOADS)
                .append(FileUtils.readFileToString(scriptFile, "UTF-8"));

        return sb.toString();
    }
}
