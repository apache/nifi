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
import org.apache.commons.lang3.StringUtils;

public enum JavaScriptScriptFactory {

    INSTANCE;

    private static final String PRELOADS = "var Scripting = JavaImporter(\n"
            + "        Packages.org.apache.nifi.components,\n"
            + "        Packages.org.apache.nifi.processor.util,\n"
            + "        Packages.org.apache.nifi.processor,\n"
            + "        Packages.org.apache.nifi.logging,\n"
            + "        Packages.org.apache.nifi.scripting,\n"
            + "        Packages.org.apache.commons.io);\n"
            + "var readFile = function (file) {\n"
            + "  var script = Packages.org.apache.commons.io.FileUtils.readFileToString("
            + "      new java.io.File($PATH, file)"
            + "    );\n"
            + "  return \"\" + script;\n"
            + "}\n"
            + "var require = function (file){\n"
            + "  var exports={}, module={};\n"
            + "  module.__defineGetter__('id', function(){return file;});"
            + "  eval(readFile(file));\n"
            + "  return exports;\n"
            + "}\n";

    public String getScript(File scriptFile) throws IOException {
        StringBuilder sb = new StringBuilder();
        final String parent = StringUtils.replace(scriptFile.getParent(), "\\", "/");
        sb.append(PRELOADS).append("var $PATH = \"").append(parent).append("\"\n")
                .append(FileUtils.readFileToString(scriptFile, "UTF-8"));
        return sb.toString();
    }
}
