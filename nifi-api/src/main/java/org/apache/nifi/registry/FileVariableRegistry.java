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

package org.apache.nifi.registry;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;


abstract class FileVariableRegistry extends MultiMapVariableRegistry {

    FileVariableRegistry() {
        super();
    }

    FileVariableRegistry(File... files) throws IOException{
        super();
        addVariables(files);
    }

    FileVariableRegistry(Path... paths) throws IOException{
        super();
        addVariables(paths);
    }

    private void addVariables(File ...files) throws IOException{
        if(files != null) {
            for (final File file : files) {
                Map<String,String> map = convertFile(file);
                if(map != null) {
                    registry.addMap(convertFile(file));
                }
            }

        }
    }

    private void addVariables(Path ...paths) throws IOException{
        if(paths != null) {
            for (final Path path : paths) {
                Map<String,String> map = convertFile(path.toFile());
                if(map != null) {
                    registry.addMap(map);
                }
            }
        }
    }

    protected abstract Map<String,String> convertFile(File file) throws IOException;

}
