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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PropertiesVariableRegistry extends FileVariableRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesVariableRegistry.class);

    PropertiesVariableRegistry(File... files) throws IOException{
        super(files);
    }

    PropertiesVariableRegistry(Path... paths) throws IOException {
        super(paths);
    }

    PropertiesVariableRegistry(Properties...properties){
        super();
        addVariables(properties);
    }

    private void addVariables(Properties... properties){
        if(properties != null) {
            for (Properties props : properties) {
                addVariables(convertToMap(props));
            }
        }
    }

    @Override
    protected Map<String,String> convertFile(File file) throws IOException{

        if(file.exists()) {
            try (final InputStream inStream = new BufferedInputStream(new FileInputStream(file))) {
                Properties properties = new Properties();
                properties.load(inStream);
                return convertToMap(properties);
            }
        }else{
            LOG.warn("Could not add file " + file.getName() + ". file did not exist.");
            return null;
        }

    }

    private Map<String,String> convertToMap(Properties properties){
        HashMap<String,String> propertiesMap = new HashMap<>(properties.keySet().size());
        for(Object key:  properties.keySet()){
            propertiesMap.put((String)key,(String) properties.get(key));
        }
        return propertiesMap;
    }


}
