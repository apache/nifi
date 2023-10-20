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
package org.apache.nifi.yaml;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.nifi.json.TokenParserFactory;

import java.io.IOException;
import java.io.InputStream;

public class YamlParserFactory implements TokenParserFactory {
    private static final YAMLFactory YAML_FACTORY = new YAMLFactory(new YAMLMapper());

    /**
     * Get Parser implementation for YAML
     *
     * @param in Input Stream to be parsed
     * @param streamReadConstraints Stream Read Constraints are not supported in YAML
     * @param allowComments Whether to allow comments when parsing does not apply to YAML
     * @return YAML Parser
     * @throws IOException Thrown on parser creation failures
     */
    @Override
    public JsonParser getJsonParser(final InputStream in, final StreamReadConstraints streamReadConstraints, final boolean allowComments) throws IOException {
        return YAML_FACTORY.createParser(in);
    }
}
