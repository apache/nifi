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
package org.apache.nifi.script;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;

import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

/**
 * Utility methods and constants used by the scripting components.
 */
public class ScriptingComponentUtils {
    /** A relationship indicating flow files were processed successfully */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    /** A relationship indicating an error while processing flow files */
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be processed")
            .build();

    /** A property descriptor for specifying the location of a script file */
    public static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("Script File")
            .required(false)
            .description("Path to script file to execute. Only one of Script File or Script Body may be used")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    /** A property descriptor for specifying the body of a script */
    public static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder()
            .name("Script Body")
            .required(false)
            .description("Body of script to execute. Only one of Script File or Script Body may be used")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    /** A property descriptor for specifying the location of additional modules to be used by the script */
    public static final PropertyDescriptor MODULES = new PropertyDescriptor.Builder()
            .name("Module Directory")
            .description("Comma-separated list of paths to files and/or directories which contain modules required by the script.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
            .build();

    public static Collection<SearchResult> search(final SearchContext context, final ComponentLog logger) {
        final Collection<SearchResult> results = new ArrayList<>();

        final String term = context.getSearchTerm();

        final ResourceReference scriptFile = context.getProperty(ScriptingComponentUtils.SCRIPT_FILE).evaluateAttributeExpressions().asResource();
        String script = context.getProperty(ScriptingComponentUtils.SCRIPT_BODY).getValue();

        if (StringUtils.isBlank(script) && scriptFile == null) {
            return results;
        } else if (StringUtils.isBlank(script)) {
            try (final InputStream in = scriptFile.read()) {
                script = IOUtils.toString(in, StandardCharsets.UTF_8);
            } catch (Exception e) {
                logger.error(String.format("Could not read from path %s", scriptFile), e);
                return results;
            }
        }

        final Scanner scanner = new Scanner(script);
        int index = 1;

        while (scanner.hasNextLine()) {
            final String line = scanner.nextLine();
            if (StringUtils.containsIgnoreCase(line, term)) {
                final String text = String.format("Matched script at line %d: %s", index, line);
                results.add(new SearchResult.Builder().label(text).match(term).build());
            }

            index++;
        }

        return results;
    }

    public static Set<String> getAvailableEngines() {
        final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        final List<ScriptEngineFactory> scriptEngineFactories = scriptEngineManager.getEngineFactories();
        final Set<String> engines = new TreeSet<>();

        for (ScriptEngineFactory factory : scriptEngineFactories) {
            engines.add(factory.getLanguageName());
        }

        return engines;
    }
}

