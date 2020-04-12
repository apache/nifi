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
package org.apache.nifi.graph;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GremlinBytecodeClientService extends AbstractTinkerpopClientService implements GraphClientService {

    private static final List<PropertyDescriptor> NEW_DESCRIPTORS;

    public static final PropertyDescriptor ADDITIONAL_JARS = new PropertyDescriptor.Builder()
            .name("gremlin-bytecode-additional-jars")
            .displayName("Additional Jars")
            .description("Additional jars needed for the traversal to work go here.")
            .dynamicallyModifiesClasspath(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static {
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.addAll(DESCRIPTORS);
        _temp.add(ADDITIONAL_JARS);
        NEW_DESCRIPTORS = Collections.unmodifiableList(_temp);
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return NEW_DESCRIPTORS;
    }

    private ScriptEngineManager MANAGER = new ScriptEngineManager();
    private ScriptEngine engine;
    private Map<String, CompiledScript> compiledCode;
    private Cluster cluster;

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        cluster = buildCluster(context);

        compiledCode = new ConcurrentHashMap<>();
        engine = MANAGER.getEngineByName("groovy");
    }

    @OnDisabled
    public void shutdown() {
        try {
            compiledCode = null;
            engine = null;
            cluster.close();
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public Map<String, String> executeQuery(String s, Map<String, Object> map, GraphQueryResultCallback graphQueryResultCallback) {
        String hash = DigestUtils.md5Hex(s);
        CompiledScript compiled;
        GraphTraversalSource traversal;

        try {
            traversal = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster));
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        if (compiledCode.containsKey(hash)) {
            compiled = compiledCode.get(hash);
        } else {
            try {
                compiled = ((Compilable)engine).compile(s);
                compiledCode.put(s, compiled);
            } catch (ScriptException e) {
                throw new ProcessException(e);
            }
        }

        Bindings bindings = engine.createBindings();
        bindings.putAll(map);
        bindings.put("g", traversal);

        try {
            Object result = compiled.eval(bindings);
            if (result instanceof Map) {
                graphQueryResultCallback.process((Map<String, Object>)result, false);
            } else {
                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("result", result);
                graphQueryResultCallback.process(resultMap, false);
            }

            traversal.close();
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        return new HashMap<>();
    }

    @Override
    public String getTransitUrl() {
        return transitUrl;
    }
}
