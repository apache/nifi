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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.StringUtils;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.yaml.snakeyaml.Yaml;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.nifi.graph.GremlinClientService.NOT_SUPPORTED;

@CapabilityDescription("A client service that provides a scriptable interface to open a remote connection/travseral " +
        "against a Gremlin Server and execute operations against it.")
@Tags({ "graph", "database", "gremlin", "tinkerpop" })
public class GremlinBytecodeClientService extends AbstractTinkerpopClientService implements GraphClientService {
    private static final List<PropertyDescriptor> NEW_DESCRIPTORS;

    public static final PropertyDescriptor REMOTE_OBJECTS_STRING = new PropertyDescriptor.Builder()
            .name("remote-objects-string")
            .displayName("Remote Objects String")
            .description("The remote-objects string yaml used for connecting to the gremlin server. Only the yaml file or the string can be specified.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor REMOTE_OBJECTS_FILE = new PropertyDescriptor.Builder()
            .name("remote-objects-file")
            .displayName("Remote Objects File")
            .description("The remote-objects file yaml used for connecting to the gremlin server. Only the yaml file or the string can be specified.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor EXTRA_RESOURCE = new PropertyDescriptor.Builder()
            .name("extension")
            .displayName("Extension JARs")
            .description("A comma-separated list of Java JAR files to be loaded. This has no practical effect unless combined " +
                    "with the Extension Classes property which provides a list of particular classes to use as extensions for the " +
                    "parsing engine.")
            .defaultValue("")
            .addValidator(Validator.VALID)
            .required(false)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor EXTENSION_CLASSES = new PropertyDescriptor.Builder()
            .name("extension-classes")
            .displayName("Extension Classes")
            .addValidator(Validator.VALID)
            .defaultValue("")
            .description("A comma-separated list of fully qualified Java class names that correspond to classes to implement. This " +
                    "is useful for services such as JanusGraph that need specific serialization classes. "+
                    "This configuration property has no effect unless a value for the Extension JAR field is " +
                    "also provided.")
            .required(false)
            .build();

    public static final PropertyDescriptor TRAVERSAL_SOURCE_NAME = new PropertyDescriptor.Builder()
            .name("gremlin-traversal-source-name")
            .displayName("Traversal Source Name")
            .description("An optional property that lets you set the name of the remote traversal instance. " +
                    "This can be really important when working with databases like JanusGraph that support " +
                    "multiple backend traversal configurations simultaneously.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(Validator.VALID)
            .build();

    static {
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(TRAVERSAL_SOURCE_NAME);
        _temp.add(REMOTE_OBJECTS_STRING);
        _temp.add(REMOTE_OBJECTS_FILE);
        _temp.add(EXTRA_RESOURCE);
        _temp.add(EXTENSION_CLASSES);
        _temp.add(SSL_CONTEXT_SERVICE);
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
    private String traversalSourceName;
    private File configurationFile;
    private File yamlFile;
    private String remoteObjectString;
    private ConfigurationContext configurationContext;

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        String path = context.getProperty(EXTRA_RESOURCE).getValue();
        String classList = context.getProperty(EXTENSION_CLASSES).getValue();
        if (path != null && classList != null && !path.isEmpty() && !classList.isEmpty()) {
            try {
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                String[] classes = context.getProperty(EXTENSION_CLASSES).getValue().split(",[\\s]*");
                for (String cls : classes) {
                    Class clz = Class.forName(cls, true, loader);
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug(clz.getName());
                    }
                }
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }

        remoteObjectString = context.getProperty(REMOTE_OBJECTS_STRING).evaluateAttributeExpressions().getValue();

        compiledCode = new ConcurrentHashMap<>();
        engine = MANAGER.getEngineByName("groovy");

        if (context.getProperty(TRAVERSAL_SOURCE_NAME).isSet()) {
            traversalSourceName = context.getProperty(TRAVERSAL_SOURCE_NAME).evaluateAttributeExpressions()
                    .getValue();
        }

        configurationContext = context;

        createConfigurationFile();
        cluster = buildCluster(context);
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new HashSet<>();

        boolean jarsIsSet = !StringUtils.isEmpty(validationContext.getProperty(EXTRA_RESOURCE).getValue());
        boolean clzIsSet  = !StringUtils.isEmpty(validationContext.getProperty(EXTENSION_CLASSES).getValue());

        if (jarsIsSet && clzIsSet) {
            try {
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                String[] classes = validationContext.getProperty(EXTENSION_CLASSES).getValue().split(",[\\s]*");
                for (String clz : classes) {
                    loader.loadClass(clz);
                }
            } catch (Exception ex) {
                results.add(new ValidationResult.Builder().subject(EXTENSION_CLASSES.getDisplayName()).valid(false).explanation(ex.toString()).build());
            }
        }

        yamlFile = asFile(validationContext.getProperty(REMOTE_OBJECTS_FILE).evaluateAttributeExpressions().getValue());  //SCRIPT_FILE

        try {
            if(validationContext.getProperty(REMOTE_OBJECTS_FILE).isSet() && validationContext.getProperty(REMOTE_OBJECTS_STRING).isSet()) {
                throw new ProcessException(String.format("Only one parameter accepted: \"%s\" or \"%s\"", REMOTE_OBJECTS_FILE.getDisplayName(), REMOTE_OBJECTS_STRING.getDisplayName()));
            }
            if(!validationContext.getProperty(REMOTE_OBJECTS_FILE).isSet() && !validationContext.getProperty(REMOTE_OBJECTS_STRING).isSet()) {
                throw new ProcessException(String.format("At least one parameter required: \"%s\" or \"%s\"",REMOTE_OBJECTS_FILE.getDisplayName(),REMOTE_OBJECTS_STRING.getDisplayName()));
            }
            //Catching Throwable means we're starting to go down a rabbithole on error checking
        } catch (Exception t) {
            results.add(new ValidationResult.Builder().subject("Yaml File").input(yamlFile != null ? yamlFile.toString() : null).valid(false).explanation(t.toString()).build());
        }

        return results;
    }

    @Override
    protected Cluster buildCluster(ConfigurationContext context) {
        Cluster.Builder builder;
        try {
            if (configurationFile == null) {
                createConfigurationFile();
            }
            builder = Cluster.build(configurationFile);
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }
        Map<String, Object> configuration;
        try (FileInputStream is = new FileInputStream(configurationFile)){
            //This is because there appears to be an error in the cluster.allHosts() method
            Yaml yaml = new Yaml();
            configuration = (Map) yaml.load(is);
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }

        builder = setupSSL(context, builder);

        Cluster temp = builder.create();
        transitUrl = String.format("gremlin%s://%s:%s%s", usesSSL ? "+ssl" : "",
                configuration.get("hosts"), temp.getPort(), temp.getPath());

        return temp;
    }

    @Override
    public Map<String, String> executeQuery(String s, Map<String, Object> map, GraphQueryResultCallback graphQueryResultCallback) {
        try {
            return doQuery(s, map, graphQueryResultCallback);
        } catch (Exception ex) {
            if (cluster != null) {
                cluster.close();
                cluster = null;
            }
            cluster = buildCluster(configurationContext);
            return doQuery(s, map, graphQueryResultCallback);
        }
    }

    @Override
    public String getTransitUrl() {
        return transitUrl;
    }

    private File asFile(String f) {
        if (f == null || f.length() == 0) {
            return null;
        }
        return new File(f);
    }

    private Map<String, String> doQuery(String s, Map<String, Object> map, GraphQueryResultCallback graphQueryResultCallback) {
        String hash = DigestUtils.md5Hex(s);
        CompiledScript compiled;
        GraphTraversalSource traversal;

        traversal = createTraversal();
        int rowsReturned = 0;

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

        if (getLogger().isDebugEnabled()) {
            getLogger().debug(map.toString());
        }

        Bindings bindings = engine.createBindings();
        bindings.putAll(map);
        bindings.put("g", traversal);
        try {
            Object result = compiled.eval(bindings);
            if (result instanceof Map) {
                Map<String, Object> resultMap = (Map<String, Object>) result;
                if (!resultMap.isEmpty()) {
                    Iterator outerResultSet = resultMap.entrySet().iterator();
                    while(outerResultSet.hasNext()) {
                        Map.Entry<String, Object> innerResultSet = (Map.Entry<String, Object>) outerResultSet.next();
                        if (innerResultSet.getValue() instanceof Map) {
                            Iterator resultSet = ((Map) innerResultSet.getValue()).entrySet().iterator();
                            while (resultSet.hasNext()) {
                                Map.Entry<String, Object> tempResult = (Map.Entry<String, Object>) resultSet.next();
                                Map<String, Object> tempRetObject = new HashMap<>();
                                tempRetObject.put(tempResult.getKey(), tempResult.getValue());
                                GremlinBytecodeClientService.SimpleEntry returnObject = new GremlinBytecodeClientService.SimpleEntry<String, Object>(tempResult.getKey(), tempRetObject);
                                Map<String, Object> resultReturnMap = new HashMap<>();
                                resultReturnMap.put(innerResultSet.getKey(), returnObject);
                                if (getLogger().isDebugEnabled()) {
                                    getLogger().debug(resultReturnMap.toString());
                                }
                                graphQueryResultCallback.process(resultReturnMap, resultSet.hasNext());
                            }
                        } else {
                            Map<String, Object> resultReturnMap = new HashMap<>();
                            resultReturnMap.put(innerResultSet.getKey(), innerResultSet.getValue());
                            graphQueryResultCallback.process(resultReturnMap, false);
                        }
                        rowsReturned++;
                    }

                }
            }
            traversal.close();
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        Map<String, String> resultAttributes = new HashMap<>();
        resultAttributes.put(NODES_CREATED, NOT_SUPPORTED);
        resultAttributes.put(RELATIONS_CREATED, NOT_SUPPORTED);
        resultAttributes.put(LABELS_ADDED, NOT_SUPPORTED);
        resultAttributes.put(NODES_DELETED, NOT_SUPPORTED);
        resultAttributes.put(RELATIONS_DELETED, NOT_SUPPORTED);
        resultAttributes.put(PROPERTIES_SET, NOT_SUPPORTED);
        resultAttributes.put(ROWS_RETURNED, String.valueOf(rowsReturned));

        return resultAttributes;
    }

    protected GraphTraversalSource createTraversal() {
        GraphTraversalSource traversal;
        try {
            if (StringUtils.isEmpty(traversalSourceName)) {
                traversal = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster));
            } else {
                traversal = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster, traversalSourceName));
            }
        } catch (Exception e) {
            throw new ProcessException(e);
        }
        return traversal;
    }

    protected void createConfigurationFile() {
        try {
            if (yamlFile != null) {
                configurationFile = yamlFile;
            } else {
                configurationFile = File.createTempFile("remote-object", ".yaml");
                BufferedWriter outFile = new BufferedWriter(new FileWriter(configurationFile));
                outFile.write(remoteObjectString);
                outFile.close();
            }
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }

    }

    final class SimpleEntry<K, V> implements Map.Entry<K, V> {
        private final K key;
        private V value;

        public SimpleEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V old = this.value;
            this.value = value;
            return old;
        }

        @Override
        public String toString() {
            return String.format("%s:%s", this.key, this.value);
        }

    }
}
