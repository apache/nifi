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
package org.apache.nifi.attribute.expression.language.evaluation.functions;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.attribute.expression.language.ExpressionLanguageExecutable;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.util.DatatypeCast;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This evaluator provides capabilities to execute user defined functions.<br/>
 * For that, UDF should implement an {@link ExpressionLanguageExecutable}.
 */
public class ExecEvaluator extends StringEvaluator{

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecEvaluator.class);

    private static final NiFiProperties props = getProperties();

    private static ClassLoader udfClassLoader = getUDFClassLoader();

    private final List<Evaluator<?>> args;
    private final Map<String,ExpressionLanguageExecutable> cachedExecutors = new HashMap<>();

    public ExecEvaluator(final List<Evaluator<?>> list) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("Exec function should have at least two arguments -"
                    + " executor class name and a method name."
                    + " Refer to Expression Language documentation for more details on usage.");
        }
        this.args = list;
    }

    @Override
    public QueryResult<String> evaluate(Map<String, String> attributes) {
        String executorClassStr = args.get(0).evaluate(attributes).getValue().toString().trim();
        executorClassStr = executorClassStr == null || executorClassStr.isEmpty() ? null : executorClassStr;

        ExpressionLanguageExecutable executor = getExecutor(executorClassStr);

        final Pair<Class<?>[], Object[]> pair = getMethodArguments(executor, attributes);
        final Class<?>[] signatureArr = pair.getLeft();
        final Object[] objArr = pair.getRight();

        final Pair<Boolean, Method> method = findMethod(executor, signatureArr);

        Object result = null;

        try { //reflection is used to allow method overloading, so one class have multiple UDFs in the same domain.
            result = method.getRight().invoke(executor, method.getLeft()? new Object[] {objArr}: objArr);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new IllegalArgumentException("Method invocation failed for: " + method.getRight().toGenericString(), e);
        }

        return new StringQueryResult((result == null ? null : result.toString()));
    }

    private Pair<Boolean, Method> findMethod(ExpressionLanguageExecutable executor, Class<?>[] signatureArr) {
        Method method = null;
        Boolean isDefault = false;
        try {
            method = executor.getClass().getMethod("execute", signatureArr);
        } catch (NoSuchMethodException e) {
            //avoid this exception, as signature can have array of objects as input arguments;
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot use specified method: ", e);
        }
        if (method == null) {
            try {
                method = executor.getClass().getMethod("execute", Object[].class);
                isDefault = true;
            } catch(Exception e) {
                throw new IllegalArgumentException("Unexpected exception: ", e);
            }
        }
        return Pair.of(isDefault, method);
    }

    /**
     * Prepares two arrays: classes of objects for signature lookup, and objects themselves to be used for method invocation.
     *
     * @param executor - first arguments is always executor, need to keep it aside from the rest of the arguments.
     * @param attributes - flowfile attributes to be used to compute the values of evaluators (arguments)
     * @return array of classes for signature lookup, and array of objects to be passed during method invocation.
     */
    private Pair<Class<?>[], Object[]> getMethodArguments(ExpressionLanguageExecutable executor, Map<String, String> attributes) {
        List<Class<?>> lc = new LinkedList<>();
        List<Object> lo = new LinkedList<>();
        Set<Evaluator<?>> s = new HashSet<>();

        if (args.size() < 2) {
            // meaning, the method doesn't have any input parameters
            return Pair.of(new Class[] {}, new Object[] {});
        }

        Evaluator<?> eArg = args.get(0);
        s.add(eArg);

        args.subList(1, args.size()).forEach(eval -> {
            if (!s.contains(eval) && eval != eArg && !eval.equals(eArg)) {
                s.add(eval);
                Object eo = eval.evaluate(attributes).getValue();

                lo.add(eo);
                lc.add(DatatypeCast.resultType2Java(eval.getResultType()));
            }
        });

        return Pair.of(lc.toArray(new Class[lc.size()]), lo.toArray());
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }

    @SuppressWarnings("unchecked")
    private ExpressionLanguageExecutable getExecutor(String cName){
        ExpressionLanguageExecutable o = cachedExecutors.get(cName);

        if (o == null) {
            if (udfClassLoader == null) {
                throw new AttributeExpressionLanguageException("Cannot Load a class: " + cName +
                        ". Make sure property is defined and a directory with JARs exists.");
            }

            Class<ExpressionLanguageExecutable> c = null;
            try {
                c = (Class<ExpressionLanguageExecutable>) udfClassLoader.loadClass(cName);
            } catch (ClassNotFoundException | ClassCastException e) {
                throw new AttributeExpressionLanguageException("Cannot Load a class", e);
            }
            try {
                o = c.newInstance();
            }catch(Exception e) {
                throw new AttributeExpressionLanguageException("Cannot create new instance: ", e);
            }
            cachedExecutors.put(cName, o);
        }
        return o;
    }

    protected static ClassLoader getUDFClassLoader() {
        ClassLoader cl = null;
        String udfLoc = getUdfLocation();
        try {
            cl = ClassLoaderUtils.getCustomClassLoader(
                    udfLoc,
                    ExecEvaluator.class.getClassLoader(),
                    new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return name.endsWith(".jar");
                        }
                    });
        } catch (MalformedURLException e) {
            // do nothing. This can be due to absent/undefined dir with UDF JARs, that can be created later on.
            // there will be another attempt every time any UDF is being called.
            LOGGER.error(String.format("Cannot create class loader due to exception %s Make sure path exists: %s",
                    new Object[] {e.getMessage(), udfLoc}),
                    e);
        }
        return cl;
    }

    protected static String getUdfLocation() {
        String propLoc = props.getExpressionLangUDFPath();
        if (propLoc == null) {
            propLoc = "";
        }
        Path p = FileSystems.getDefault().getPath(System.getProperty("user.dir"), propLoc);
        return p.normalize().toString();
    }

    private static NiFiProperties getProperties() {
        return NiFiProperties.createBasicNiFiProperties(null, null);
    }

}
