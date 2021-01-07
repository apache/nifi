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

package org.apache.nifi.stateless.classloader;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class InstanceClassLoaderIT extends StatelessSystemIT {

    @Test
    public void testProcessorHasNarClassLoader() throws IOException, StatelessConfigurationException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor passThrough1 = builder.createSimpleProcessor("PassThrough");
        passThrough1.setAutoTerminatedRelationships(Collections.singleton("success"));

        final VersionedProcessor passThrough2 = builder.createSimpleProcessor("PassThrough");
        passThrough2.setAutoTerminatedRelationships(Collections.singleton("success"));

        // Create the flow
        final StatelessDataflow dataflow = loadDataflow(builder.getFlowSnapshot());
        final Set<Processor> processors = findProcessors(dataflow);
        assertEquals(2, processors.size());

        final Set<ClassLoader> classLoaders = new HashSet<>();
        for (final Processor processor : processors) {
            final ClassLoader classLoader = processor.getClass().getClassLoader();
            classLoaders.add(classLoader);
            assertEquals("org.apache.nifi.nar.NarClassLoader", classLoader.getClass().getName());
        }

        assertEquals(1, classLoaders.size());
    }

    @Test
    public void testProcessorHasInstanceClassLoader() throws IOException, StatelessConfigurationException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor passThrough1 = builder.createSimpleProcessor("PassThroughRequiresInstanceClassLoading");
        passThrough1.setAutoTerminatedRelationships(Collections.singleton("success"));

        final VersionedProcessor passThrough2 = builder.createSimpleProcessor("PassThroughRequiresInstanceClassLoading");
        passThrough2.setAutoTerminatedRelationships(Collections.singleton("success"));

        // Create the flow
        final StatelessDataflow dataflow = loadDataflow(builder.getFlowSnapshot());
        final Set<Processor> processors = findProcessors(dataflow);
        assertEquals(2, processors.size());

        final Set<ClassLoader> classLoaders = new HashSet<>();
        for (final Processor processor : processors) {
            final ClassLoader classLoader = processor.getClass().getClassLoader();
            classLoaders.add(classLoader);
            assertEquals("org.apache.nifi.nar.InstanceClassLoader", classLoader.getClass().getName());
        }

        assertEquals(2, classLoaders.size());

        final Set<ClassLoader> parentClassLoaders = classLoaders.stream()
            .map(ClassLoader::getParent)
            .collect(Collectors.toSet());

        assertEquals(1, parentClassLoaders.size());
    }

    private Set<Processor> findProcessors(final StatelessDataflow dataflow) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Method method = dataflow.getClass().getDeclaredMethod("findAllProcessors");
        method.setAccessible(true);
        return (Set<Processor>) method.invoke(dataflow);
    }

}
