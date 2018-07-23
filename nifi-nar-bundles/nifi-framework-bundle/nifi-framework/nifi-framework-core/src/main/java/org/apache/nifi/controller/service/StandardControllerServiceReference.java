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
package org.apache.nifi.controller.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;

public class StandardControllerServiceReference implements ControllerServiceReference {

    private final ControllerServiceNode referenced;
    private final Set<ComponentNode> components;

    public StandardControllerServiceReference(final ControllerServiceNode referencedService, final Set<ComponentNode> referencingComponents) {
        this.referenced = referencedService;
        this.components = new HashSet<>(referencingComponents);
    }

    @Override
    public ControllerServiceNode getReferencedComponent() {
        return referenced;
    }

    @Override
    public Set<ComponentNode> getReferencingComponents() {
        return Collections.unmodifiableSet(components);
    }

    private boolean isRunning(final ComponentNode component) {
        if (component instanceof ReportingTaskNode) {
            return ((ReportingTaskNode) component).isRunning();
        }

        if (component instanceof ProcessorNode) {
            return ((ProcessorNode) component).isRunning();
        }

        if (component instanceof ControllerServiceNode) {
            return ((ControllerServiceNode) component).isActive();
        }

        return false;
    }

    @Override
    public Set<ComponentNode> getActiveReferences() {
        final Set<ComponentNode> activeReferences = new HashSet<>();

        for (final ComponentNode component : components) {
            if (isRunning(component)) {
                activeReferences.add(component);
            }
        }

        for (final ComponentNode component : findRecursiveReferences(ComponentNode.class)) {
            if (isRunning(component)) {
                activeReferences.add(component);
            }
        }

        return activeReferences;
    }


    @Override
    public <T> List<T> findRecursiveReferences(final Class<T> componentType) {
        return findRecursiveReferences(referenced, componentType);
    }

    private <T> List<T> findRecursiveReferences(final ControllerServiceNode referencedNode, final Class<T> componentType) {
        return findRecursiveReferences(referencedNode, componentType, new HashSet<>());
    }

    private <T> List<T> findRecursiveReferences(final ControllerServiceNode referencedNode, final Class<T> componentType, final Set<ControllerServiceNode> servicesVisited) {
        final List<T> references = new ArrayList<>();

        for (final ComponentNode referencingComponent : referencedNode.getReferences().getReferencingComponents()) {
            if (componentType.isAssignableFrom(referencingComponent.getClass())) {
                references.add(componentType.cast(referencingComponent));
            }

            if (referencingComponent instanceof ControllerServiceNode) {
                final ControllerServiceNode referencingNode = (ControllerServiceNode) referencingComponent;

                // find components recursively that depend on referencingNode.
                final boolean added = servicesVisited.add(referencingNode);
                if (added) {
                    final List<T> recursive = findRecursiveReferences(referencingNode, componentType, servicesVisited);

                    // For anything that depends on referencing node, we want to add it to the list, but we know
                    // that it must come after the referencing node, so we first remove any existing occurrence.
                    references.removeAll(recursive);
                    references.addAll(recursive);
                }
            }
        }

        return references;
    }

}
