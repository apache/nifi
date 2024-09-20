/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.ComponentNode;

import java.util.Set;
import java.util.function.Predicate;

class ComponentNodeDefinitionPredicate implements Predicate<ComponentNode> {

    private final Set<ExtensionDefinition> extensionDefinitions;

    public ComponentNodeDefinitionPredicate(final Set<ExtensionDefinition> extensionDefinitions) {
        this.extensionDefinitions = extensionDefinitions;
    }

    @Override
    public boolean test(final ComponentNode componentNode) {
        for (final ExtensionDefinition extensionDefinition : extensionDefinitions) {
            if (isComponentFromType(componentNode, extensionDefinition)) {
                return true;
            }
        }
        return false;
    }

    private <T extends ComponentNode> boolean isComponentFromType(final T componentNode, final ExtensionDefinition extensionDefinition) {
        final String componentClassName = componentNode.getCanonicalClassName();
        final BundleCoordinate componentCoordinate = componentNode.getBundleCoordinate();

        final String extensionDefinitionClassName = extensionDefinition.getImplementationClassName();
        final BundleCoordinate extensionDefinitionCoordinate = extensionDefinition.getBundle().getBundleDetails().getCoordinate();

        if (PythonBundle.isPythonCoordinate(componentCoordinate)) {
            final String componentType = componentNode.getComponentType();
            return componentType.equals(extensionDefinitionClassName) && componentCoordinate.equals(extensionDefinitionCoordinate);
        } else if (componentNode.isExtensionMissing()) {
            return componentClassName.equals(extensionDefinitionClassName)
                    && componentCoordinate.getGroup().equals(extensionDefinitionCoordinate.getGroup())
                    && componentCoordinate.getId().equals(extensionDefinitionCoordinate.getId());
        } else {
            return componentClassName.equals(extensionDefinitionClassName) && componentCoordinate.equals(extensionDefinitionCoordinate);
        }
    }
}
