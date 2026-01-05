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

package org.apache.nifi.jolt.util;

import io.joltcommunity.jolt.CardinalityTransform;
import io.joltcommunity.jolt.Chainr;
import io.joltcommunity.jolt.Defaultr;
import io.joltcommunity.jolt.JoltTransform;
import io.joltcommunity.jolt.Modifier;
import io.joltcommunity.jolt.Shiftr;
import io.joltcommunity.jolt.Sortr;
import io.joltcommunity.jolt.SpecDriven;
import io.joltcommunity.jolt.chainr.spec.ChainrEntry;
import io.joltcommunity.jolt.exception.SpecException;
import io.joltcommunity.jolt.removr.Removr;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransformFactory {

    public static JoltTransform getTransform(final ClassLoader classLoader, final String transformType, final Object specJson) throws Exception {
        return switch (JoltTransformStrategy.get(transformType)) {
            case DEFAULTR -> new Defaultr(specJson);
            case SHIFTR -> new Shiftr(specJson);
            case REMOVR -> new Removr(specJson);
            case CARDINALITY -> new CardinalityTransform(specJson);
            case SORTR -> new Sortr();
            case MODIFIER_DEFAULTR -> new Modifier.Defaultr(specJson);
            case MODIFIER_OVERWRITER -> new Modifier.Overwritr(specJson);
            case MODIFIER_DEFINER -> new Modifier.Definr(specJson);
            case CHAINR -> new Chainr(getChainrJoltTransformations(classLoader, specJson));
            default -> null;
        };
    }

    public static JoltTransform getCustomTransform(final ClassLoader classLoader, final String customTransformType, final Object specJson) throws Exception {
        final Class<?> clazz = classLoader.loadClass(customTransformType);
        if (SpecDriven.class.isAssignableFrom(clazz)) {
            final Constructor<?> constructor = clazz.getConstructor(Object.class);
            return (JoltTransform) constructor.newInstance(specJson);
        } else {
            return (JoltTransform) clazz.getDeclaredConstructor().newInstance();
        }
    }


    protected static List<JoltTransform> getChainrJoltTransformations(final ClassLoader classLoader, final Object specJson) throws Exception {
        if (!(specJson instanceof List)) {
            throw new SpecException("JOLT Chainr expects a JSON array of objects - Malformed spec.");
        } else {
            final List<?> operations = (List<?>) specJson;

            if (operations.isEmpty()) {
                throw new SpecException("JOLT Chainr passed an empty JSON array.");
            } else {
                final List<JoltTransform> entries = new ArrayList<>(operations.size());

                for (final Object chainrEntryObj : operations) {
                    if (!(chainrEntryObj instanceof Map)) {
                        throw new SpecException("JOLT ChainrEntry expects a JSON map - Malformed spec");
                    }

                    final Map<?, ?> chainrEntryMap = (Map<?, ?>) chainrEntryObj;
                    final String opString = (String) chainrEntryMap.get("operation");
                    final String operationClassName;

                    if (opString == null) {
                        throw new SpecException("JOLT Chainr 'operation' must implement Transform or ContextualTransform");
                    } else {
                        operationClassName = ChainrEntry.STOCK_TRANSFORMS.getOrDefault(opString, opString);
                        entries.add(getCustomTransform(classLoader, operationClassName, chainrEntryMap.get("spec")));
                    }
                }

                return entries;
            }
        }
    }

}
