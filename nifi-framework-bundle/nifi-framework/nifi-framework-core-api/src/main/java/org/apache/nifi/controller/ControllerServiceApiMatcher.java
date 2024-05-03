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
package org.apache.nifi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

public class ControllerServiceApiMatcher {

    private static Logger LOGGER = LoggerFactory.getLogger(ControllerServiceApiMatcher.class);

    /**
     * Determines if all of the methods from the API class are present in the implementation class.
     *
     * @param serviceApi the controller service API class
     * @param serviceImplementation the controller service implementation class
     * @return true if all API methods exists in the implementation with the same name, parameters, and return type
     */
    public boolean matches(Class<? extends ControllerService> serviceApi, Class<? extends ControllerService> serviceImplementation) {
        for (final Method apiMethod : serviceApi.getMethods()) {
            boolean foundMatchingImplMethod = false;
            for (final Method implMethod : serviceImplementation.getMethods()) {
                if (!apiMethod.getName().equals(implMethod.getName())) {
                    continue;
                }

                // if the service interface has new methods that the implementation doesn't implement,
                // those methods still show up list of methods when calling impl getMethods(), but they
                // are marked as abstract and will produce an AbstractMethodError at runtime if invoked
                if (Modifier.isAbstract(implMethod.getModifiers())) {
                    continue;
                }

                final boolean returnTypeMatches = apiMethod.getReturnType().equals(implMethod.getReturnType());
                final boolean argsMatch = Arrays.equals(apiMethod.getParameterTypes(), implMethod.getParameterTypes());

                if (returnTypeMatches && argsMatch) {
                    foundMatchingImplMethod = true;
                    break;
                }
            }

            if (!foundMatchingImplMethod) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{} does not implement the API method [{}] from {}",
                            new Object[]{serviceImplementation.getCanonicalName(), apiMethod.toString(), serviceApi.getCanonicalName()});
                }
                return false;
            }
        }

        return true;
    }

}
