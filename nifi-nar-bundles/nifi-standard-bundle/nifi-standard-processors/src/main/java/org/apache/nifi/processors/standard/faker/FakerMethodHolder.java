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
package org.apache.nifi.processors.standard.faker;

import java.lang.reflect.Method;

// This class holds references to objects in order to programmatically make calls to Faker objects to generate random data
public class FakerMethodHolder {
    private final String propertyName;
    private final Object methodObject;
    private final Method method;

    public FakerMethodHolder(final String propertyName, final Object methodObject, final Method method) {
        this.propertyName = propertyName;
        this.methodObject = methodObject;
        this.method = method;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Object getMethodObject() {
        return methodObject;
    }

    public Method getMethod() {
        return method;
    }
}