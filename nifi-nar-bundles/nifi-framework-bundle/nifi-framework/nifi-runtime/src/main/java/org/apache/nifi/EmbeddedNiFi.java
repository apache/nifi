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

package org.apache.nifi;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * <p>
 * Starts an instance of NiFi within the <b>same JVM</b>, which can later properly be shut down.
 * Intended to be used for testing purposes.</p>
 *
 */
public class EmbeddedNiFi extends NiFi {

    public EmbeddedNiFi(String[] args, ClassLoader rootClassLoader)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
            InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        super(convertArgumentsToValidatedNiFiProperties(args), rootClassLoader);
    }

    public EmbeddedNiFi(String[] args, ClassLoader rootClassLoader, ClassLoader bootstrapClassLoader)
        throws ClassNotFoundException, IOException, NoSuchMethodException,
        InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        super(convertArgumentsToValidatedNiFiProperties(args, bootstrapClassLoader), rootClassLoader);
    }

    @Override
    protected void initLogging() {
        // do nothing when running in embedded mode
    }

    @Override
    protected void setDefaultUncaughtExceptionHandler() {
        // do nothing when running in embedded mode
    }

    @Override
    protected void addShutdownHook() {
        // do nothing when running in embedded mode
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }
}
