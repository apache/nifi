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
package org.apache.nifi.nar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Each processor, controller service, and reporting task will have an InstanceClassLoader.
 *
 * The InstanceClassLoader will either be an empty pass-through to the NARClassLoader, or will contain a
 * copy of all the NAR's resources in the case of components that @RequireInstanceClassLoading.
 */
public class InstanceClassLoader extends URLClassLoader {

    private static final Logger logger = LoggerFactory.getLogger(InstanceClassLoader.class);

    private final String identifier;
    private final String instanceType;

    private final Set<URL> instanceUrls;
    private final Set<URL> additionalResourceUrls;

    /**
     * @param identifier the id of the component this ClassLoader was created for
     * @param instanceUrls the urls for the instance, will either be empty or a copy of the NARs urls
     * @param additionalResourceUrls the urls that came from runtime properties of the component
     * @param parent the parent ClassLoader
     */
    public InstanceClassLoader(final String identifier, final String type, final Set<URL> instanceUrls, final Set<URL> additionalResourceUrls, final ClassLoader parent) {
        super(combineURLs(instanceUrls, additionalResourceUrls), parent);
        this.identifier = identifier;
        this.instanceType = type;
        this.instanceUrls = Collections.unmodifiableSet(
                instanceUrls == null ? Collections.emptySet() : new LinkedHashSet<>(instanceUrls));
        this.additionalResourceUrls = Collections.unmodifiableSet(
                additionalResourceUrls == null ? Collections.emptySet() : new LinkedHashSet<>(additionalResourceUrls));
    }

    private static URL[] combineURLs(final Set<URL> instanceUrls, final Set<URL> additionalResourceUrls) {
        final Set<URL> allUrls = new LinkedHashSet<>();

        if (instanceUrls != null) {
            allUrls.addAll(instanceUrls);
        }

        if (additionalResourceUrls != null) {
            allUrls.addAll(additionalResourceUrls);
        }

        return allUrls.toArray(new URL[allUrls.size()]);
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public Set<URL> getInstanceUrls() {
        return instanceUrls;
    }

    public Set<URL> getAdditionalResourceUrls() {
        return additionalResourceUrls;
    }
}
