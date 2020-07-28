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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Each processor, controller service, and reporting task will have an InstanceClassLoader.
 *
 * The InstanceClassLoader will either be an empty pass-through to the NARClassLoader, or will contain a
 * copy of all the NAR's resources in the case of components that @RequireInstanceClassLoading.
 */
public class InstanceClassLoader extends AbstractNativeLibHandlingClassLoader {

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
        this(identifier, type, instanceUrls, additionalResourceUrls, Collections.emptySet(), parent);
    }

    public InstanceClassLoader(
            final String identifier,
            final String type,
            final Set<URL> instanceUrls,
            final Set<URL> additionalResourceUrls,
            final Set<File> narNativeLibDirs,
            final ClassLoader parent
    ) {
        super(combineURLs(instanceUrls, additionalResourceUrls), parent, initNativeLibDirList(narNativeLibDirs, additionalResourceUrls), identifier);
        this.identifier = identifier;
        this.instanceType = type;
        this.instanceUrls = Collections.unmodifiableSet(
                instanceUrls == null ? Collections.emptySet() : new LinkedHashSet<>(instanceUrls));
        this.additionalResourceUrls = Collections.unmodifiableSet(
                additionalResourceUrls == null ? Collections.emptySet() : new LinkedHashSet<>(additionalResourceUrls));
    }

    /**
     * Note: Normally URLClassLoader will only load resources that are inside JARs, or in directories, but many times we allow
     * properties to specify specific files to add to the classpath. This allows those files to be found by checking the known
     * URLs of the InstanceClassLoader, when the resource wasn't find in the parent hierarchy.
     */
    @Override
    public URL findResource(String name) {
        URL resourceUrl = super.findResource(name);

        if (resourceUrl == null) {
            resourceUrl = findResource(instanceUrls, name);
        }

        if (resourceUrl == null) {
            resourceUrl = findResource(additionalResourceUrls, name);
        }

        return resourceUrl;
    }

    private URL findResource(final Set<URL> urls, final String name) {
        if (urls == null || name == null) {
            return null;
        }

        for (final URL url : urls) {
            try {
                final URI uri = url.toURI();
                final File file = new File(uri);
                if (name.equals(file.getName())) {
                    logger.debug("Found resource '" + name + "' from URL '" + url.toExternalForm() + "'");
                    return url;
                }
            } catch (URISyntaxException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        }

        return null;
    }

    private static List<File> initNativeLibDirList(Set<File> narNativeLibDirs, Set<URL> additionalResourceUrls) {
        List<File> nativeLibDirList = new ArrayList<>(narNativeLibDirs);

        Set<File> additionalNativeLibDirs = new HashSet<>();
        if (additionalResourceUrls != null) {
            for (URL url : additionalResourceUrls) {
                File file;

                try {
                    file = new File(url.toURI());
                } catch (URISyntaxException e) {
                    file = new File(url.getPath());
                } catch (Exception e) {
                    logger.error("Couldn't convert url '" + url + "' to a file");
                    file = null;
                }

                File dir = toDir(file);
                if (dir != null) {
                    additionalNativeLibDirs.add(dir);
                }
            }
        }

        nativeLibDirList.addAll(additionalNativeLibDirs);

        return nativeLibDirList;
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
