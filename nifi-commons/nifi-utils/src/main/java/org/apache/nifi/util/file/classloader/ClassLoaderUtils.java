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
package org.apache.nifi.util.file.classloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ClassLoaderUtils {

    static final Logger LOGGER = LoggerFactory.getLogger(ClassLoaderUtils.class);

    public static ClassLoader getCustomClassLoader(String modulePath, ClassLoader parentClassLoader, FilenameFilter filenameFilter) throws MalformedURLException {
        URL[] classpaths = getURLsForClasspath(modulePath, filenameFilter, false);
        return createModuleClassLoader(classpaths, parentClassLoader);
    }

    /**
     *
     * @param modulePath a module path to get URLs from, the module path may be
     * a comma-separated list of paths
     * @param filenameFilter a filter to apply when a module path is a directory
     * and performs a listing, a null filter will return all matches
     * @param suppressExceptions indicates whether to suppress exceptions
     * @return an array of URL instances representing all of the modules
     * resolved from processing modulePath
     * @throws MalformedURLException if a module path does not exist
     */
    public static URL[] getURLsForClasspath(String modulePath, FilenameFilter filenameFilter, boolean suppressExceptions) throws MalformedURLException {
        return getURLsForClasspath(modulePath == null ? Collections.emptySet() : Collections.singleton(modulePath), filenameFilter, suppressExceptions);
    }

    /**
     *
     * @param modulePaths one or modules paths to get URLs from, each module
     * path may be a comma-separated list of paths
     * @param filenameFilter a filter to apply when a module path is a directory
     * and performs a listing, a null filter will return all matches
     * @param suppressExceptions if true then all modules will attempt to be
     * resolved even if some throw an exception, if false the first exception
     * will be thrown
     * @return an array of URL instances representing all of the modules
     * resolved from processing modulePaths
     * @throws MalformedURLException if a module path does not exist
     */
    public static URL[] getURLsForClasspath(Set<String> modulePaths, FilenameFilter filenameFilter, boolean suppressExceptions) throws MalformedURLException {
        // use LinkedHashSet to maintain the ordering that the incoming paths are processed
        Set<String> modules = new LinkedHashSet<>();
        if (modulePaths != null) {
            modulePaths.stream()
                    .flatMap(path -> Arrays.stream(path.split(",")))
                    .filter(path -> isNotBlank(path))
                    .map(String::trim)
                    .forEach(m -> modules.add(m));
        }
        return toURLs(modules, filenameFilter, suppressExceptions);
    }

    private static boolean isNotBlank(final String value) {
        return value != null && !value.trim().isEmpty();
    }

    protected static URL[] toURLs(Set<String> modulePaths, FilenameFilter filenameFilter, boolean suppressExceptions) throws MalformedURLException {
        List<URL> additionalClasspath = new LinkedList<>();
        if (modulePaths != null) {
            for (String modulePathString : modulePaths) {
                // If the path is already a URL, just add it (but don't check if it exists, too expensive and subject to network availability)
                boolean isUrl = true;
                try {
                    additionalClasspath.add(new URL(modulePathString));
                } catch (MalformedURLException mue) {
                    isUrl = false;
                }
                if (!isUrl) {
                    try {
                        File modulePath = new File(modulePathString);

                        if (modulePath.exists()) {

                            additionalClasspath.add(modulePath.toURI().toURL());

                            if (modulePath.isDirectory()) {
                                File[] files = modulePath.listFiles(filenameFilter);

                                if (files != null) {
                                    for (File classpathResource : files) {
                                        if (classpathResource.isDirectory()) {
                                            LOGGER.warn("Recursive directories are not supported, skipping " + classpathResource.getAbsolutePath());
                                        } else {
                                            additionalClasspath.add(classpathResource.toURI().toURL());
                                        }
                                    }
                                }
                            }
                        } else {
                            throw new MalformedURLException("Path specified does not exist");
                        }
                    } catch (MalformedURLException e) {
                        if (!suppressExceptions) {
                            throw e;
                        }
                    }
                }
            }
        }
        return additionalClasspath.toArray(new URL[additionalClasspath.size()]);
    }

    public static String generateAdditionalUrlsFingerprint(Set<URL> urls) {
        List<String> listOfUrls = urls.stream().map(Object::toString).collect(Collectors.toList());
        StringBuffer urlBuffer = new StringBuffer();

        //Sorting so that the order is maintained for generating the fingerprint
        Collections.sort(listOfUrls);
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            listOfUrls.forEach(url -> {
                urlBuffer.append(url).append("-").append(getLastModified(url)).append(";");
            });
            byte[] bytesOfAdditionalUrls = urlBuffer.toString().getBytes("UTF-8");
            byte[] bytesOfDigest = md.digest(bytesOfAdditionalUrls);

            return DatatypeConverter.printHexBinary(bytesOfDigest);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            LOGGER.error("Unable to generate fingerprint for the provided additional resources {}", new Object[]{urls, e});
            return null;
        }
    }

    private static long getLastModified(String url) {
        File file = null;
        try {
            file = new File(new URI(url));
        } catch (URISyntaxException e) {
            LOGGER.error("Error getting last modified date for " + url);
        }
        return file != null ? file.lastModified() : 0;
    }

    protected static ClassLoader createModuleClassLoader(URL[] modules, ClassLoader parentClassLoader) {
        return new URLClassLoader(modules, parentClassLoader);
    }

}
