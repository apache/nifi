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
package org.apache.nifi.fn;

import org.apache.nifi.nar.NarManifestEntry;
import org.apache.nifi.nar.NarUnpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class NiFiFn {
    private static final Logger logger = LoggerFactory.getLogger(NiFiFn.class);

    private static final String FN_CORE_NAR_ID = "nifi-fn-nar";

    public static final String PROGRAM_CLASS_NAME = "org.apache.nifi.fn.runtimes.Program";

    public static final String RUN_FROM_REGISTRY = "RunFromRegistry";
    public static final String RUN_YARN_SERVICE_FROM_REGISTRY = "RunYARNServiceFromRegistry";
    public static final String RUN_OPENWHISK_ACTION_SERVER = "RunOpenwhiskActionServer";

    public static final String REGISTRY = "nifi_registry";
    public static final String BUCKETID = "nifi_bucket";
    public static final String FLOWID = "nifi_flow";
    public static final String CONTENT = "nifi_content";

    public static void main(final String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (args.length < 4) {
            printUsage();
            return;
        }

        final File libDir = new File(args[0]);
        if (!libDir.exists()) {
            System.out.println("Specified lib directory <" + libDir + "> does not exist");
            return;
        }

        final File[] jarFiles = libDir.listFiles(file -> file.getName().endsWith(".jar"));
        if (jarFiles == null) {
            System.out.println("Could not obtain listing of NiFi-FN Lib directory <" + libDir + ">");
            return;
        }

        final URL[] jarUrls = toURLs(jarFiles);

        final URLClassLoader rootClassLoader = new URLClassLoader(jarUrls);

        final File[] narFiles = libDir.listFiles(file -> file.getName().endsWith(".nar"));
        if (narFiles == null) {
            System.out.println("Could not obtain listing of NiFi-FN lib directory <" + libDir + ">");
            return;
        }

        final File narWorkingDirectory = new File(args[1]);
        if (!narWorkingDirectory.exists() && !narWorkingDirectory.mkdirs()) {
            throw new IOException("Could not create NAR Working Directory <" + narWorkingDirectory + ">");
        }

        File fnCoreWorkingDirectory = null;
        logger.info("Unpacking {} NARs", narFiles.length);
        final long startUnpack = System.nanoTime();
        for (final File narFile : narFiles) {
            final File unpackedDirectory = NarUnpacker.unpackNar(narFile, narWorkingDirectory);
            if (isFnCoreNar(narFile)) {
                fnCoreWorkingDirectory = unpackedDirectory;
            }
        }
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startUnpack);
        logger.info("Finished unpacking {} NARs in {} millis", narFiles.length, millis);


        if (fnCoreWorkingDirectory == null) {
            throw new FileNotFoundException("Could not find NiFi FN core NAR in the lib directory <" + libDir + ">");
        }

        final File bundledDependenciesDir = new File(fnCoreWorkingDirectory, NarUnpacker.BUNDLED_DEPENDENCIES_DIRECTORY);
        final File[] fnCoreFiles = bundledDependenciesDir.listFiles();
        if (fnCoreFiles == null) {
            throw new IOException("Could not obtain listing of NiFi-FN NAR's bundled dependencies in working directory <" + bundledDependenciesDir + ">");
        }
        final URL[] fnCoreUrls = toURLs(fnCoreFiles);

        final URLClassLoader fnCoreClassLoader = new URLClassLoader(fnCoreUrls, rootClassLoader);
        Thread.currentThread().setContextClassLoader(fnCoreClassLoader);

        final Class<?> programClass = Class.forName(PROGRAM_CLASS_NAME, true, fnCoreClassLoader);
        final Method launchMethod = programClass.getMethod("launch", String[].class, ClassLoader.class, File.class);
        launchMethod.setAccessible(true);

        final String[] shiftedArgs = Arrays.copyOfRange(args, 2, args.length);
        launchMethod.invoke(null, shiftedArgs, rootClassLoader, narWorkingDirectory);
    }

    private static URL[] toURLs(final File[] files) throws MalformedURLException {
        final List<URL> urls = new ArrayList<>();
        for (final File file : files) {
            urls.add(file.toURI().toURL());
        }

        return urls.toArray(new URL[0]);
    }

    private static boolean isFnCoreNar(final File file) throws IOException {
        final String filename = file.getName();
        if (filename.startsWith("nifi-fn") && filename.endsWith(".nar")) {
            try (final JarFile jarFile = new JarFile(file)) {
                final Manifest manifest = jarFile.getManifest();

                final Attributes attributes = manifest.getMainAttributes();
                final String narId = attributes.getValue(NarManifestEntry.NAR_ID.getManifestName());

                return FN_CORE_NAR_ID.equals(narId);
            }
        }

        return false;
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("    1) " + RUN_FROM_REGISTRY + " <Lib Directory> <NAR Working Directory> [Once|Continuous] <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> " +
            "[<Failure Output Ports>] [<Input FlowFile>]");
        System.out.println("       " + RUN_FROM_REGISTRY + " <Lib Directory> <NAR Working Directory> [Once|Continuous] --json <JSON>");
        System.out.println("       " + RUN_FROM_REGISTRY + " <Lib Directory> <NAR Working Directory> [Once|Continuous] --file <File Name>");
        System.out.println();
        System.out.println("    2) " + RUN_YARN_SERVICE_FROM_REGISTRY + "        <Lib Directory> <NAR Working Directory> <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> \\");
        System.out.println("                                               <Lib Directory> <NAR Working Directory> <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> "
            + "[<Failure Output Ports>] [<Input FlowFile>]");
        System.out.println("       " + RUN_YARN_SERVICE_FROM_REGISTRY + "        <Lib Directory> <NAR Working Directory> <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> "
            + "--json <JSON>");
        System.out.println("       " + RUN_YARN_SERVICE_FROM_REGISTRY + "        <Lib Directory> <NAR Working Directory> <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> "
            + "--file <File Name>");
        System.out.println();
        System.out.println("    3) " + RUN_OPENWHISK_ACTION_SERVER + "          <Lib Directory> <NAR Working Directory> <Port>");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("    1) " + RUN_FROM_REGISTRY + " ./lib ./work Once http://172.0.0.1:61080 e53b8a0d-5c85-4fcd-912a-1c549a586c83 6cf8277a-c402-4957-8623-0fa9890dd45d \\");
        System.out.println("             \"DestinationDirectory-/tmp/nififn/output2/\" \"\" \"absolute.path-/tmp/nififn/input/;filename-test.txt\" \"absolute.path-/tmp/nififn/input/;" +
            "filename-test2.txt\"");
        System.out.println("    2) " + RUN_FROM_REGISTRY + " ./lib ./work Once http://172.0.0.1:61080 e53b8a0d-5c85-4fcd-912a-1c549a586c83 6cf8277a-c402-4957-8623-0fa9890dd45d \\");
        System.out.println("             \"DestinationDirectory-/tmp/nififn/output2/\" \"f25c9204-6c95-3aa9-b0a8-c556f5f61849\" \"absolute.path-/tmp/nififn/input/;filename-test.txt\"");
        System.out.println("    3) " + RUN_YARN_SERVICE_FROM_REGISTRY + " ./lib ./work http://127.0.0.1:8088 nifi-fn:latest kafka-to-solr 3 --file kafka-to-solr.json");
        System.out.println("    4) " + RUN_OPENWHISK_ACTION_SERVER + " ./lib ./work 8080");
        System.out.println();
        System.out.println("Notes:");
        System.out.println("    1) <Input Variables> will be split on ';' and '-' then injected into the flow using the variable registry interface.");
        System.out.println("    2) <Failure Output Ports> will be split on ';'. FlowFiles routed to matching output ports will immediately fail the flow.");
        System.out.println("    3) <Input FlowFile> will be split on ';' and '-' then injected into the flow using the \"" + CONTENT + "\" field as the FlowFile content.");
        System.out.println("    4) Multiple <Input FlowFile> arguments can be provided.");
        System.out.println("    5) The configuration file must be in JSON format. ");
        System.out.println("    6) When providing configurations via JSON, the following attributes must be provided: " + REGISTRY + ", " + BUCKETID + ", " + FLOWID + ".");
        System.out.println("          All other attributes will be passed to the flow using the variable registry interface");
        System.out.println();
    }
}
