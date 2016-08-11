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

package org.apache.nifi.toolkit.tls;

import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.service.client.TlsCertificateAuthorityClientCommandLine;
import org.apache.nifi.toolkit.tls.service.server.TlsCertificateAuthorityServiceCommandLine;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Command line entry point
 */
public class TlsToolkitMain {
    public static final String DESCRIPTION = "DESCRIPTION";
    public static final String UNABLE_TO_GET_DESCRIPTION = "Unable to get description. (";
    private final Map<String, Class<?>> mainMap;

    public TlsToolkitMain() {
        mainMap = new LinkedHashMap<>();
        mainMap.put("standalone", TlsToolkitStandaloneCommandLine.class);
        mainMap.put("server", TlsCertificateAuthorityServiceCommandLine.class);
        mainMap.put("client", TlsCertificateAuthorityClientCommandLine.class);
    }

    public static void main(String[] args) {
        new TlsToolkitMain().doMain(args);
    }

    private <T> T printUsageAndExit(String message, ExitCode exitCode) {
        System.out.println(message);
        System.out.println();
        System.out.println("Usage: tls-toolkit service [-h] [args]");
        System.out.println();
        System.out.println("Services:");
        mainMap.forEach((s, aClass) -> System.out.println("   " + s + ": " + getDescription(aClass)));
        System.out.println();
        System.exit(exitCode.ordinal());
        return null;
    }

    protected String getDescription(Class<?> clazz) {
        try {
            Field declaredField = clazz.getDeclaredField(DESCRIPTION);
            return String.valueOf(declaredField.get(null));
        } catch (Exception e) {
            return UNABLE_TO_GET_DESCRIPTION + e.getMessage() + ")";
        }
    }

    protected Map<String, Class<?>> getMainMap() {
        return mainMap;
    }

    protected Method getMain(String service) {
        Class<?> mainClass = mainMap.get(service);
        if (mainClass == null) {
            printUsageAndExit("Unknown service: " + service, ExitCode.INVALID_ARGS);
        }

        try {
            return mainClass.getDeclaredMethod("main", String[].class);
        } catch (NoSuchMethodException e) {
            return printUsageAndExit("Service " + service + " is missing main method.", ExitCode.SERVICE_ERROR);
        }
    }

    public void doMain(String[] args) {
        if (args.length < 1) {
            printUsageAndExit("Expected at least a service argument.", ExitCode.INVALID_ARGS);
        }

        String service = args[0].toLowerCase();

        try {
            getMain(service).invoke(null, (Object) args);
        } catch (IllegalAccessException e) {
            printUsageAndExit("Service " + service + " has invalid main method.", ExitCode.SERVICE_ERROR);
        } catch (InvocationTargetException e) {
            printUsageAndExit("Service " + service + " error: " + e.getCause().getMessage(), ExitCode.SERVICE_ERROR);
        }
        return;
    }
}