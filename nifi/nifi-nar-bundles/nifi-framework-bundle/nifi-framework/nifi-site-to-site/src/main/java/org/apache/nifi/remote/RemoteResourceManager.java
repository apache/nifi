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
package org.apache.nifi.remote;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.protocol.ClientProtocol;
import org.apache.nifi.remote.protocol.ServerProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteResourceManager {

    private static final Map<String, Class<? extends FlowFileCodec>> codecClassMap;
    private static final Map<String, Class<? extends ServerProtocol>> desiredServerProtocolClassMap = new ConcurrentHashMap<>();
    private static final Map<String, Class<? extends ClientProtocol>> desiredClientProtocolClassMap = new ConcurrentHashMap<>();

    private static final Map<String, Set<Class<? extends ServerProtocol>>> serverProtocolClassMap;
    private static final Map<String, Set<Class<? extends ClientProtocol>>> clientProtocolClassMap;

    private static final Logger logger = LoggerFactory.getLogger(RemoteResourceManager.class);

    static {
        final Map<String, Class<? extends FlowFileCodec>> codecMap = new HashMap<>();
        final Map<String, Set<Class<? extends ServerProtocol>>> serverProtocolMap = new HashMap<>();
        final Map<String, Set<Class<? extends ClientProtocol>>> clientProtocolMap = new HashMap<>();

        // load all of the FlowFileCodecs that we know
        final ClassLoader classLoader = RemoteResourceManager.class.getClassLoader();
        final ServiceLoader<FlowFileCodec> flowFileCodecLoader = ServiceLoader.load(FlowFileCodec.class, classLoader);
        final Iterator<FlowFileCodec> codecItr = flowFileCodecLoader.iterator();
        while (codecItr.hasNext()) {
            final FlowFileCodec codec = codecItr.next();
            final Class<? extends FlowFileCodec> clazz = codec.getClass();
            final String codecName = codec.getResourceName();

            final Class<? extends FlowFileCodec> previousValue = codecMap.put(codecName, clazz);
            if (previousValue != null) {
                logger.warn("Multiple FlowFileCodec's found with name {}; choosing to use {} in place of {}",
                        new Object[]{codecName, clazz.getName(), previousValue.getName()});
            }
        }

        final ServiceLoader<ServerProtocol> serverProtocolLoader = ServiceLoader.load(ServerProtocol.class, classLoader);
        final Iterator<ServerProtocol> serverItr = serverProtocolLoader.iterator();
        while (serverItr.hasNext()) {
            final ServerProtocol protocol = serverItr.next();
            final Class<? extends ServerProtocol> clazz = protocol.getClass();
            final String protocolName = protocol.getResourceName();

            Set<Class<? extends ServerProtocol>> classSet = serverProtocolMap.get(protocolName);
            if (classSet == null) {
                classSet = new HashSet<>();
                serverProtocolMap.put(protocolName, classSet);
            }

            classSet.add(clazz);
        }

        final ServiceLoader<ClientProtocol> clientProtocolLoader = ServiceLoader.load(ClientProtocol.class, classLoader);
        final Iterator<ClientProtocol> clientItr = clientProtocolLoader.iterator();
        while (clientItr.hasNext()) {
            final ClientProtocol protocol = clientItr.next();
            final Class<? extends ClientProtocol> clazz = protocol.getClass();
            final String protocolName = protocol.getResourceName();

            Set<Class<? extends ClientProtocol>> classSet = clientProtocolMap.get(protocolName);
            if (classSet == null) {
                classSet = new HashSet<>();
                clientProtocolMap.put(protocolName, classSet);
            }

            classSet.add(clazz);
        }

        codecClassMap = Collections.unmodifiableMap(codecMap);
        clientProtocolClassMap = Collections.unmodifiableMap(clientProtocolMap);
        serverProtocolClassMap = Collections.unmodifiableMap(serverProtocolMap);
    }

    public static boolean isCodecSupported(final String codecName) {
        return codecClassMap.containsKey(codecName);
    }

    public static boolean isCodecSupported(final String codecName, final int version) {
        if (!isCodecSupported(codecName)) {
            return false;
        }

        final FlowFileCodec codec = createCodec(codecName);
        final VersionNegotiator negotiator = codec.getVersionNegotiator();
        return (negotiator.isVersionSupported(version));
    }

    public static FlowFileCodec createCodec(final String codecName, final int version) {
        final FlowFileCodec codec = createCodec(codecName);
        final VersionNegotiator negotiator = codec.getVersionNegotiator();
        if (!negotiator.isVersionSupported(version)) {
            throw new IllegalArgumentException("FlowFile Codec " + codecName + " does not support version " + version);
        }

        negotiator.setVersion(version);
        return codec;
    }

    private static FlowFileCodec createCodec(final String codecName) {
        final Class<? extends FlowFileCodec> codecClass = codecClassMap.get(codecName);
        if (codecClass == null) {
            throw new IllegalArgumentException("Unknown Codec: " + codecName);
        }

        try {
            return codecClass.newInstance();
        } catch (final Exception e) {
            throw new RuntimeException("Unable to instantiate class " + codecClass.getName(), e);
        }
    }

    public static Set<String> getSupportedCodecNames() {
        return codecClassMap.keySet();
    }

    public static List<Integer> getSupportedVersions(final String codecName) {
        final FlowFileCodec codec = createCodec(codecName);
        return codec.getSupportedVersions();
    }

    public static Set<Class<? extends ClientProtocol>> getClientProtocolClasses(final String protocolName) {
        final Set<Class<? extends ClientProtocol>> classes = clientProtocolClassMap.get(protocolName);
        if (classes == null) {
            return new HashSet<>();
        }
        return new HashSet<>(classes);
    }

    public static Set<Class<? extends ServerProtocol>> getServerProtocolClasses(final String protocolName) {
        final Set<Class<? extends ServerProtocol>> classes = serverProtocolClassMap.get(protocolName);
        if (classes == null) {
            return new HashSet<>();
        }
        return new HashSet<>(classes);
    }

    public static void setServerProtocolImplementation(final String protocolName, final Class<? extends ServerProtocol> clazz) {
        desiredServerProtocolClassMap.put(protocolName, clazz);
    }

    public static void setClientProtocolImplementation(final String protocolName, final Class<? extends ClientProtocol> clazz) {
        desiredClientProtocolClassMap.put(protocolName, clazz);
    }

    public static ServerProtocol createServerProtocol(final String protocolName) {
        final Set<Class<? extends ServerProtocol>> classSet = getServerProtocolClasses(protocolName);
        if (classSet.isEmpty()) {
            throw new IllegalArgumentException("Unknkown Server Protocol: " + protocolName);
        }

        Class<? extends ServerProtocol> desiredClass = desiredServerProtocolClassMap.get(protocolName);
        if (desiredClass == null && classSet.size() > 1) {
            throw new IllegalStateException("Multiple implementations of Server Protocol " + protocolName + " were found and no preferred implementation has been specified");
        }

        if (desiredClass != null && !classSet.contains(desiredClass)) {
            throw new IllegalStateException("Desired implementation of Server Protocol " + protocolName + " is set to " + desiredClass + ", but that Protocol is not registered as a Server Protocol");
        }

        if (desiredClass == null) {
            desiredClass = classSet.iterator().next();
        }

        try {
            return desiredClass.newInstance();
        } catch (final Exception e) {
            throw new RuntimeException("Unable to instantiate class " + desiredClass.getName(), e);
        }
    }

    public static ClientProtocol createClientProtocol(final String protocolName) {
        final Set<Class<? extends ClientProtocol>> classSet = getClientProtocolClasses(protocolName);
        if (classSet.isEmpty()) {
            throw new IllegalArgumentException("Unknkown Client Protocol: " + protocolName);
        }

        Class<? extends ClientProtocol> desiredClass = desiredClientProtocolClassMap.get(protocolName);
        if (desiredClass == null && classSet.size() > 1) {
            throw new IllegalStateException("Multiple implementations of Client Protocol " + protocolName + " were found and no preferred implementation has been specified");
        }

        if (desiredClass != null && !classSet.contains(desiredClass)) {
            throw new IllegalStateException("Desired implementation of Client Protocol " + protocolName + " is set to " + desiredClass + ", but that Protocol is not registered as a Client Protocol");
        }

        if (desiredClass == null) {
            desiredClass = classSet.iterator().next();
        }

        try {
            return desiredClass.newInstance();
        } catch (final Exception e) {
            throw new RuntimeException("Unable to instantiate class " + desiredClass.getName(), e);
        }
    }
}
