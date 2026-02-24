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

package org.apache.nifi.c2.client.service;

import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.PersistentUuidGenerator;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.AgentInfo;
import org.apache.nifi.c2.protocol.api.AgentManifest;
import org.apache.nifi.c2.protocol.api.AgentRepositories;
import org.apache.nifi.c2.protocol.api.AgentResourceConsumption;
import org.apache.nifi.c2.protocol.api.AgentStatus;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.DeviceInfo;
import org.apache.nifi.c2.protocol.api.FlowInfo;
import org.apache.nifi.c2.protocol.api.NetworkInfo;
import org.apache.nifi.c2.protocol.api.ResourceInfo;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.apache.nifi.c2.protocol.api.SupportedOperation;
import org.apache.nifi.c2.protocol.api.SystemInfo;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.net.NetworkInterface.getNetworkInterfaces;
import static java.util.Collections.list;
import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static java.util.Map.entry;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class C2HeartbeatFactory {

    private static final Logger logger = LoggerFactory.getLogger(C2HeartbeatFactory.class);

    private static final String AGENT_IDENTIFIER_FILENAME = "agent-identifier";
    private static final String DEVICE_IDENTIFIER_FILENAME = "device-identifier";

    private final C2ClientConfig clientConfig;
    private final FlowIdHolder flowIdHolder;
    private final ManifestHashProvider manifestHashProvider;
    private final Supplier<ResourcesGlobalHash> resourcesGlobalHashSupplier;

    private String agentId;
    private String deviceId;
    private File confDirectory;

    public C2HeartbeatFactory(final C2ClientConfig clientConfig, final FlowIdHolder flowIdHolder, final ManifestHashProvider manifestHashProvider,
                              final Supplier<ResourcesGlobalHash> resourcesGlobalHashSupplier) {
        this.clientConfig = clientConfig;
        this.flowIdHolder = flowIdHolder;
        this.manifestHashProvider = manifestHashProvider;
        this.resourcesGlobalHashSupplier = resourcesGlobalHashSupplier;
    }

    public C2Heartbeat create(final RuntimeInfoWrapper runtimeInfoWrapper) {
        final C2Heartbeat heartbeat = new C2Heartbeat();

        heartbeat.setAgentInfo(getAgentInfo(runtimeInfoWrapper.getAgentRepositories(), runtimeInfoWrapper.getManifest()));
        heartbeat.setDeviceInfo(generateDeviceInfo());
        heartbeat.setFlowInfo(getFlowInfo(runtimeInfoWrapper));
        heartbeat.setCreated(System.currentTimeMillis());

        final ResourceInfo resourceInfo = new ResourceInfo();
        resourceInfo.setHash(resourcesGlobalHashSupplier.get().getDigest());
        heartbeat.setResourceInfo(resourceInfo);

        return heartbeat;
    }

    private FlowInfo getFlowInfo(final RuntimeInfoWrapper runtimeInfoWrapper) {
        final FlowInfo flowInfo = new FlowInfo();
        flowInfo.setQueues(runtimeInfoWrapper.getQueueStatus());
        flowInfo.setProcessorBulletins(runtimeInfoWrapper.getProcessorBulletins());
        flowInfo.setProcessorStatuses(runtimeInfoWrapper.getProcessorStatus());
        flowInfo.setRunStatus(runtimeInfoWrapper.getRunStatus());
        Optional.ofNullable(flowIdHolder.getFlowId()).ifPresent(flowInfo::setFlowId);
        return flowInfo;
    }

    private AgentInfo getAgentInfo(final AgentRepositories repos, final RuntimeManifest manifest) {
        final AgentInfo agentInfo = new AgentInfo();
        agentInfo.setAgentClass(clientConfig.getAgentClass());
        agentInfo.setIdentifier(getAgentId());

        final AgentStatus agentStatus = new AgentStatus();
        agentStatus.setUptime(ManagementFactory.getRuntimeMXBean().getUptime());
        agentStatus.setRepositories(repos);
        final AgentResourceConsumption agentResourceConsumption = new AgentResourceConsumption();
        agentResourceConsumption.setMemoryUsage(Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory());
        agentStatus.setResourceConsumption(agentResourceConsumption);
        agentInfo.setStatus(agentStatus);
        agentInfo.setAgentManifestHash(manifestHashProvider.calculateManifestHash(manifest.getBundles(), getSupportedOperations(manifest)));

        if (clientConfig.isFullHeartbeat()) {
            agentInfo.setAgentManifest(manifest);
        }

        return agentInfo;
    }

    private Set<SupportedOperation> getSupportedOperations(final RuntimeManifest manifest) {
        final Set<SupportedOperation> supportedOperations;
        // supported operations has value only in case of minifi, therefore we return empty collection if
        if (manifest instanceof AgentManifest) {
            supportedOperations = ((AgentManifest) manifest).getSupportedOperations();
        } else {
            supportedOperations = Collections.emptySet();
        }
        return supportedOperations;
    }

    private String getAgentId() {
        if (agentId == null) {
            final String rawAgentId = clientConfig.getAgentIdentifier();
            if (isNotBlank(rawAgentId)) {
                agentId = rawAgentId.trim();
            } else {
                final File idFile = new File(getConfDirectory(), AGENT_IDENTIFIER_FILENAME);
                agentId = new PersistentUuidGenerator(idFile).generate();
            }
        }

        return agentId;
    }

    private DeviceInfo generateDeviceInfo() {
        final DeviceInfo deviceInfo = new DeviceInfo();
        deviceInfo.setNetworkInfo(generateNetworkInfo());
        deviceInfo.setIdentifier(getDeviceIdentifier(deviceInfo.getNetworkInfo()));
        deviceInfo.setSystemInfo(generateSystemInfo());
        return deviceInfo;
    }

    private NetworkInfo generateNetworkInfo() {
        try {
            final Set<NetworkInterface> eligibleInterfaces = list(getNetworkInterfaces())
                .stream()
                .filter(this::isEligibleInterface)
                .collect(toSet());

            if (logger.isTraceEnabled()) {
                logger.trace("Found {} eligible interfaces with names {}", eligibleInterfaces.size(),
                    eligibleInterfaces.stream()
                        .map(NetworkInterface::getName)
                        .collect(toSet())
                );
            }

            final Comparator<Map.Entry<NetworkInterface, InetAddress>> orderByIp4AddressesFirst = comparingInt(item -> item.getValue() instanceof Inet4Address ? 0 : 1);
            final Comparator<Map.Entry<NetworkInterface, InetAddress>> orderByNetworkInterfaceName = comparing(entry -> entry.getKey().getName());
            return eligibleInterfaces.stream()
                .flatMap(networkInterface -> list(networkInterface.getInetAddresses())
                    .stream()
                    .map(inetAddress -> entry(networkInterface, inetAddress)))
                .sorted(orderByIp4AddressesFirst.thenComparing(orderByNetworkInterfaceName))
                .findFirst()
                .map(entry -> createNetworkInfo(entry.getKey(), entry.getValue()))
                .orElseGet(NetworkInfo::new);
        } catch (final Exception e) {
            logger.error("Network Interface processing failed", e);
            return new NetworkInfo();
        }
    }

    private boolean isEligibleInterface(final NetworkInterface networkInterface) {
        try {
            return !networkInterface.isLoopback()
                && !networkInterface.isVirtual()
                && networkInterface.isUp()
                && nonNull(networkInterface.getHardwareAddress());
        } catch (final SocketException e) {
            logger.warn("Error processing network interface", e);
            return false;
        }
    }

    private NetworkInfo createNetworkInfo(final NetworkInterface networkInterface, final InetAddress inetAddress) {
        final NetworkInfo networkInfo = new NetworkInfo();
        networkInfo.setDeviceId(networkInterface.getName());
        networkInfo.setHostname(inetAddress.getHostName());
        networkInfo.setIpAddress(inetAddress.getHostAddress());
        return networkInfo;
    }

    private String getDeviceIdentifier(final NetworkInfo networkInfo) {
        if (deviceId == null) {
            if (networkInfo.getDeviceId() != null) {
                try {
                    final NetworkInterface netInterface = NetworkInterface.getByName(networkInfo.getDeviceId());
                    final byte[] hardwareAddress = netInterface.getHardwareAddress();
                    final StringBuilder macBuilder = new StringBuilder();
                    if (hardwareAddress != null) {
                        for (final byte address : hardwareAddress) {
                            macBuilder.append(String.format("%02X", address));
                        }
                    }
                    deviceId = macBuilder.toString();
                } catch (final Exception e) {
                    logger.warn("Could not determine device identifier.  Generating a unique ID", e);
                    deviceId = getConfiguredDeviceId();
                }
            } else {
                deviceId = getConfiguredDeviceId();
            }
        }
        return deviceId;
    }

    private String getConfiguredDeviceId() {
        final File idFile = new File(getConfDirectory(), DEVICE_IDENTIFIER_FILENAME);
        return new PersistentUuidGenerator(idFile).generate();
    }

    private SystemInfo generateSystemInfo() {
        final SystemInfo systemInfo = new SystemInfo();

        final OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        systemInfo.setMachineArch(osMXBean.getArch());
        systemInfo.setOperatingSystem(osMXBean.getName());

        final double systemLoadAverage = osMXBean.getSystemLoadAverage();
        systemInfo.setvCores(osMXBean.getAvailableProcessors());

        // getSystemLoadAverage is not available in Windows, so we need to prevent to send invalid data
        if (systemLoadAverage >= 0) {
            systemInfo.setCpuLoadAverage(systemLoadAverage);
        }

        return systemInfo;
    }

    private File getConfDirectory() {
        if (confDirectory == null) {
            final String configDirectoryName = clientConfig.getConfDirectory();
            final File configDirectory = new File(configDirectoryName);
            if (!configDirectory.exists() || !configDirectory.isDirectory()) {
                throw new IllegalStateException("Specified conf directory " + configDirectoryName + " does not exist or is not a directory.");
            }

            confDirectory = configDirectory;
        }

        return confDirectory;
    }

}
