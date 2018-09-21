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
package org.apache.nifi.web.server;

import com.google.common.base.Strings;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.util.InetAddressUtils;
import org.apache.nifi.util.NiFiProperties;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ScopedHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class HostHeaderHandler extends ScopedHandler {
    private static final Logger logger = LoggerFactory.getLogger(HostHeaderHandler.class);

    private final String serverName;
    private final int serverPort;
    private final List<String> validHosts;

    /**
     * Instantiates a handler with a given server name and port 0.
     *
     * @param serverName the {@code serverName} to set on the request (the {@code serverPort} will not be set)
     * @deprecated Use {@link #HostHeaderHandler(NiFiProperties)} which accepts a {@link NiFiProperties} object to allow for custom network interface binding.
     */
    public HostHeaderHandler(String serverName) {
        this(serverName, 0);
    }

    /**
     * Instantiates a handler with a given server name and port.
     *
     * @param serverName the {@code serverName} to set on the request
     * @param serverPort the {@code serverPort} to set on the request
     * @deprecated Use {@link #HostHeaderHandler(NiFiProperties)} which accepts a {@link NiFiProperties} object to allow for custom network interface binding.
     */
    public HostHeaderHandler(String serverName, int serverPort) {
        this.serverName = Objects.requireNonNull(serverName);
        this.serverPort = serverPort;

        validHosts = generateDefaultHostnames(null);
        validHosts.add(serverName.toLowerCase());
        validHosts.add(serverName.toLowerCase() + ":" + serverPort);
        // Sometimes the hostname is left empty but the port is always populated
        validHosts.add("localhost");
        validHosts.add("localhost:" + serverPort);
        // Different from customizer -- empty is ok here
        validHosts.add("");
        try {
            validHosts.add(InetAddress.getLocalHost().getHostName().toLowerCase());
            validHosts.add(InetAddress.getLocalHost().getHostName().toLowerCase() + ":" + serverPort);
        } catch (final Exception e) {
            logger.warn("Failed to determine local hostname.", e);
        }

        logger.info("Created " + this.toString());
    }

    /**
     * Instantiates a handler which accepts incoming requests with a host header that is empty or contains one of the
     * valid hosts. See the Apache NiFi Admin Guide for instructions on how to set valid hostnames and IP addresses.
     *
     * @param niFiProperties the NiFiProperties
     */
    public HostHeaderHandler(NiFiProperties niFiProperties) {
        this.serverName = Objects.requireNonNull(determineServerHostname(niFiProperties));
        this.serverPort = determineServerPort(niFiProperties);

        // Default values across generic instances
        List<String> hosts = generateDefaultHostnames(niFiProperties);

        // The value from nifi.web.http|https.host
        hosts.add(serverName.toLowerCase());
        hosts.add(serverName.toLowerCase() + ":" + serverPort);

        // The value(s) from nifi.web.proxy.host
        hosts.addAll(parseCustomHostnames(niFiProperties));

        // empty is ok here
        hosts.add("");

        this.validHosts = uniqueList(hosts);
        logger.info("Determined {} valid hostnames and IP addresses for incoming headers: {}", new Object[]{validHosts.size(), StringUtils.join(validHosts, ", ")});

        logger.debug("Created " + this.toString());
    }

    /**
     * Returns the list of parsed custom hostnames from {@code nifi.web.proxy.host} in {@link NiFiProperties}.
     * This list is deduplicated (if a host {@code somehost.com:1234} is provided, it will show twice, as the "portless"
     * version {@code somehost.com} is also generated). IPv6 addresses are only modified if they adhere to the strict
     * formatting using {@code []} around the address as specified in RFC 5952 Section 6 (i.e.
     * {@code [1234.5678.90AB.CDEF.1234.5678.90AB.CDEF]:1234} will insert
     * {@code [1234.5678.90AB.CDEF.1234.5678.90AB.CDEF]} as well).
     *
     * @param niFiProperties the properties object
     * @return the list of parsed custom hostnames
     */
    List<String> parseCustomHostnames(NiFiProperties niFiProperties) {
        // Load the custom hostnames from the properties
        List<String> customHostnames = niFiProperties.getWhitelistedHostsAsList();

        /* Each IPv4 address and hostname may have the port associated, so duplicate the list and trim the port
        * (the port may be different from the port NiFi is running on if provided by a proxy, etc.) IPv6 addresses
        * are not modified.
        */
        List<String> portlessHostnames = customHostnames.stream().map(hostname -> {
                    if (isIPv6Address(hostname)) {
                        return hostname;
                    } else {
                        return StringUtils.substringBeforeLast(hostname, ":");
                    }
                }
        ).collect(Collectors.toList());

        customHostnames.addAll(portlessHostnames);
        if (logger.isDebugEnabled()) {
            logger.debug("Parsed {} custom hostnames from nifi.web.proxy.host: {}", new Object[]{customHostnames.size(), StringUtils.join(customHostnames, ", ")});
        }
        return uniqueList(customHostnames);
    }

    /**
     * Returns a unique {@code List} of the elements maintaining the original order.
     *
     * @param duplicateList a list that may contain duplicate elements
     * @return a list maintaining the original order which no longer contains duplicate elements
     */
    private static List<String> uniqueList(List<String> duplicateList) {
        return new ArrayList<>(new LinkedHashSet<>(duplicateList));
    }

    /**
     * Returns true if the provided address is an IPv6 address (or could be interpreted as one). This method is more
     * lenient than {@link InetAddressUtils#isIPv6Address(String)} because of different interpretations of IPv4-mapped
     * IPv6 addresses.
     *
     * See RFC 5952 Section 4 for more information on textual representation of the IPv6 addresses.
     *
     * @param address the address in text form
     * @return true if the address is or could be parsed as an IPv6 address
     */
    static boolean isIPv6Address(String address) {
        // Note: InetAddressUtils#isIPv4MappedIPv64Address() fails on addresses that do not compress the leading 0:0:0... to ::
        // Expanded for debugging purposes
        boolean isNormalIPv6 = InetAddressUtils.isIPv6Address(address);

        // If the last two hextets are written in IPv4 form, treat it as an IPv6 address as well
        String everythingAfterLastColon = StringUtils.substringAfterLast(address, ":");
        boolean isIPv4 = InetAddressUtils.isIPv4Address(everythingAfterLastColon);
        boolean isIPv4Mapped = InetAddressUtils.isIPv4MappedIPv64Address(everythingAfterLastColon);
        boolean isCompressable = address.contains("0:0") && !address.contains("::");

        return isNormalIPv6 || isIPv4;
    }

    private int determineServerPort(NiFiProperties props) {
        return props.getSslPort() != null ? props.getSslPort() : props.getPort();
    }

    private String determineServerHostname(NiFiProperties props) {
        if (props.getSslPort() != null) {
            return props.getProperty(NiFiProperties.WEB_HTTPS_HOST, "localhost");
        } else {
            return props.getProperty(NiFiProperties.WEB_HTTP_HOST, "localhost");
        }
    }

    @Override
    public void doScope(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        logger.debug("HostHeaderHandler#doScope on " + request.getRequestURI());
        nextScope(target, baseRequest, request, response);
    }

    boolean hostHeaderIsValid(String hostHeader) {
        return validHosts.contains(hostHeader.toLowerCase().trim());
    }

    @Override
    public String toString() {
        return "HostHeaderHandler for " + serverName + ":" + serverPort;
    }

    /**
     * Returns an error message to the response and marks the request as handled if the host header is not valid.
     * Otherwise passes the request on to the next scoped handler.
     *
     * @param target      the target (not relevant here)
     * @param baseRequest the original request object
     * @param request     the request as an HttpServletRequest
     * @param response    the current response
     */
    @Override
    public void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        final String hostHeader = request.getHeader("Host");
        logger.debug("Received request [" + request.getRequestURI() + "] with host header: " + hostHeader);
        if (!hostHeaderIsValid(hostHeader)) {
            logger.warn("Request host header [" + hostHeader + "] different from web hostname [" +
                    serverName + "(:" + serverPort + ")]. Overriding to [" + serverName + ":" +
                    serverPort + request.getRequestURI() + "]");

            response.setContentType("text/html; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);

            PrintWriter out = response.getWriter();

            out.println("<h1>System Error</h1>");
            // TODO: Change to org.apache.commons.text.StringEscapeUtils
            out.println("<h2>The request contained an invalid host header [<code>" + StringEscapeUtils.escapeHtml4(hostHeader) +
                    "</code>] in the request [<code>" + StringEscapeUtils.escapeHtml4(request.getRequestURI()) +
                    "</code>]. Check for request manipulation or third-party intercept.</h2>");
            out.println("<h3>Valid host headers are [<code>empty</code>] or: <br/><code>");
            out.println(printValidHosts());
            out.println("</code></h3>");

            baseRequest.setHandled(true);
        }
    }

    String printValidHosts() {
        StringBuilder sb = new StringBuilder("<ul>");
        for (String vh : validHosts) {
            if (StringUtils.isNotBlank(vh))
                sb.append("<li>").append(StringEscapeUtils.escapeHtml4(vh)).append("</li>\n");
        }
        return sb.append("</ul>\n").toString();
    }

    public static List<String> generateDefaultHostnames(NiFiProperties niFiProperties) {
        List<String> validHosts = new ArrayList<>();
        int serverPort = 0;

        if (niFiProperties == null) {
            logger.warn("NiFiProperties not configured; returning minimal default hostnames");
        } else {
            try {
                serverPort = niFiProperties.getConfiguredHttpOrHttpsPort();
            } catch (RuntimeException e) {
                logger.warn("Cannot fully generate list of default hostnames because the server port is not configured in nifi.properties. Defaulting to port 0 for host header evaluation");
            }

            // Add any custom network interfaces
            try {
                final int lambdaPort = serverPort;
                List<String> customIPs = extractIPsFromNetworkInterfaces(niFiProperties);
                customIPs.stream().forEach(ip -> {
                    validHosts.add(ip);
                    validHosts.add(ip + ":" + lambdaPort);
                });
            } catch (final Exception e) {
                logger.warn("Failed to determine custom network interfaces.", e);
            }
        }

        // Sometimes the hostname is left empty but the port is always populated
        validHosts.add("127.0.0.1");
        validHosts.add("127.0.0.1:" + serverPort);
        validHosts.add("localhost");
        validHosts.add("localhost:" + serverPort);
        validHosts.add("[::1]");
        validHosts.add("[::1]:" + serverPort);

        // Add the loopback and actual IP address and hostname used
        try {
            validHosts.add(InetAddress.getLoopbackAddress().getHostAddress().toLowerCase());
            validHosts.add(InetAddress.getLoopbackAddress().getHostAddress().toLowerCase() + ":" + serverPort);

            validHosts.add(InetAddress.getLocalHost().getHostName().toLowerCase());
            validHosts.add(InetAddress.getLocalHost().getHostName().toLowerCase() + ":" + serverPort);

            validHosts.add(InetAddress.getLocalHost().getHostAddress().toLowerCase());
            validHosts.add(InetAddress.getLocalHost().getHostAddress().toLowerCase() + ":" + serverPort);
        } catch (final Exception e) {
            logger.warn("Failed to determine local hostname.", e);
        }

        // Dedupe but maintain order
        final List<String> uniqueHosts = uniqueList(validHosts);
        if (logger.isDebugEnabled()) {
            logger.debug("Determined {} valid default hostnames and IP addresses for incoming headers: {}", new Object[]{uniqueHosts.size(), StringUtils.join(uniqueHosts, ", ")});
        }
        return uniqueHosts;
    }

    /**
     * Extracts the list of IP addresses from custom bound network interfaces. If both HTTPS and HTTP interfaces are
     * defined and HTTPS is enabled, only HTTPS interfaces will be returned. If none are defined, an empty list will be
     * returned.
     *
     * @param niFiProperties the NiFiProperties object
     * @return the list of IP addresses
     */
    static List<String> extractIPsFromNetworkInterfaces(NiFiProperties niFiProperties) {
        Map<String, String> networkInterfaces = niFiProperties.isHTTPSConfigured() ? niFiProperties.getHttpsNetworkInterfaces() : niFiProperties.getHttpNetworkInterfaces();
        if (isNotDefined(networkInterfaces)) {
            // No custom interfaces defined
            return new ArrayList<>(0);
        } else {
            List<String> allIPAddresses = new ArrayList<>();
            for (Map.Entry<String, String> entry : networkInterfaces.entrySet()) {
                final String networkInterfaceName = entry.getValue();
                try {
                    NetworkInterface ni = NetworkInterface.getByName(networkInterfaceName);
                    List<String> ipAddresses = Collections.list(ni.getInetAddresses()).stream().map(inetAddress -> inetAddress.getHostAddress().toLowerCase()).collect(Collectors.toList());
                    logger.debug("Resolved the following IP addresses for network interface {}: {}", new Object[]{networkInterfaceName, StringUtils.join(ipAddresses, ", ")});
                    allIPAddresses.addAll(ipAddresses);
                } catch (SocketException e) {
                    logger.warn("Cannot resolve network interface named " + networkInterfaceName);
                }
            }

            // Dedupe while maintaining order
            return uniqueList(allIPAddresses);
        }
    }

    /**
     * Returns true if the provided map of properties and network interfaces is null, empty, or the actual definitions are empty.
     *
     * @param networkInterfaces the map of properties to bindings
     *                          ({@code ["nifi.web.http.network.interface.first":"eth0"]})
     * @return
     */
    static boolean isNotDefined(Map<String, String> networkInterfaces) {
        return networkInterfaces == null || networkInterfaces.isEmpty() || networkInterfaces.values().stream().filter(value -> !Strings.isNullOrEmpty(value)).collect(Collectors.toList()).isEmpty();
    }
}
