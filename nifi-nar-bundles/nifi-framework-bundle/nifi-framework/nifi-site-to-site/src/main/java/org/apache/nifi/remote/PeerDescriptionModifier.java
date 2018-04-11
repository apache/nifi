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

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class PeerDescriptionModifier {

    private static final Logger logger = LoggerFactory.getLogger(PeerDescriptionModifier.class);

    public enum RequestType {
        SiteToSiteDetail,
        Peers
    }

    private static class Route {
        private String name;
        private SiteToSiteTransportProtocol protocol;
        private PreparedQuery predicate;
        private PreparedQuery hostname;
        private PreparedQuery port;
        private PreparedQuery secure;

        private Route validate() {
            if (hostname == null) {
                throw new IllegalArgumentException(
                        format("Found an invalid Site-to-Site route definition [%s] 'hostname' is not specified.", name));
            }
            if (port == null) {
                throw new IllegalArgumentException(
                        format("Found an invalid Site-to-Site route definition [%s] 'port' is not specified.", name));
            }
            return this;
        }

        private PeerDescription getTarget(final Map<String, String> variables) {
            final String targetHostName = hostname.evaluateExpressions(variables, null);
            if (isBlank(targetHostName)) {
                throw new IllegalStateException("Target hostname was not resolved for the route definition " + name);
            }

            final String targetPortStr = port.evaluateExpressions(variables, null);
            if (isBlank(targetPortStr)) {
                throw new IllegalStateException("Target port was not resolved for the route definition " + name);
            }

            final String targetIsSecure = secure == null ? null : secure.evaluateExpressions(variables, null);
            return new PeerDescription(targetHostName, Integer.valueOf(targetPortStr), Boolean.valueOf(targetIsSecure));
        }
    }

    private Map<SiteToSiteTransportProtocol, List<Route>> routes;


    private static final String PROPERTY_PREFIX = "nifi.remote.route.";
    private static final Pattern PROPERTY_REGEX = Pattern.compile("^nifi\\.remote\\.route\\.(raw|http)\\.([^.]+)\\.(when|hostname|port|secure)$");

    public PeerDescriptionModifier(final NiFiProperties properties) {
        final Map<Tuple<String, String>, List<Tuple<String, String>>> routeDefinitions = properties.getPropertyKeys().stream()
                .filter(propertyKey -> propertyKey.startsWith(PROPERTY_PREFIX))
                .map(propertyKey -> {
                            final Matcher matcher = PROPERTY_REGEX.matcher(propertyKey);
                            if (!matcher.matches()) {
                                throw new IllegalArgumentException(
                                        format("Found an invalid Site-to-Site route definition property '%s'." +
                                                        " Routing property keys should be formatted as 'nifi.remote.route.{protocol}.{name}.{routingConfigName}'." +
                                                        " Where {protocol} is 'raw' or 'http', and {routingConfigName} is 'when', 'hostname', 'port' or 'secure'.",
                                                propertyKey));
                            }
                            return matcher;
                })
                .collect(Collectors.groupingBy(matcher -> new Tuple<>(matcher.group(1), matcher.group(2)),
                        Collectors.mapping(matcher -> new Tuple<>(matcher.group(3), matcher.group(0)), Collectors.toList())));

        routes = routeDefinitions.entrySet().stream().map(routeDefinition -> {
            final Route route = new Route();
            // E.g. [raw, example1], [http, example2]
            final Tuple<String, String> protocolAndRoutingName = routeDefinition.getKey();
            route.protocol = SiteToSiteTransportProtocol.valueOf(protocolAndRoutingName.getKey().toUpperCase());
            route.name = protocolAndRoutingName.getValue();
            routeDefinition.getValue().forEach(routingConfigNameAndPropertyKey -> {
                final String routingConfigName = routingConfigNameAndPropertyKey.getKey();
                final String propertyKey = routingConfigNameAndPropertyKey.getValue();
                final String routingConfigValue = properties.getProperty(propertyKey);
                try {
                    switch (routingConfigName) {
                        case "when":
                            route.predicate = Query.prepare(routingConfigValue);
                            break;
                        case "hostname":
                            route.hostname = Query.prepare(routingConfigValue);
                            break;
                        case "port":
                            route.port = Query.prepare(routingConfigValue);
                            break;
                        case "secure":
                            route.secure = Query.prepare(routingConfigValue);
                            break;
                    }
                } catch (AttributeExpressionLanguageParsingException e) {
                    throw new IllegalArgumentException(format("Failed to parse NiFi expression language configured" +
                            " for Site-to-Site routing property at '%s' due to '%s'", propertyKey, e.getMessage()), e);
                }
            });
            return route;
        }).map(Route::validate).collect(Collectors.groupingBy(r -> r.protocol));

    }

    private void addVariables(Map<String, String> map, String prefix, PeerDescription peer) {
        map.put(format("%s.hostname", prefix), peer.getHostname());
        map.put(format("%s.port", prefix), String.valueOf(peer.getPort()));
        map.put(format("%s.secure", prefix), String.valueOf(peer.isSecure()));
    }

    public boolean isModificationNeeded(final SiteToSiteTransportProtocol protocol) {
        return routes != null && routes.containsKey(protocol) && !routes.get(protocol).isEmpty();
    }

    /**
     * Modifies target peer description so that subsequent request can go through the appropriate route
     * @param source The source peer from which a request was sent, this can be any server host participated to relay the request,
     *              but should be the one which can contribute to derive the correct target peer.
     * @param target The original target which should receive and process further incoming requests.
     * @param protocol The S2S protocol being used.
     * @param requestType The requested API type.
     * @param variables Containing context variables those can be referred from Expression Language.
     * @return A peer description. The original target peer can be returned if there is no intermediate peer such as reverse proxies needed.
     */
    public PeerDescription modify(final PeerDescription source, final PeerDescription target,
                                  final SiteToSiteTransportProtocol protocol, final RequestType requestType,
                                  final Map<String, String> variables) {

        addVariables(variables, "s2s.source", source);
        addVariables(variables, "s2s.target", target);
        variables.put("s2s.protocol", protocol.name());
        variables.put("s2s.request", requestType.name());

        logger.debug("Modifying PeerDescription, variables={}", variables);

        return routes.get(protocol).stream().filter(r -> r.predicate == null
                || Boolean.valueOf(r.predicate.evaluateExpressions(variables, null)))
                .map(r -> {
                    final PeerDescription t = r.getTarget(variables);
                    logger.debug("Route definition {} matched, {}", r.name, t);
                    return t;
                })
                // If a matched route was found, use it, else use the original target.
                .findFirst().orElse(target);

    }
}
