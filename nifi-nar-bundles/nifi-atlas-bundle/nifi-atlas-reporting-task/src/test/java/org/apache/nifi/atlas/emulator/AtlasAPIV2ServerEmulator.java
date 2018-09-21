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
package org.apache.nifi.atlas.emulator;

import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.AtlasUtils;
import org.apache.nifi.atlas.NiFiTypes;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.atlas.AtlasUtils.isGuidAssigned;
import static org.apache.nifi.atlas.AtlasUtils.toStr;
import static org.apache.nifi.atlas.AtlasUtils.toTypedQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_GUID;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_TYPENAME;
import static org.apache.nifi.util.StringUtils.isEmpty;

/**
 * Emulate Atlas API v2 server for NiFi implementation testing.
 */
public class AtlasAPIV2ServerEmulator {

    private static final Logger logger = LoggerFactory.getLogger(AtlasAPIV2ServerEmulator.class);
    private Server server;
    private ServerConnector httpConnector;
    private AtlasNotificationServerEmulator notificationServerEmulator;
    private EmbeddedKafka embeddedKafka;

    public static void main(String[] args) throws Exception {
        final AtlasAPIV2ServerEmulator emulator = new AtlasAPIV2ServerEmulator();
        emulator.start();
    }

    public void start() throws Exception {
        if (server == null) {
            createServer();
        }

        server.start();
        logger.info("Starting {} on port {}", AtlasAPIV2ServerEmulator.class.getSimpleName(), httpConnector.getLocalPort());

        embeddedKafka = new EmbeddedKafka(false);
        embeddedKafka.start();

        notificationServerEmulator.consume(m -> {
            if (m instanceof HookNotification.EntityCreateRequest) {
                HookNotification.EntityCreateRequest em = (HookNotification.EntityCreateRequest) m;
                for (Referenceable ref : em.getEntities()) {
                    final AtlasEntity entity = toEntity(ref);
                    createEntityByNotification(entity);
                }
            } else if (m instanceof HookNotification.EntityPartialUpdateRequest) {
                HookNotification.EntityPartialUpdateRequest em
                        = (HookNotification.EntityPartialUpdateRequest) m;
                final AtlasEntity entity = toEntity(em.getEntity());
                entity.setAttribute(em.getAttribute(), em.getAttributeValue());
                updateEntityByNotification(entity);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void createEntityByNotification(AtlasEntity entity) {
        final String key = toTypedQname(entity);
        final AtlasEntity exEntity = atlasEntitiesByTypedQname.get(key);

        if (exEntity != null) {
            convertReferenceableToObjectId(entity.getAttributes()).forEach((k, v) -> {
                Object r = v;
                final Object exAttr = exEntity.getAttribute(k);
                if (exAttr != null && exAttr instanceof Collection) {
                    ((Collection) exAttr).addAll((Collection) v);
                    r = exAttr;
                }
                exEntity.setAttribute(k, r);
            });
        } else {
            String guid = String.valueOf(guidSeq.getAndIncrement());
            entity.setGuid(guid);
            atlasEntitiesByTypedQname.put(key, entity);
            atlasEntitiesByGuid.put(guid, entity);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> resolveIOReference(Object _refs) {
        if (_refs == null) {
            return Collections.emptyList();
        }
        final Collection<Map<String, Object>> refs = (Collection<Map<String, Object>>) _refs;
        return refs.stream().map(ref -> {
            final String typeName = toStr(ref.get(ATTR_TYPENAME));
            final String qualifiedName = toStr(((Map<String, Object>) ref.get("uniqueAttributes")).get(ATTR_QUALIFIED_NAME));
            final String guid = toStr(ref.get(ATTR_GUID));
            if (isEmpty(guid)) {
                final String typedQname = toTypedQualifiedName(typeName, qualifiedName);
                final AtlasEntity referredEntity = atlasEntitiesByTypedQname.get(typedQname);
                if (referredEntity == null) {
                    throw new RuntimeException("Entity does not exist for " + typedQname);
                }
                ref.put(ATTR_GUID, referredEntity.getGuid());
            }
            return ref;
        }).collect(Collectors.toList());
    }

    private void updateEntityByNotification(AtlasEntity entity) {
        final String inputGuid = entity.getGuid();
        final String inputTypedQname = toTypedQname(entity);
        final AtlasEntity exEntity = isGuidAssigned(inputGuid)
                ? atlasEntitiesByGuid.get(inputGuid)
                : atlasEntitiesByTypedQname.get(inputTypedQname);

        if (exEntity != null) {
            convertReferenceableToObjectId(entity.getAttributes()).forEach((k, v) -> {
                final Object r;
                switch (k) {
                    case "inputs":
                    case "outputs": {
                        // If a reference doesn't have guid, then find it.
                        r = resolveIOReference(v);
                    }
                    break;
                    default:
                        r = v;
                }
                exEntity.setAttribute(k, r);
            });
        } else {
            throw new RuntimeException("Existing entity to be updated was not found for, " + entity);
        }
    }

    private void createServer() throws Exception {
        server = new Server();
        final ContextHandlerCollection handlerCollection = new ContextHandlerCollection();

        final ServletContextHandler staticContext = new ServletContextHandler();
        staticContext.setContextPath("/");

        final ServletContextHandler atlasApiV2Context = new ServletContextHandler();
        atlasApiV2Context.setContextPath("/api/atlas/v2/");

        handlerCollection.setHandlers(new Handler[]{staticContext, atlasApiV2Context});

        server.setHandler(handlerCollection);

        final ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setBaseResource(Resource.newClassPathResource("public", false, false));
        staticContext.setHandler(resourceHandler);

        final ServletHandler servletHandler = new ServletHandler();
        atlasApiV2Context.insertHandler(servletHandler);

        httpConnector = new ServerConnector(server);
        httpConnector.setPort(21000);

        server.setConnectors(new Connector[]{httpConnector});

        servletHandler.addServletWithMapping(TypeDefsServlet.class, "/types/typedefs/");
        servletHandler.addServletWithMapping(EntityBulkServlet.class, "/entity/bulk/");
        servletHandler.addServletWithMapping(EntityGuidServlet.class, "/entity/guid/*");
        servletHandler.addServletWithMapping(SearchByUniqueAttributeServlet.class, "/entity/uniqueAttribute/type/*");
        servletHandler.addServletWithMapping(SearchBasicServlet.class, "/search/basic/");
        servletHandler.addServletWithMapping(LineageServlet.class, "/debug/lineage/");

        notificationServerEmulator = new AtlasNotificationServerEmulator();
    }

    public void stop() throws Exception {
        notificationServerEmulator.stop();
        embeddedKafka.stop();
        server.stop();
    }

    private static void respondWithJson(HttpServletResponse resp, Object entity) throws IOException {
        respondWithJson(resp, entity, HttpServletResponse.SC_OK);
    }

    private static void respondWithJson(HttpServletResponse resp, Object entity, int statusCode) throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        final ServletOutputStream out = resp.getOutputStream();
        new ObjectMapper().writer().writeValue(out, entity);
        out.flush();
    }

    private static <T> T readInputJSON(HttpServletRequest req, Class<? extends T> clazz) throws IOException {
        return new ObjectMapper().reader().withType(clazz).readValue(req.getInputStream());
    }

    private static final AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
    // key = type::qualifiedName
    private static final Map<String, AtlasEntity> atlasEntitiesByTypedQname = new HashMap<>();
    private static final Map<String, AtlasEntity> atlasEntitiesByGuid = new HashMap<>();

    public static class TypeDefsServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final String name = req.getParameter("name");
            AtlasTypesDef result = atlasTypesDef;
            if (name != null && !name.isEmpty()) {
                result = new AtlasTypesDef();
                final Optional<AtlasEntityDef> entityDef = atlasTypesDef.getEntityDefs().stream().filter(en -> en.getName().equals(name)).findFirst();
                if (entityDef.isPresent()) {
                    result.getEntityDefs().add(entityDef.get());
                }
            }
            respondWithJson(resp, result);
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final AtlasTypesDef newTypes = readInputJSON(req, AtlasTypesDef.class);
            final Map<String, AtlasEntityDef> defs = new HashMap<>();
            for (AtlasEntityDef existingDef : atlasTypesDef.getEntityDefs()) {
                defs.put(existingDef.getName(), existingDef);
            }
            for (AtlasEntityDef entityDef : newTypes.getEntityDefs()) {
                defs.put(entityDef.getName(), entityDef);
            }
            atlasTypesDef.setEntityDefs(defs.values().stream().collect(Collectors.toList()));

            respondWithJson(resp, atlasTypesDef);
        }

        @Override
        protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            super.doPut(req, resp);
        }
    }

    private static final AtomicInteger guidSeq = new AtomicInteger(0);

    public static class EntityBulkServlet extends HttpServlet {

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final AtlasEntity.AtlasEntitiesWithExtInfo withExtInfo = readInputJSON(req, AtlasEntity.AtlasEntitiesWithExtInfo.class);
            final Map<String, String> guidAssignments = new HashMap<>();
            withExtInfo.getEntities().forEach(entity -> {
                atlasEntitiesByTypedQname.put(toTypedQname(entity), entity);
                String guid = entity.getGuid();
                if (!AtlasUtils.isGuidAssigned(guid)) {
                    final String _guid = String.valueOf(guidSeq.getAndIncrement());
                    guidAssignments.put(guid, _guid);
                    entity.setGuid(_guid);
                    guid = _guid;
                }
                atlasEntitiesByGuid.put(guid, entity);
            });
            final EntityMutationResponse mutationResponse = new EntityMutationResponse();
            mutationResponse.setGuidAssignments(guidAssignments);
            respondWithJson(resp, mutationResponse);
        }

        @Override
        protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            atlasEntitiesByTypedQname.clear();
            atlasEntitiesByGuid.clear();
            resp.setStatus(200);
        }
    }

    public static class SearchBasicServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final AtlasSearchResult result = new AtlasSearchResult();
            result.setEntities(atlasEntitiesByTypedQname.values().stream()
                    .map(entity -> new AtlasEntityHeader(entity.getTypeName(), entity.getAttributes())).collect(Collectors.toList()));
            respondWithJson(resp, result);
        }
    }

    public static class EntityGuidServlet extends HttpServlet {

        private static Pattern URL_PATTERN = Pattern.compile(".+/guid/([^/]+)");

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final Matcher matcher = URL_PATTERN.matcher(req.getRequestURI());
            if (matcher.matches()) {
                final String guid = matcher.group(1);
                final AtlasEntity entity = atlasEntitiesByGuid.get(guid);
                if (entity != null) {
                    respondWithJson(resp, createSearchResult(entity));
                    return;
                }
            }
            resp.setStatus(404);
        }
    }

    private static AtlasEntity.AtlasEntityWithExtInfo createSearchResult(AtlasEntity entity) {
        entity.setAttribute(ATTR_INPUTS, resolveIOReference(entity.getAttribute(ATTR_INPUTS)));
        entity.setAttribute(ATTR_OUTPUTS, resolveIOReference(entity.getAttribute(ATTR_OUTPUTS)));
        return new AtlasEntity.AtlasEntityWithExtInfo(entity);
    }

    public static class SearchByUniqueAttributeServlet extends HttpServlet {

        private static Pattern URL_PATTERN = Pattern.compile(".+/uniqueAttribute/type/([^/]+)");

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            // http://localhost:21000/api/atlas/v2/entity/uniqueAttribute/type/nifi_flow_path?attr:qualifiedName=2e9a2852-228f-379b-0000-000000000000@example
            final Matcher matcher = URL_PATTERN.matcher(req.getRequestURI());
            if (matcher.matches()) {
                final String typeName = matcher.group(1);
                final String qualifiedName = req.getParameter("attr:qualifiedName");
                final AtlasEntity entity = atlasEntitiesByTypedQname.get(toTypedQualifiedName(typeName, qualifiedName));
                if (entity != null) {
                    respondWithJson(resp, createSearchResult(entity));
                    return;
                }
            }
            resp.setStatus(404);
        }
    }

    private static AtlasEntity toEntity(Referenceable ref) {
        return new AtlasEntity(ref.getTypeName(), convertReferenceableToObjectId(ref.getValuesMap()));
    }

    private static Map<String, Object> convertReferenceableToObjectId(Map<String, Object> values) {
        final Map<String, Object> result = new HashMap<>();
        for (String k : values.keySet()) {
            Object v = values.get(k);
            result.put(k, toV2(v));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Object toV2(Object v) {
        Object r = v;
        if (v instanceof Referenceable) {
            r = toMap((Referenceable) v);
        } else if (v instanceof Map) {
            r = convertReferenceableToObjectId((Map<String, Object>) v);
        } else if (v instanceof Collection) {
            r = ((Collection) v).stream().map(AtlasAPIV2ServerEmulator::toV2).collect(Collectors.toList());
        }
        return r;
    }

    private static Map<String, Object> toMap(Referenceable ref) {
        final HashMap<String, Object> result = new HashMap<>();
        result.put(ATTR_TYPENAME, ref.getTypeName());
        final HashMap<String, String> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put(ATTR_QUALIFIED_NAME, (String) ref.getValuesMap().get(ATTR_QUALIFIED_NAME));
        result.put("uniqueAttributes", uniqueAttrs);
        result.put(ATTR_GUID, ref.getId() != null ? ref.getId()._getId() : null);
        return result;
    }

    private static String toTypedQname(AtlasEntity entity) {
        return toTypedQualifiedName(entity.getTypeName(), (String) entity.getAttribute("qualifiedName"));
    }

    public static class LineageServlet extends HttpServlet {

        private Node toNode(AtlasEntity entity) {
            Node node = new Node();
            node.setName(entity.getAttribute(NiFiTypes.ATTR_NAME).toString());
            node.setQualifiedName(entity.getAttribute(ATTR_QUALIFIED_NAME).toString());
            node.setType(entity.getTypeName());
            return node;
        }

        private Link toLink(AtlasEntity s, AtlasEntity t, Map<String, Integer> nodeIndices) {
            final Integer sid = nodeIndices.get(toTypedQname(s));
            final Integer tid = nodeIndices.get(toTypedQname(t));

            return new Link(sid, tid);
        }

        private String toLinkKey(Integer s, Integer t) {
            return s + "::" + t;
        }

        @SuppressWarnings("unchecked")
        private AtlasEntity getReferredEntity(Map<String, Object> reference) {
            final String guid = toStr(reference.get(ATTR_GUID));
            final String qname = ((Map<String, String>) reference.get("uniqueAttributes")).get(ATTR_QUALIFIED_NAME);
            return isGuidAssigned(guid)
                    ? atlasEntitiesByGuid.get(guid)
                    : atlasEntitiesByTypedQname.get(toTypedQualifiedName((String) reference.get(ATTR_TYPENAME), qname));
        }

        @SuppressWarnings("unchecked")
        private void traverse(Set<AtlasEntity> seen, AtlasEntity s, List<Link> links, Map<String, Integer> nodeIndices, Map<String, List<AtlasEntity>> outgoingEntities) {

            // To avoid cyclic links.
            if (seen.contains(s)) {
                return;
            }
            seen.add(s);

            // Traverse entities those are updated by this entity.
            final Object outputs = s.getAttribute(ATTR_OUTPUTS);
            if (outputs != null) {
                for (Map<String, Object> output : ((List<Map<String, Object>>) outputs)) {
                    final AtlasEntity t = getReferredEntity(output);

                    if (t != null) {
                        links.add(toLink(s, t, nodeIndices));
                        traverse(seen, t, links, nodeIndices, outgoingEntities);
                    }
                }
            }

            // Add link to the input objects for this entity.
            final Object inputs = s.getAttribute(NiFiTypes.ATTR_INPUTS);
            if (inputs != null) {
                for (Map<String, Object> input : ((List<Map<String, Object>>) inputs)) {
                    final AtlasEntity t = getReferredEntity(input);

                    if (t != null) {
                        links.add(toLink(t, s, nodeIndices));
                    }
                }
            }

            // Traverse entities those consume this entity as their input.
            final List<AtlasEntity> outGoings = Stream.of(outgoingEntities.getOrDefault(toTypedQname(s), Collections.emptyList()),
                    outgoingEntities.getOrDefault(s.getGuid(), Collections.emptyList())).flatMap(List::stream).collect(Collectors.toList());
            outGoings.forEach(o -> {
                links.add(toLink(s, o, nodeIndices));
                traverse(seen, o, links, nodeIndices, outgoingEntities);
            });

        }

        @SuppressWarnings("unchecked")
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final Lineage result = new Lineage();
            final List<Node> nodes = new ArrayList<>();
            final List<Link> links = new ArrayList<>();
            final Map<String, Integer> nodeIndices = new HashMap<>();
            // DataSet to outgoing Processes, either by guid or typed qname.
            final Map<String, List<AtlasEntity>> outgoingEntities = new HashMap<>();
            result.setNodes(nodes);

            // Add all nodes.
            atlasEntitiesByTypedQname.entrySet().forEach(entry -> {
                nodeIndices.put(entry.getKey(), nodes.size());
                final AtlasEntity entity = entry.getValue();
                nodes.add(toNode(entity));

                // Capture inputs
                final Object inputs = entity.getAttribute(NiFiTypes.ATTR_INPUTS);
                if (inputs != null) {
                    for (Map<String, Object> input : ((List<Map<String, Object>>) inputs)) {
                        final AtlasEntity t = getReferredEntity(input);
                        if (t != null) {
                            final String typedQname = toTypedQualifiedName(t.getTypeName(), toStr(t.getAttribute(ATTR_QUALIFIED_NAME)));
                            final String guid = t.getGuid();
                            outgoingEntities.computeIfAbsent(typedQname, k -> new ArrayList<>()).add(entity);
                            outgoingEntities.computeIfAbsent(guid, k -> new ArrayList<>()).add(entity);
                        }

                    }
                }

            });

            // Correct all flow_path
            final Map<String, List<AtlasEntity>> entities = atlasEntitiesByTypedQname.values().stream()
                    .collect(Collectors.groupingBy(AtlasEntity::getTypeName));
            final HashSet<AtlasEntity> seen = new HashSet<>();

            // Add nifi_flow
            if (entities.containsKey(NiFiTypes.TYPE_NIFI_FLOW)) {

                if (entities.containsKey(NiFiTypes.TYPE_NIFI_FLOW_PATH)) {

                    final List<AtlasEntity> flowPaths = entities.get(NiFiTypes.TYPE_NIFI_FLOW_PATH);

                    // Find the starting flow_paths
                    final List<AtlasEntity> heads = flowPaths.stream()
                            .filter(p -> {
                                Object inputs = p.getAttribute(NiFiTypes.ATTR_INPUTS);
                                return inputs == null || ((Collection) inputs).isEmpty()
                                        // This condition matches the head processor but has some inputs those are created by notification.
                                        || ((Collection<Map<String, Object>>) inputs).stream().anyMatch(m -> !"nifi_queue".equals(m.get("typeName")));
                            })
                            .collect(Collectors.toList());

                    final List<AtlasEntity> inputPorts = entities.get(NiFiTypes.TYPE_NIFI_INPUT_PORT);
                    if (inputPorts != null) {
                        heads.addAll(inputPorts);
                    }

                    heads.forEach(s -> {

                        // Link it to parent NiFi Flow.
                        final Object nifiFlowRef = s.getAttribute("nifiFlow");
                        if (nifiFlowRef != null) {

                            final AtlasEntity nifiFlow = getReferredEntity((Map<String, Object>) nifiFlowRef);
                            if (nifiFlow != null) {
                                links.add(toLink(nifiFlow, s, nodeIndices));
                            }
                        }

                        // Traverse recursively
                        traverse(seen, s, links, nodeIndices, outgoingEntities);
                    });

                }
            }

            final List<Link> uniqueLinks = new ArrayList<>();
            final Set linkKeys = new HashSet<>();
            for (Link link : links) {
                final String linkKey = toLinkKey(link.getSource(), link.getTarget());
                if (!linkKeys.contains(linkKey)) {
                    uniqueLinks.add(link);
                    linkKeys.add(linkKey);
                }
            }

            // Links from nifi_flow to nifi_flow_path/nifi_input_port should be narrow (0.1).
            // Others will share 1.0.
            final Map<Boolean, List<Link>> flowToPathOrElse = uniqueLinks.stream().collect(Collectors.groupingBy(l -> {
                final String stype = nodes.get(l.getSource()).getType();
                final String ttype = nodes.get(l.getTarget()).getType();
                return "nifi_flow".equals(stype) && ("nifi_flow_path".equals(ttype) || ("nifi_input_port".equals(ttype)));
            }));

            flowToPathOrElse.forEach((f2p, ls) -> {
                if (f2p) {
                    ls.forEach(l -> l.setValue(0.1));
                } else {
                    // Group links by its target, and configure each weight value.
                    // E.g. 1 -> 3 and 2 -> 3, then 1 (0.5) -> 3 and 2 (0.5) -> 3.
                    ls.stream().collect(Collectors.groupingBy(Link::getTarget))
                            .forEach((t, ls2SameTgt) -> ls2SameTgt.forEach(l -> l.setValue(1.0 / (double) ls2SameTgt.size())));
                }
            });

            result.setLinks(uniqueLinks);

            respondWithJson(resp, result);
        }
    }

}
