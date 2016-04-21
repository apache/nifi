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
package org.apache.nifi.web.filter;

import java.io.IOException;
import java.io.Serializable;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.util.WebUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * A filter that prevents direct access to nodes (i.e., flow controllers connected to a cluster). Direct access to nodes by clients external to the cluster is prevented because the dataflow must be
 * identical across the cluster.
 *
 * Direct access to a node is determined by the presence of a custom request header. The header key is "X-CLUSTER_MANAGER" and the value can be anything/empty. The presence of this header is a simple
 * way to flag that the request was issued by the cluster manager and may proceed to the next filter.
 *
 * Since this header may be faked, we only make decisions about the header if the application instance is a node and connected to the cluster.
 *
 */
public class NodeRequestFilter implements Filter {

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(NodeRequestFilter.class));

    private FilterConfig config;

    @Override
    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain filterChain)
            throws IOException, ServletException {

        ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(config.getServletContext());
        NiFiProperties properties = ctx.getBean("nifiProperties", NiFiProperties.class);

        HttpServletRequest httpReq = (HttpServletRequest) req;
        HttpServletResponse httpResp = (HttpServletResponse) resp;

        if (properties.isClusterManager() || "HEAD".equalsIgnoreCase(httpReq.getMethod())) {
            filterChain.doFilter(req, resp);
        } else {

            NiFiServiceFacade serviceFacade = ctx.getBean("serviceFacade", NiFiServiceFacade.class);

            if (!serviceFacade.isClustered()) {
                filterChain.doFilter(req, resp);
            } else {
                // get the cluster context from the request
                ClusterContext clusterContext = null;
                String clusterContextHeaderValue = httpReq.getHeader(WebClusterManager.CLUSTER_CONTEXT_HTTP_HEADER);
                if (StringUtils.isNotBlank(clusterContextHeaderValue)) {
                    try {
                        // deserialize object
                        Serializable clusterContextObj = WebUtils.deserializeHexToObject(clusterContextHeaderValue);
                        if (clusterContextObj instanceof ClusterContext) {
                            clusterContext = (ClusterContext) clusterContextObj;
                        }
                    } catch (final ClassNotFoundException cnfe) {
                        logger.warn("Failed to deserialize cluster context from request due to: " + cnfe, cnfe);
                    }
                }

                // if don't have a cluster context or the context indicates
                if (clusterContext == null || !clusterContext.isRequestSentByClusterManager()) {
                    // node is connected and request is not from cluster manager, so respond with error
                    httpResp.setContentType("text/plain");
                    httpResp.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    httpResp.getWriter().print("Direct access to Flow Controller node is only permissible if node is disconnected.");
                } else {
                    ClusterContextThreadLocal.setContext(clusterContext);
                    filterChain.doFilter(req, resp);
                }
            }
        }
    }

    @Override
    public void init(final FilterConfig config) {
        this.config = config;
    }

    @Override
    public void destroy() {
    }
}
