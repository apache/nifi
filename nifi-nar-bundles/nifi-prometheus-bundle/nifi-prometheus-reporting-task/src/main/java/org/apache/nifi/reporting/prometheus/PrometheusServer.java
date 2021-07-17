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

package org.apache.nifi.reporting.prometheus;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class PrometheusServer {
    private static ComponentLog logger;
    private Server server;
    private ServletContextHandler handler;
    private ReportingContext context;

    private List<Function<ReportingContext, CollectorRegistry>> metricsCollectors;

    class MetricsServlet extends HttpServlet {

        @Override
        protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
            if (logger.isDebugEnabled()) {
                logger.debug("PrometheusServer Do get called");
            }

            ServletOutputStream response = resp.getOutputStream();
            OutputStreamWriter osw = new OutputStreamWriter(response);

            for(Function<ReportingContext, CollectorRegistry> mc : metricsCollectors) {
                CollectorRegistry collectorRegistry = mc.apply(getReportingContext());
                TextFormat.write004(osw, collectorRegistry.metricFamilySamples());
            }

            osw.flush();
            osw.close();
            response.flush();
            response.close();
            resp.setHeader("Content-Type", TextFormat.CONTENT_TYPE_004);
            resp.setStatus(HttpURLConnection.HTTP_OK);
            resp.flushBuffer();
        }
    }

    public PrometheusServer(InetSocketAddress addr, ComponentLog logger) throws Exception {
        PrometheusServer.logger = logger;
        metricsCollectors = Collections.emptyList();
        this.server = new Server(addr);
        this.handler = new ServletContextHandler(server, "/metrics");
        this.handler.addServlet(new ServletHolder(new MetricsServlet()), "/");
        try {
            this.server.start();
        } catch (Exception e) {
            // If Jetty couldn't start, stop it explicitly to avoid dangling threads
            logger.debug("PrometheusServer: Couldn't start Jetty server, stopping manually");
            this.server.stop();
            throw e;
        }
    }

    public PrometheusServer(int addr, SSLContextService sslContextService, ComponentLog logger, boolean needClientAuth, boolean wantClientAuth) throws Exception {
        PrometheusServer.logger = logger;
        this.server = new Server();
        this.handler = new ServletContextHandler(server, "/metrics");
        this.handler.addServlet(new ServletHolder(new MetricsServlet()), "/");

        SslContextFactory sslFactory = createSslFactory(sslContextService, needClientAuth, wantClientAuth);
        HttpConfiguration httpsConfiguration = new HttpConfiguration();
        httpsConfiguration.setSecureScheme("https");
        httpsConfiguration.setSecurePort(addr);
        httpsConfiguration.addCustomizer(new SecureRequestCustomizer());

        ServerConnector https = new ServerConnector(server, new SslConnectionFactory(sslFactory, "http/1.1"),
                new HttpConnectionFactory(httpsConfiguration));
        https.setPort(addr);
        this.server.setConnectors(new Connector[]{https});
        try {
            this.server.start();
        } catch (Exception e) {
            // If Jetty couldn't start, stop it explicitly to avoid dangling threads
            logger.debug("PrometheusServer: Couldn't start Jetty server, stopping manually");
            this.server.stop();
            throw e;
        }
    }

    private SslContextFactory createSslFactory(final SSLContextService sslService, boolean needClientAuth, boolean wantClientAuth) {
        SslContextFactory sslFactory = new SslContextFactory.Server();

        sslFactory.setNeedClientAuth(needClientAuth);
        sslFactory.setWantClientAuth(wantClientAuth);
        sslFactory.setProtocol(sslService.getSslAlgorithm());

        if (sslService.isKeyStoreConfigured()) {
            sslFactory.setKeyStorePath(sslService.getKeyStoreFile());
            sslFactory.setKeyStorePassword(sslService.getKeyStorePassword());
            sslFactory.setKeyStoreType(sslService.getKeyStoreType());
        }

        if (sslService.isTrustStoreConfigured()) {
            sslFactory.setTrustStorePath(sslService.getTrustStoreFile());
            sslFactory.setTrustStorePassword(sslService.getTrustStorePassword());
            sslFactory.setTrustStoreType(sslService.getTrustStoreType());
        }

        return sslFactory;
    }

    public Server getServer() {
        return this.server;
    }

    public ReportingContext getReportingContext() {
        return this.context;
    }

    public void setReportingContext(ReportingContext rc) {
        this.context = rc;
    }

    public List<Function<ReportingContext, CollectorRegistry>> getMetricsCollectors() {
        return metricsCollectors;
    }

    public void setMetricsCollectors(List<Function<ReportingContext, CollectorRegistry>> metricsCollectors) {
        this.metricsCollectors = metricsCollectors;
    }
}
