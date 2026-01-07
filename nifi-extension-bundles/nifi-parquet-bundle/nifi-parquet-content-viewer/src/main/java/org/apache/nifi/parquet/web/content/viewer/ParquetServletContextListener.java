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
package org.apache.nifi.parquet.web.content.viewer;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.FilterRegistration;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.ServletRegistration;
import jakarta.servlet.annotation.WebListener;
import org.apache.nifi.parquet.web.controller.ParquetContentViewerController;
import org.apache.nifi.web.servlet.filter.QueryStringToFragmentFilter;
import org.eclipse.jetty.ee11.servlet.DefaultServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;

/**
 * Servlet Context Listener supporting registration of Filters
 */
@WebListener
public class ParquetServletContextListener implements ServletContextListener {
    private static final String API_CONTENT_MAPPING = "/api/content";

    private static final int LOAD_ON_STARTUP_ENABLED = 1;

    private static final String DIR_ALLOWED_PARAMETER = "dirAllowed";

    private static final String BASE_RESOURCE_PARAMETER = "baseResource";

    private static final String BASE_RESOURCE_DIRECTORY = "WEB-INF/classes/static";

    private static final String DEFAULT_MAPPING = "/";

    private static final Logger logger = LoggerFactory.getLogger(ParquetServletContextListener.class);

    @Override
    public void contextInitialized(final ServletContextEvent sce) {
        final ServletContext servletContext = sce.getServletContext();
        final FilterRegistration.Dynamic filter = servletContext.addFilter(QueryStringToFragmentFilter.class.getSimpleName(), QueryStringToFragmentFilter.class);
        filter.addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), false, DEFAULT_MAPPING);

        final ServletRegistration.Dynamic servlet = servletContext.addServlet(ParquetContentViewerController.class.getSimpleName(), ParquetContentViewerController.class);
        servlet.addMapping(API_CONTENT_MAPPING);
        servlet.setLoadOnStartup(LOAD_ON_STARTUP_ENABLED);

        final ServletRegistration.Dynamic defaultServlet = servletContext.addServlet(DefaultServlet.class.getSimpleName(), DefaultServlet.class);
        defaultServlet.addMapping(DEFAULT_MAPPING);
        defaultServlet.setInitParameter(DIR_ALLOWED_PARAMETER, Boolean.FALSE.toString());
        defaultServlet.setInitParameter(BASE_RESOURCE_PARAMETER, BASE_RESOURCE_DIRECTORY);
        defaultServlet.setLoadOnStartup(LOAD_ON_STARTUP_ENABLED);

        logger.info("Parquet Content Viewer Initialized");
    }
}
