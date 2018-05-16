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
package org.apache.nifi.web.docs;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.nar.ExtensionMapping;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.Collator;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public class DocumentationController extends HttpServlet {

    private static final int GENERAL_LINK_COUNT = 4;
    private static final int DEVELOPER_LINK_COUNT = 2;

    // context for accessing the extension mapping
    private ServletContext servletContext;

    @Override
    public void init(final ServletConfig config) throws ServletException {
        super.init(config);
        servletContext = config.getServletContext();
    }

    /**
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final ExtensionMapping extensionMappings = (ExtensionMapping) servletContext.getAttribute("nifi-extension-mapping");
        final Collator collator = Collator.getInstance(Locale.US);

        // create the processors lookup
        final Map<String, String> processors = new TreeMap<>(collator);
        for (final String processorClass : extensionMappings.getProcessorNames().keySet()) {
            processors.put(StringUtils.substringAfterLast(processorClass, "."), processorClass);
        }

        // create the controller service lookup
        final Map<String, String> controllerServices = new TreeMap<>(collator);
        for (final String controllerServiceClass : extensionMappings.getControllerServiceNames().keySet()) {
            controllerServices.put(StringUtils.substringAfterLast(controllerServiceClass, "."), controllerServiceClass);
        }

        // create the reporting task lookup
        final Map<String, String> reportingTasks = new TreeMap<>(collator);
        for (final String reportingTaskClass : extensionMappings.getReportingTaskNames().keySet()) {
            reportingTasks.put(StringUtils.substringAfterLast(reportingTaskClass, "."), reportingTaskClass);
        }

        // make the available components available to the documentation jsp
        request.setAttribute("processors", processors);
        request.setAttribute("processorBundleLookup", extensionMappings.getProcessorNames());
        request.setAttribute("controllerServices", controllerServices);
        request.setAttribute("controllerServiceBundleLookup", extensionMappings.getControllerServiceNames());
        request.setAttribute("reportingTasks", reportingTasks);
        request.setAttribute("reportingTaskBundleLookup", extensionMappings.getReportingTaskNames());
        request.setAttribute("totalComponents", GENERAL_LINK_COUNT + extensionMappings.size() + DEVELOPER_LINK_COUNT);

        // forward appropriately
        request.getRequestDispatcher("/WEB-INF/jsp/documentation.jsp").forward(request, response);
    }

}
