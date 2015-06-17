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
package org.apache.nifi.web.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@WebServlet(name = "DownloadSvg", urlPatterns = {"/download-svg"})
public class DownloadSvg extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(DownloadSvg.class);

    /**
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final String svg = request.getParameter("svg");

        // ensure the image markup has been included
        if (svg == null) {
            // set the response status
            response.setContentType("text/plain");
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

            // write the response message
            PrintWriter out = response.getWriter();
            out.println("SVG must be specified.");
            return;
        }

        try {
            if (logger.isDebugEnabled()) {
                logger.debug(svg);
            }

            String filename = request.getParameter("filename");
            if (filename == null) {
                filename = "image.svg";
            } else if (!filename.endsWith(".svg")) {
                filename += ".svg";
            }

            response.setContentType("image/svg+xml");
            response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
            response.setStatus(HttpServletResponse.SC_OK);

            response.getWriter().print(svg);
        } catch (final Exception e) {
            logger.error(e.getMessage(), e);

            // set the response status
            response.setContentType("text/plain");
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

            // write the response message
            PrintWriter out = response.getWriter();
            out.println("Unable to export image as a SVG.");
        }
    }
}
