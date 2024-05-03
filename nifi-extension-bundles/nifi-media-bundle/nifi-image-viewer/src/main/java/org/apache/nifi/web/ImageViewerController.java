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
package org.apache.nifi.web;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class ImageViewerController extends HttpServlet {

    /**
     * Handles generating markup for viewing an image.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final ViewableContent content = (ViewableContent) request.getAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE);

        // handle images
        if ("image/png".equals(content.getContentType()) || "image/jpeg".equals(content.getContentType())
                || "image/gif".equals(content.getContentType()) || "image/webp".equals(content.getContentType())) {
            // defer to the jsp
            this.getServletContext().getRequestDispatcher("/WEB-INF/jsp/image.jsp").include(request, response);
        } else {
            final PrintWriter out = response.getWriter();
            out.println("Unexpected content type: " + content.getContentType());
        }
    }
}
