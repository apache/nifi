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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.URLDecoder;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.batik.dom.svg.SAXSVGDocumentFactory;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.PNGTranscoder;
import org.apache.batik.util.XMLResourceDescriptor;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.svg.SVGDocument;

/**
 *
 */
@WebServlet(name = "ConvertSvg", urlPatterns = {"/convert-svg"})
public class ConvertSvg extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(ConvertSvg.class);

    /**
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final String uri = request.getRequestURL().toString();
        final String rawSvg = request.getParameter("svg");

        // ensure the image markup has been included
        if (rawSvg == null) {
            // set the response status
            response.setContentType("text/plain");
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

            // write the response message
            PrintWriter out = response.getWriter();
            out.println("SVG must be specified.");
            return;
        }

        OutputStream bufferedOut = null;
        try {
            // get the svg and decode it, +'s need to be converted
            final String svg = URLDecoder.decode(rawSvg.replace("+", "%2B"), "UTF-8");

            if (logger.isDebugEnabled()) {
                logger.debug(svg);
            }

            String filename = request.getParameter("filename");
            if (filename == null) {
                filename = "image.png";
            } else if (!filename.endsWith(".png")) {
                filename += ".png";
            }

            final StringReader reader = new StringReader(svg);
            final String parser = XMLResourceDescriptor.getXMLParserClassName();
            final SAXSVGDocumentFactory f = new SAXSVGDocumentFactory(parser);
            final SVGDocument doc = f.createSVGDocument(uri, reader);

            response.setContentType("image/png");
            response.setHeader("Content-Disposition", "attachment; filename=" + filename);
            response.setStatus(HttpServletResponse.SC_OK);

            bufferedOut = new BufferedOutputStream(response.getOutputStream());
            final TranscoderInput transcoderInput = new TranscoderInput(doc);
            final TranscoderOutput transcoderOutput = new TranscoderOutput(bufferedOut);

            final PNGTranscoder transcoder = new PNGTranscoder();
            transcoder.transcode(transcoderInput, transcoderOutput);
        } catch (final Exception e) {
            logger.error(e.getMessage(), e);

            // set the response status
            response.setContentType("text/plain");
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

            // write the response message
            PrintWriter out = response.getWriter();
            out.println("Unable to export image as a PNG.");
        } finally {
            IOUtils.closeQuietly(bufferedOut);
        }
    }
}
