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
package org.apache.nifi;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.StreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class AbstractHTMLTest {

    protected final String ATL_WEATHER_TEXT = "Atlanta Weather";
    protected final String GDR_WEATHER_TEXT = "<i>Grand Rapids Weather</i>";
    protected final String ATL_WEATHER_LINK = "http://w1.weather.gov/obhistory/KPDK.html";
    protected final String GR_WEATHER_LINK = "http://w1.weather.gov/obhistory/KGRR.html";
    protected final String AUTHOR_NAME = "Jeremy Dyer";
    protected final String ATL_ID = "ATL";
    protected final String GDR_ID = "GDR";

    protected final String HTML = "<!doctype html>\n" +
            "\n" +
            "<html lang=\"en\">\n" +
            "<head>\n" +
            "  <meta charset=\"utf-8\">\n" +
            "\n" +
            "  <title>NiFi HTML Parsing Demo</title>\n" +
            "  <meta name=\"description\" content=\"NiFi HTML Parsing Demo\">\n" +
            "  <meta name=\"author\" content=\"" + AUTHOR_NAME + "\">\n" +
            "\n" +
            "  <link rel=\"stylesheet\" href=\"css/styles.css?v=1.0\">\n" +
            "\n" +
            "  <!--[if lt IE 9]>\n" +
            "  <script src=\"http://html5shiv.googlecode.com/svn/trunk/html5.js\"></script>\n" +
            "  <![endif]-->\n" +
            "</head>\n" +
            "\n" +
            "<body>\n" +
            "  <script src=\"js/scripts.js\"></script>\n" +
            "  <p>Check out this weather! <a id=\"" + ATL_ID + "\" href=\"" +
            ATL_WEATHER_LINK + "\">" + ATL_WEATHER_TEXT + "</a></p>\n" +
            "  <p>I guess it could be colder ... <a id=\"" + GDR_ID + "\" href=\"" +
            GR_WEATHER_LINK + "\">" + GDR_WEATHER_TEXT + "</a></p>\n" +
            "   <div id=\"put\"><a href=\"httpd://localhost\" /></div>\n" +
            "</body>\n" +
            "</html>";


    protected FlowFile writeContentToNewFlowFile(final byte[] content, ProcessSession session) {
        FlowFile ff = session.write(session.create(), new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
                out.write(content);
            }
        });
        return ff;
    }
}
