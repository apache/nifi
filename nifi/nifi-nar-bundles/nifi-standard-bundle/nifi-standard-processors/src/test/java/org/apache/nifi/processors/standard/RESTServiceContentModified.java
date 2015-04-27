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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class RESTServiceContentModified extends HttpServlet {

    private static final long serialVersionUID = 1L;
    static String result = "[\"sample1\",\"sample2\",\"sample3\",\"sample4\"]";
    static long modificationDate = System.currentTimeMillis() / 1000 * 1000; // time resolution is to the second
    static int ETAG;
    public static boolean IGNORE_ETAG = false;
    public static boolean IGNORE_LAST_MODIFIED = false;

    public RESTServiceContentModified() {
        ETAG = this.hashCode();
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String ifModifiedSince = request.getHeader("If-Modified-Since");
        String ifNoneMatch = request.getHeader("If-None-Match");

        final SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        response.setContentType("application/json");
        if (ifNoneMatch != null && ifNoneMatch.length() > 0 && !IGNORE_ETAG && Integer.
                parseInt(ifNoneMatch) == ETAG) {
            response.setStatus(304);
            response.setHeader("Last-Modified", dateFormat.
                    format(modificationDate));
            response.setHeader("ETag", Integer.toString(ETAG));
            return;
        }

        long date = -1;
        if (ifModifiedSince != null && ifModifiedSince.length() > 0 && !IGNORE_LAST_MODIFIED) {
            try {
                date = dateFormat.parse(ifModifiedSince).
                        getTime();
            } catch (Exception e) {

            }
        }
        if (date >= modificationDate) {
            response.setStatus(304);
            response.setHeader("Last-Modified", dateFormat.
                    format(modificationDate));
            response.setHeader("ETag", Integer.toString(ETAG));
            return;
        }

        response.setStatus(200);
        response.setHeader("Last-Modified", dateFormat.format(modificationDate));
        response.setHeader("ETag", Integer.toString(ETAG));
        response.getOutputStream().
                println(result);
    }

}
