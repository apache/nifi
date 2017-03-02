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
package org.apache.nifi.processors.standard.util;

import java.util.Map;

public class HTTPUtils {

    public static final String HTTP_REQUEST_URI = "http.request.uri";
    public static final String HTTP_REMOTE_HOST = "http.remote.host";
    public static final String HTTP_LOCAL_NAME = "http.local.name";
    public static final String HTTP_PORT = "http.server.port";
    public static final String HTTP_SSL_CERT = "http.subject.dn";
    public static final String HTTP_CONTEXT_ID = "http.context.identifier";

    public static String getURI(Map<String, String> map) {
        final String client = map.get(HTTP_REMOTE_HOST);
        final String server = map.get(HTTP_LOCAL_NAME);
        final String port = map.get(HTTP_PORT);
        final String uri = map.get(HTTP_REQUEST_URI);
        if(map.get(HTTP_SSL_CERT) == null) {
            return "http://" + client + "@" + server + ":" + port + uri;
        } else {
            return "https://" + client + "@" + server + ":" + port + uri;
        }
    }

}
