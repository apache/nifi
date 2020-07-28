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

package org.apache.nifi.lookup.rest.handlers

import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.handler.AbstractHandler

import javax.servlet.ServletException
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class BasicAuth extends AbstractHandler {

    @Override
    void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        baseRequest.handled = true
        def authString = request.getHeader("Authorization")
        def headers = []
        request.headerNames.each { headers << it }

        if (!authString || authString != "Basic am9obi5zbWl0aDp0ZXN0aW5nMTIzNA==") {
            response.status = 401
            response.setHeader("WWW-Authenticate", "Basic realm=\"Jetty\"")
            response.setHeader("response.phrase", "Unauthorized")
            response.contentType = "text/plain"
            response.writer.println("Get off my lawn!")
            return
        }

        response.writer.println(prettyPrint(
            toJson([
                username: "john.smith",
                password: "testing1234"
            ])
        ))
    }
}