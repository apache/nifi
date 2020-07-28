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

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static groovy.json.JsonOutput.*

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class SimpleJson extends HttpServlet {
    Logger logger = LoggerFactory.getLogger(SimpleJson.class)
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String u = request.getHeader("X-USER")
        String p = request.getHeader("X-PASS")

        response.contentType = "application/json"
        response.outputStream.write(prettyPrint(
            toJson([
                username: u ?: "john.smith",
                password: p ?: "testing1234"
            ])
        ).bytes)
    }
}
