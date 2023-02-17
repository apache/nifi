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

import org.apache.nifi.util.StringUtils
import org.junit.Assert
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class VerbTest extends HttpServlet {
    Logger logger = LoggerFactory.getLogger(VerbTest.class)

    void doDelete(HttpServletRequest request, HttpServletResponse response) {
        validateBody(request)
        sendResponse(response)
    }

    void doPost(HttpServletRequest request, HttpServletResponse response) {
        validateBody(request)
        sendResponse(response)
    }

    void doPut(HttpServletRequest request, HttpServletResponse response) {
        validateBody(request)
        sendResponse(response)
    }

    void sendResponse(HttpServletResponse response) {
        response.contentType = "application/json"
        response.outputStream.write(prettyPrint(
            toJson([
                username: "john.smith",
                password: "testing1234"
            ])
        ).bytes)
    }

    void validateBody(HttpServletRequest request) {
        String needsBody = request.getHeader("needs-body")
        boolean bodyRequired = !StringUtils.isBlank(needsBody)
        String body = request.inputStream.text
        if (bodyRequired) {
            Assert.assertNotNull(body)
            Assert.assertFalse(StringUtils.isBlank(body))
        }
    }
}
