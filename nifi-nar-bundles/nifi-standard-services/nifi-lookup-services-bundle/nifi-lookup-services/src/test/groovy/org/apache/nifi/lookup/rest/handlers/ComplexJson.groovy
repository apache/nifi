package org.apache.nifi.lookup.rest.handlers

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class ComplexJson extends HttpServlet {
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.contentType = "application/json"
        response.outputStream.write(prettyPrint(
            toJson([
                top: [
                    middle: [
                        inner: [
                            "username": "jane.doe",
                            "password": "testing7890",
                            "email": "jane.doe@company.com"
                        ]
                    ]
                ]
            ])
        ).bytes)
    }
}
