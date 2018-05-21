package org.apache.nifi.lookup.rest.handlers

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class SimpleJsonArray extends HttpServlet {
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.contentType = "application/json"
        response.outputStream.write(prettyPrint(
            toJson([[
                username: "john.smith",
                password: "testing1234"
            ],
                [
                    username: "jane.doe",
                    password: "testing7890"
            ]])
        ).bytes)
    }
}
