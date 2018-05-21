package org.apache.nifi.lookup.rest.handlers

import static groovy.json.JsonOutput.*

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class SimpleJson extends HttpServlet {
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.contentType = "application/json"
        response.outputStream.write(prettyPrint(
            toJson([
                username: "john.smith",
                password: "testing1234"
            ])
        ).bytes)
    }
}
