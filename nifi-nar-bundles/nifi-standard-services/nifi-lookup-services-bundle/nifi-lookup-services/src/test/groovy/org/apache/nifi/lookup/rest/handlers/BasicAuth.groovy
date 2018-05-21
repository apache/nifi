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

        if (!authString || authString != "Basic am9obi5zbWl0aDpudWxs") {
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