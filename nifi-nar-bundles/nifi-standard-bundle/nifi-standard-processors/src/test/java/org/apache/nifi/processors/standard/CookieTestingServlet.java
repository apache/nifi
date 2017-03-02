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

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.DateGenerator;

public class CookieTestingServlet extends HttpServlet {
    public static final String DATEMODE_COOKIE_DEFAULT = "cookieDateDefault";
    public static final String DATEMODE_COOKIE_NOT_TYPICAL = "cookieDateNotTypical";

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String redirect = req.getParameter("redirect");
        if (redirect == null) {
            redirect = "null";
        }

        String dateMode = req.getParameter("datemode");
        if (dateMode == null) {
            dateMode = DATEMODE_COOKIE_DEFAULT;
        }
        switch (dateMode) {
        case DATEMODE_COOKIE_DEFAULT:
        default:
            // standard way of building a cookie header date uses format "EEE, dd-MMM-yy HH:mm:ss z"
            // this results in Set-Cookie: session=abc123; path=/; expires=EEE, dd-MMM-yy HH:mm:ss z; HttpOnly
            Cookie cookie = new Cookie("session", "abc123");
            cookie.setPath("/");
            cookie.setHttpOnly(true);
            cookie.setMaxAge(86400);
            resp.addCookie(cookie);
            break;

        case DATEMODE_COOKIE_NOT_TYPICAL:
            // hacked way of building a cookie header, to get less-often-used date format "EEE, dd MMM yy HH:mm:ss z"
            // this results in Set-Cookie: session=abc123; path=/; expires=EEE, dd MMM yy HH:mm:ss z; HttpOnly
            StringBuilder buf = new StringBuilder("session=abc123; path=/; expires=");
            buf.append(DateGenerator.formatDate(System.currentTimeMillis() + 1000L * 60 * 60 * 24));
            buf.append("; HttpOnly");
            resp.addHeader("Set-Cookie", buf.toString());
        }

        resp.sendRedirect(redirect);
    }

}
