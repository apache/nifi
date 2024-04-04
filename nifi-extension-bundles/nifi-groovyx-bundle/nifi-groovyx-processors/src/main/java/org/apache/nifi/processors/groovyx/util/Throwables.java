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
package org.apache.nifi.processors.groovyx.util;

import java.lang.reflect.InvocationTargetException;
import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * Class with helper to return simplified human readable error message with one main `at` code position.
 */
public class Throwables {
    /** returns stacktrace as a String */
    public static String stringStackTrace(Throwable e) {
        StringWriter sw = new StringWriter(500);
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.flush();
        sw.flush();
        return sw.toString();
    }

    /**
     * returns error message with one main line from stacktrace
     */
    public static String getMessage(Throwable e) {
        return getMessage(e, null, -1);
    }

    /**
     * returns human readable error message with only one element from stacktrace.
     * The parameter `priority` could identify the stacktrace element.
     * To find stacktrace element tries to find `priority`,
     * then any non-standard java/groovy class.
     * @param e throwable to convert to message
     * @param priority package name, class, or object that could identify the stacktrace element
     * @param maxlen the max length of returned string or -1 for unlimited
     */
    public static String getMessage(Throwable e, Object priority, int maxlen) {

        if (e == null) {
            return null;
        }
        e = getRootException(e);

        StackTraceElement[] trace = e.getStackTrace();
        int traceIndex = -1;

        if (priority != null) {
            if (priority instanceof String) {
                for (int i = 0; i < trace.length; i++) {
                    if (trace[i].getClassName().startsWith((String) priority)) {
                        traceIndex = i;
                        break;
                    }
                }
            } else {
                if (!(priority instanceof Class)) {
                    priority = priority.getClass();
                }

                String cl = ((Class) priority).getName();
                for (int i = 0; i < trace.length; i++) {
                    if (trace[i].getClassName().startsWith(cl)) {
                        traceIndex = i;
                        break;
                    }
                }
                if (traceIndex == -1) {
                    cl = ((Class) priority).getPackage().getName();
                    for (int i = 0; i < trace.length; i++) {
                        if (trace[i].getClassName().startsWith(cl)) {
                            traceIndex = i;
                            break;
                        }
                    }
                }
            }
        }

        if (traceIndex == -1) {
            for (int i = 0; i < trace.length; i++) {
                String cl = trace[i].getClassName();
                if (cl.startsWith("java.") || cl.startsWith("javax.") || cl.startsWith("org.omg.") || cl.startsWith("org.w3c.") || cl.startsWith("org.xml.") || cl.startsWith("groovy.lang.") || cl
                        .startsWith("groovy.util.") || cl.startsWith("org.codehaus.") || cl.startsWith("com.springsource.") || cl.startsWith("org.springframework.") || cl.startsWith("org.apache.")
                        || cl.startsWith("sun.") || cl.startsWith("com.sun.") || cl.startsWith("org.junit.") || cl.startsWith("junit.framework.")

                        ) {
                    //skip standard classes
                } else {
                    traceIndex = i;
                    break;
                }
            }
        }

        if (traceIndex == -1) {
            traceIndex = 0;
        }

        //build message text
        String msg = e.getMessage();
        if (msg == null) {
            msg = "";
        }
        msg = msg.trim();
        //append dot at the end if no others
        if (msg.length() > 0 && ".!:,;?".indexOf(msg.substring(msg.length() - 1)) == -1) {
            msg += ".";
        }

        //exception class name without package
        String msgSuffix = " " + e.getClass().getName().replaceAll("^.*\\.(\\w+)$", "$1") + " at ";
        //append callers line
        if (traceIndex < 0 || traceIndex >= trace.length) {
            System.err.println("Error formatting exception: " + e);
            e.printStackTrace(System.err);
            msgSuffix = e.getClass().getName();
        } else {
            msgSuffix += trace[traceIndex].toString();
        }
        if (maxlen > 0 && msgSuffix.length() + msg.length() > maxlen) {
            if (maxlen > msgSuffix.length() + 2) {
                int newlen = maxlen - msgSuffix.length() - 2;
                if (newlen < msg.length()) {
                    msg = msg.substring(0, newlen);
                }
                msg = msg + ".." + msgSuffix;
            } else if (msg.length() > maxlen) {
                msg = msg.substring(0, maxlen);
            }
        } else {
            msg = msg + msgSuffix;
        }

        return msg;
    }

    private static Throwable getRootException(Throwable e) {
        Throwable t;

        if (e instanceof InvocationTargetException) {
            t = ((InvocationTargetException) e).getTargetException();
        } else if (e instanceof RuntimeException) {
            t = e.getCause();
        } else if (e.getCause() != null && e.getClass().getName().equals(e.getCause().getClass().getName())) {
            t = e.getCause();
        /*
        }else if(e instanceof UserError){
            return e;
        */
        } else {
            return e;
        }

        if (t != null) {
            return getRootException(t);
        }
        return e;
    }
}

