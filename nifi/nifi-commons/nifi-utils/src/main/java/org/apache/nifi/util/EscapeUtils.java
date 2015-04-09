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
package org.apache.nifi.util;

public class EscapeUtils {

    /**
     * Escapes the specified html by replacing &amp;, &lt;, &gt;, &quot;, &#39;, &#x2f; 
     * with their corresponding html entity. If html is null, null is returned.
     * 
     * @param html
     * @return 
     */
    public static String escapeHtml(String html) {
        if (html == null) {
            return null;
        }
        
        html = html.replace("&", "&amp;");
        html = html.replace("<", "&lt;");
        html = html.replace(">", "&gt;");
        html = html.replace("\"", "&quot;");
        html = html.replace("'", "&#39;");
        html = html.replace("/", "&#x2f;");
        
        return html;
    }
}
