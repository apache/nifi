<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<link rel="stylesheet" href="../nifi/js/codemirror/lib/codemirror.css" type="text/css" />
<link rel="stylesheet" href="../nifi/js/codemirror/addon/fold/foldgutter.css" type="text/css" />
<script type="text/javascript" src="../nifi/js/codemirror/lib/codemirror-compressed.js"></script>
<script type="text/javascript" src="../nifi/js/jquery/jquery-2.1.1.min.js"></script>

<textarea id="codemirror-content"><%= request.getAttribute("content") == null ? "" : org.apache.nifi.util.EscapeUtils.escapeHtml(request.getAttribute("content").toString()) %></textarea>
<span id="codemirror-mode" style="display: none;"><%= org.apache.nifi.util.EscapeUtils.escapeHtml(request.getAttribute("mode").toString()) %></span> 

<script type="text/javascript">
    $(document).ready(function() {
        var mode = $('#codemirror-mode').text();
        
        var field = document.getElementById('codemirror-content');
        var editor = CodeMirror.fromTextArea(field, {
            mode: mode,
            lineNumbers: true,
            matchBrackets: true,
            foldGutter: true,
            gutters: ["CodeMirror-linenumbers", "CodeMirror-foldgutter"],
            readOnly: true
        });
        
        var setEditorSize = function() {
            editor.setSize($(window).width() - 150, $(window).height() - 150);
        };
        
        // reset the editor size when the window changes
        $(window).resize(setEditorSize);
        
        // initialize the editor size
        setEditorSize();
    });
</script>