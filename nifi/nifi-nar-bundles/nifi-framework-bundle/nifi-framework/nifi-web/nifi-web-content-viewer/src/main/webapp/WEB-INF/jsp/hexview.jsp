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
<link rel="stylesheet" href="js/hexview/hexview.default.css" type="text/css" />
<script type="text/javascript" src="../nifi/js/jquery/jquery-2.1.1.min.js"></script>
<script type="text/javascript" src="js/hexview/hexview.js"></script>

<div id="hexview-content" class="hexviewwindow" title="">
    ${content}
    <form id="hexviewwindow_params">
        <input type="hidden" name="highlights" value="" />
        <input type="hidden" name="row_width" value="16" />
        <input type="hidden" name="word_size" value="1" />
        <input type="hidden" name="hide_0x" value="1" />
        <input type="hidden" name="caption" value="" />
    </form>
</div>
<div id="truncation-message">Showing up to 1.5 KB</div>