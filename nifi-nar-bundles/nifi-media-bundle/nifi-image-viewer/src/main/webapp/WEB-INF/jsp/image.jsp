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
<script type="text/javascript" src="../nifi/assets/jquery/dist/jquery.min.js"></script>
<style>
    #image-holder {
        position: absolute;
        right: 50px;
        bottom: 50px;
        left: 100px;
        top: 100px;
        border: 1px solid #aaa;
        overflow: auto;
        background-color: #fff;
        padding: 4px;
    }
</style>
<div id="image-holder"></div>
<script type="text/javascript">
    $(document).ready(function() {
        var ref = $('#ref').text();
        var imgElement = $('<img/>').attr('src', ref);
        $('#image-holder').append(imgElement);
    });
</script>