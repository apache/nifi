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
<div id="breadcrumbs">
    <div id="cluster-indicator"></div>
    <div id="data-flow-title-viewport">
        <div id="breadcrumbs-left-border"></div>
        <div id="data-flow-title-container"></div>
        <div id="breadcrumbs-right-border"></div>
    </div>
    <div id="breadcrumbs-background"></div>
</div>
<div id="pan-and-zoom">
    <div id="graph-control-separator">&nbsp;</div>
    <table class="pan">
        <tr>
            <td id="pan-top-left"></td>
            <td id="pan-up-button" title="Pan Up" class="pan-up"></td>
            <td id="pan-top-right"></td>
        </tr>
        <tr>
            <td id="pan-left-button" title="Pan Left" class="pan-left"></td>
            <td id="pan-center"></td>
            <td id="pan-right-button" title="Pan Right" class="pan-right"></td>
        </tr>
        <tr>
            <td id="pan-bottom-left"></td>
            <td id="pan-down-button" title="Pan Down" class="pan-down"></td>
            <td id="pan-bottom-right"></td>
        </tr>
    </table>
    <div id="graph-control-separator">&nbsp;</div>
    <div id="zoom-in-button" title="Zoom In" class="zoom-in"></div>
    <div id="zoom-out-button" title="Zoom Out" class="zoom-out"></div>
    <div id="graph-control-separator">&nbsp;</div>
    <div id="zoom-fit-button" title="Fit" class="fit-image"></div>
    <div id="graph-control-separator">&nbsp;</div>
    <div id="zoom-actual-button" title="Actual Size" class="actual-size"></div>
</div>
<div id="birdseye-collapse" class="birdseye-expanded"></div>
<div id="birdseye-container">
    <div id="birdseye"><!-- inject outline here --></div>
</div>