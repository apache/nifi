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
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<div id="flowfile-details-dialog" layout="column" class="hidden large-dialog">
    <div id="flowfile-details-dialog-content" class="dialog-content">
        <div id="flowfile-details-tabs" class="tab-container"></div>
        <div id="flowfile-details-tabs-content">
            <div id="flowfile-details-tab-content" class="details-tab">
                <span id="flowfile-uri" class="hidden"></span>
                <span id="flowfile-cluster-node-id" class="hidden"></span>
                <div class="settings-left">
                    <div id="flowfile-details">
                        <div class="flowfile-header"><fmt:message key="partials.canvas.flowfile-details-dialog.detail"/></div>
                        <div class="flowfile-detail">
                            <div class="detail-name">UUID</div>
                            <div id="flowfile-uuid" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.filename"/></div>
                            <div id="flowfile-filename" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.filesize"/></div>
                            <div id="flowfile-file-size" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.queuelocation"/></div>
                            <div id="flowfile-queue-position" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.queueduration"/></div>
                            <div id="flowfile-queued-duration" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.lineageduration"/></div>
                            <div id="flowfile-lineage-duration" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.penalized"/></div>
                            <div id="flowfile-penalized" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div id="additional-flowfile-details"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div id="flowfile-content-details" class="content-details">
                        <div class="flowfile-header"><fmt:message key="partials.canvas.flowfile-details-dialog.contentclaim"/></div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.container"/></div>
                            <div id="content-container" class="content-detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.section"/></div>
                            <div id="content-section" class="content-detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.identifier"/></div>
                            <div id="content-identifier" class="content-detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.offset"/></div>
                            <div id="content-offset" class="content-detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name"><fmt:message key="partials.canvas.flowfile-details-dialog.size"/></div>
                            <div id="content-size" class="content-detail-value"></div>
                            <div id="content-bytes" class="content-detail-value hidden"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div id="content-download" class="secondary-button fa fa-download button-icon"><span><fmt:message key="partials.canvas.flowfile-details-dialog.download"/></span></div>
                            <div id="content-view" class="secondary-button fa fa-eye button-icon hidden"><span><fmt:message key="partials.canvas.flowfile-details-dialog.view"/></span></div>
                            <div class="clear"></div>
                        </div>
                    </div>
                </div>
                <div class="clear"></div>
            </div>
            <div id="flowfile-attributes-tab-content" class="details-tab">
                <div id="flowfile-attributes-details">
                    <div id="flowfile-attributes-header" class="flowfile-header"><fmt:message key="partials.canvas.flowfile-details-dialog.attributevalue"/></div>
                    <div class="clear"></div>
                    <div id="flowfile-attributes-container"></div>
                </div>
            </div>
        </div>
    </div>
</div>
