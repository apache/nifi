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
<div id="flowfile-details-dialog" layout="column" class="hidden large-dialog">
    <div id="flowfile-details-dialog-content" class="dialog-content">
        <div id="flowfile-details-tabs" class="tab-container"></div>
        <div id="flowfile-details-tabs-content">
            <div id="flowfile-details-tab-content" class="details-tab">
                <span id="flowfile-uri" class="hidden"></span>
                <span id="flowfile-cluster-node-id" class="hidden"></span>
                <div class="settings-left">
                    <div id="flowfile-details">
                        <div class="flowfile-header">FlowFile Details</div>
                        <div class="flowfile-detail">
                            <div class="detail-name">UUID</div>
                            <div id="flowfile-uuid" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name">Filename</div>
                            <div id="flowfile-filename" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name">File Size</div>
                            <div id="flowfile-file-size" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name">Queue Position</div>
                            <div id="flowfile-queue-position" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name">Queued Duration</div>
                            <div id="flowfile-queued-duration" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name">Lineage Duration</div>
                            <div id="flowfile-lineage-duration" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="detail-name">Penalized</div>
                            <div id="flowfile-penalized" class="detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div id="additional-flowfile-details"></div>
                    </div>
                </div>
                <div class="spacer">&nbsp;</div>
                <div class="settings-right">
                    <div id="flowfile-with-no-content" class="content-details">
                        <div class="flowfile-header">Content Claim</div>
                        <div class="flowfile-info unset">No Content Available</div>
                    </div>
                    <div id="flowfile-content-details" class="content-details">
                        <div class="flowfile-header">Content Claim</div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name">Container</div>
                            <div id="content-container" class="content-detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name">Section</div>
                            <div id="content-section" class="content-detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name">Identifier</div>
                            <div id="content-identifier" class="content-detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name">Offset</div>
                            <div id="content-offset" class="content-detail-value"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div class="content-detail-name">Size</div>
                            <div id="content-size" class="content-detail-value"></div>
                            <div id="content-bytes" class="content-detail-value hidden"></div>
                            <div class="clear"></div>
                        </div>
                        <div class="flowfile-detail">
                            <div id="content-download" class="secondary-button fa fa-download button-icon"><span>Download</span></div>
                            <div id="content-view" class="secondary-button fa fa-eye button-icon hidden"><span>View</span></div>
                            <div class="clear"></div>
                        </div>
                    </div>
                </div>
                <div class="clear"></div>
            </div>
            <div id="flowfile-attributes-tab-content" class="details-tab">
                <div id="flowfile-attributes-details">
                    <div id="flowfile-attributes-header" class="flowfile-header">Attribute Values</div>
                    <div class="clear"></div>
                    <div id="flowfile-attributes-container"></div>
                </div>
            </div>
        </div>
    </div>
</div>
