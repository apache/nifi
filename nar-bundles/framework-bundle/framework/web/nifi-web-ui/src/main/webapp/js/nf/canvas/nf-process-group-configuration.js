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
nf.ProcessGroupConfiguration = (function () {

    return {
        init: function () {
            $('#process-group-configuration').modal({
                headerText: 'Configure Process Group',
                overlayBackground: true,
                buttons: [{
                        buttonText: 'Apply',
                        handler: {
                            click: function () {
                                var revision = nf.Client.getRevision();

                                // get the process group data to reference the uri
                                var processGroupId = $('#process-group-id').text();
                                var processGroupData = d3.select('#id-' + processGroupId).datum();

                                // update the selected component
                                $.ajax({
                                    type: 'PUT',
                                    data: {
                                        version: revision.version,
                                        clientId: revision.clientId,
                                        name: $('#process-group-name').val(),
                                        comments: $('#process-group-comments').val()
                                    },
                                    url: processGroupData.component.uri,
                                    dataType: 'json'
                                }).done(function (response) {
                                    if (nf.Common.isDefinedAndNotNull(response.processGroup)) {
                                        // update the revision
                                        nf.Client.setRevision(response.revision);

                                        // refresh the process group
                                        nf.ProcessGroup.set(response.processGroup);

                                        // close the details panel
                                        $('#process-group-configuration').modal('hide');
                                    }
                                }).fail(function (xhr, status, error) {
                                    // close the details panel
                                    $('#process-group-configuration').modal('hide');

                                    // handle the error
                                    nf.Common.handleAjaxError(xhr, status, error);
                                });
                            }
                        }
                    }, {
                        buttonText: 'Cancel',
                        handler: {
                            click: function () {
                                $('#process-group-configuration').modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        // clear the process group details
                        $('#process-group-id').text('');
                        $('#process-group-name').val('');
                        $('#process-group-comments').val('');
                    }
                }
            });
        },
        
        /**
         * Shows the details for the specified selection.
         * 
         * @argument {selection} selection      The selection
         */
        showConfiguration: function (selection) {
            // if the specified selection is a processor, load its properties
            if (nf.CanvasUtils.isProcessGroup(selection)) {
                var selectionData = selection.datum();

                // populate the process group settings
                $('#process-group-id').text(selectionData.component.id);
                $('#process-group-name').val(selectionData.component.name);
                $('#process-group-comments').val(selectionData.component.comments);

                // show the details
                $('#process-group-configuration').modal('show');
            }
        }
    };
}());