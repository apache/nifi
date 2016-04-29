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

/* global nf, d3 */

nf.PortConfiguration = (function () {

    /**
     * Initializes the port dialog.
     */
    var initPortConfigurationDialog = function () {
        $('#port-configuration').modal({
            headerText: 'Configure Port',
            overlayBackground: true,
            buttons: [{
                    buttonText: 'Apply',
                    handler: {
                        click: function () {
                            // get the port data to reference the uri
                            var portId = $('#port-id').text();
                            var portData = d3.select('#id-' + portId).datum();

                            // build the updated port
                            var port = {
                                'id': portId,
                                'name': $('#port-name').val(),
                                'comments': $('#port-comments').val()
                            };

                            // include the concurrent tasks if appropriate
                            if ($('#port-concurrent-task-container').is(':visible')) {
                                port['concurrentlySchedulableTaskCount'] = $('#port-concurrent-tasks').val();
                            }

                            // mark the processor disabled if appropriate
                            if ($('#port-enabled').hasClass('checkbox-unchecked')) {
                                port['state'] = 'DISABLED';
                            } else if ($('#port-enabled').hasClass('checkbox-checked')) {
                                port['state'] = 'STOPPED';
                            }
                            
                            // build the port entity
                            var portEntity = {
                                'revision': nf.Client.getRevision(),
                                'component': port
                            };

                            // update the selected component
                            $.ajax({
                                type: 'PUT',
                                data: JSON.stringify(portEntity),
                                url: portData.component.uri,
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                // update the revision
                                nf.Client.setRevision(response.revision);

                                // refresh the port component
                                nf.Port.set(response);

                                // close the details panel
                                $('#port-configuration').modal('hide');
                            }).fail(function (xhr, status, error) {
                                // handle bad request locally to keep the dialog open, allowing the user
                                // to make changes. if the request fails for another reason, the dialog
                                // should be closed so the issue can be addressed (stale flow for instance)
                                if (xhr.status === 400) {
                                    var errors = xhr.responseText.split('\n');

                                    var content;
                                    if (errors.length === 1) {
                                        content = $('<span></span>').text(errors[0]);
                                    } else {
                                        content = nf.Common.formatUnorderedList(errors);
                                    }

                                    nf.Dialog.showOkDialog({
                                        dialogContent: content,
                                        overlayBackground: false,
                                        headerText: 'Configuration Error'
                                    });
                                } else {
                                    // close the details panel
                                    $('#port-configuration').modal('hide');

                                    // handle the error
                                    nf.Common.handleAjaxError(xhr, status, error);
                                }
                            });
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            $('#port-configuration').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the port details
                    $('#port-id').text('');
                    $('#port-name').val('');
                    $('#port-enabled').removeClass('checkbox-unchecked checkbox-checked');
                    $('#port-concurrent-tasks').val('');
                    $('#port-comments').val('');
                }
            }
        }).draggable({
            containment: 'parent',
            handle: '.dialog-header'
        });
    };

    return {
        init: function () {
            initPortConfigurationDialog();
        },
        
        /**
         * Shows the details for the specified selection.
         * 
         * @argument {selection} selection      The selection
         */
        showConfiguration: function (selection) {
            // if the specified component is a port, load its properties
            if (nf.CanvasUtils.isInputPort(selection) || nf.CanvasUtils.isOutputPort(selection)) {
                var selectionData = selection.datum();

                // determine if the enabled checkbox is checked or not
                var portEnableStyle = 'checkbox-checked';
                if (selectionData.component.state === 'DISABLED') {
                    portEnableStyle = 'checkbox-unchecked';
                }

                // show concurrent tasks for root groups only
                if (nf.Canvas.getParentGroupId() === null) {
                    $('#port-concurrent-task-container').show();
                } else {
                    $('#port-concurrent-task-container').hide();
                }

                // populate the port settings
                $('#port-id').text(selectionData.id);
                $('#port-name').val(selectionData.component.name);
                $('#port-enabled').removeClass('checkbox-unchecked checkbox-checked').addClass(portEnableStyle);
                $('#port-concurrent-tasks').val(selectionData.component.concurrentlySchedulableTaskCount);
                $('#port-comments').val(selectionData.component.comments);

                // show the details
                $('#port-configuration').modal('show');
            }
        }
    };
}());