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

nf.CanvasHeader = (function () {

    var MIN_TOOLBAR_WIDTH = 640;

    var config = {
        urls: {
            helpDocument: '../nifi-docs/documentation'
        }
    };

    return {
        /**
         * Initialize the canvas header.
         * 
         * @argument {boolean} supportsLogin Whether login is supported
         */
        init: function (supportsLogin) {
            // mouse over for the reporting link
            nf.Common.addHoverEffect('#reporting-link', 'reporting-link', 'reporting-link-hover').click(function () {
                nf.Shell.showPage('summary');
            });

            // mouse over for the counters link
            nf.Common.addHoverEffect('#counters-link', 'counters-link', 'counters-link-hover').click(function () {
                nf.Shell.showPage('counters');
            });

            // mouse over for the history link
            nf.Common.addHoverEffect('#history-link', 'history-link', 'history-link-hover').click(function () {
                nf.Shell.showPage('history');
            });

            // mouse over for the provenance link
            if (nf.Common.canAccessProvenance()) {
                nf.Common.addHoverEffect('#provenance-link', 'provenance-link', 'provenance-link-hover').click(function () {
                    nf.Shell.showPage('provenance');
                });
            } else {
                $('#provenance-link').addClass('provenance-link-disabled');
            }

            // mouse over for the templates link
            nf.Common.addHoverEffect('#templates-link', 'templates-link', 'templates-link-hover').click(function () {
                nf.Shell.showPage('templates?' + $.param({
                    groupId: nf.Canvas.getGroupId()
                }));
            });

            // mouse over for the flow settings link
            nf.Common.addHoverEffect('#flow-settings-link', 'flow-settings-link', 'flow-settings-link-hover').click(function () {
                nf.Settings.loadSettings().done(function () {
                    nf.Settings.showSettings();
                });
            });

            // mouse over for the cluster link
            if (nf.Canvas.isClustered()) {
                nf.Common.addHoverEffect('#cluster-link', 'cluster-link', 'cluster-link-hover').click(function () {
                    nf.Shell.showPage('cluster');
                });

                // show the connected nodes
                $('#connected-nodes-element').show();
            } else {
                $('#cluster-link').hide();
            }

            // mouse over for the reporting link
            nf.Common.addHoverEffect('#bulletin-board-link', 'bulletin-board-link', 'bulletin-board-hover').click(function () {
                nf.Shell.showPage('bulletin-board');
            });

            // setup the refresh link actions
            $('#refresh-required-link').click(function () {
                nf.CanvasHeader.reloadAndClearWarnings();
            });

            // configure the about dialog
            $('#nf-about').modal({
                overlayBackground: true,
                buttons: [{
                        buttonText: 'Ok',
                        handler: {
                            click: function () {
                                $('#nf-about').modal('hide');
                            }
                        }
                    }]
            });

            // show about dialog
            $('#about-link').click(function () {
                $('#nf-about').modal('show');
            });

            // download the help documentation
            $('#help-link').click(function () {
                nf.Shell.showPage(config.urls.helpDocument);
            });

            // login link
            $('#login-link').click(function () {
                nf.Shell.showPage('login', false);
            });
            
            // logout link
            $('#logout-link').click(function () {
                nf.Storage.removeItem("jwt");
                window.location = '/nifi';
            });

            // if the user is not anonymous or accessing via http
            if ($('#current-user').text() !== nf.Common.ANONYMOUS_USER_TEXT || location.protocol === 'http:') {
                $('#login-link-container').css('display', 'none');
            }

            // if accessing via http, don't show the current user
            if (location.protocol === 'http:') {
                $('#current-user-container').css('display', 'none');
            }

            // initialize the new template dialog
            $('#new-template-dialog').modal({
                headerText: 'Create Template',
                overlayBackground: false
            });

            // configure the fill color dialog
            $('#fill-color-dialog').modal({
                headerText: 'Fill',
                overlayBackground: false,
                buttons: [{
                        buttonText: 'Apply',
                        handler: {
                            click: function () {
                                var selection = nf.CanvasUtils.getSelection();

                                // color the selected components
                                selection.each(function (d) {
                                    var selected = d3.select(this);
                                    var selectedData = selected.datum();

                                    // get the color and update the styles
                                    var color = $('#fill-color').minicolors('value');

                                    // ensure the color actually changed
                                    if (color !== selectedData.component.style['background-color']) {
                                        // build the request entity
                                        var entity = {
                                            'revision': nf.Client.getRevision(),
                                            'component': {
                                                'id': selectedData.id,
                                                'style': {
                                                    'background-color': color
                                                }
                                            }
                                        };

                                        // update the style for the specified component
                                        $.ajax({
                                            type: 'PUT',
                                            url: selectedData.component.uri,
                                            data: JSON.stringify(entity),
                                            dataType: 'json',
                                            contentType: 'application/json'
                                        }).done(function (response) {
                                            // update the revision
                                            nf.Client.setRevision(response.revision);

                                            // update the component
                                            nf[selectedData.type].set(response);
                                        }).fail(function (xhr, status, error) {
                                            if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                                                nf.Dialog.showOkDialog({
                                                    dialogContent: nf.Common.escapeHtml(xhr.responseText),
                                                    overlayBackground: true
                                                });
                                            }
                                        });
                                    }
                                });

                                // close the dialog
                                $('#fill-color-dialog').modal('hide');
                            }
                        }
                    }, {
                        buttonText: 'Cancel',
                        handler: {
                            click: function () {
                                // close the dialog
                                $('#fill-color-dialog').modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        // clear the current color
                        $('#fill-color-value').val('');
                        $('#fill-color').minicolors('value', '');
                    }
                }
            }).draggable({
                containment: 'parent',
                handle: '.dialog-header'
            });

            // initialize the fill color picker
            $('#fill-color').minicolors({
                inline: true,
                change: function (hex, opacity) {
                    // update the value
                    $('#fill-color-value').val(hex);

                    // always update the preview
                    $('#fill-color-processor-preview, #fill-color-label-preview').css({
                        'border-color': hex,
                        'background': 'linear-gradient(to bottom, #ffffff, ' + hex + ')',
                        'filter': 'progid:DXImageTransform.Microsoft.gradient(gradientType=0, startColorstr=#ffffff, endColorstr=' + hex + ')'
                    });
                }
            });

            // updates the color if its a valid hex color string
            var updateColor = function () {
                var hex = $('#fill-color-value').val();

                // only update the fill color when its a valid hex color string
                // #[six hex characters|three hex characters] case insensitive
                if (/(^#[0-9A-F]{6}$)|(^#[0-9A-F]{3}$)/i.test(hex)) {
                    $('#fill-color').minicolors('value', hex);
                }
            };

            // apply fill color from field on blur and enter press
            $('#fill-color-value').on('blur', updateColor).on('keyup', function (e) {
                var code = e.keyCode ? e.keyCode : e.which;
                if (code === $.ui.keyCode.ENTER) {
                    updateColor();
                }
            });

            var toolbar = $('#toolbar');
            var groupButton = $('#action-group');
            $(window).on('resize', function () {
                if (toolbar.width() < MIN_TOOLBAR_WIDTH && groupButton.is(':visible')) {
                    toolbar.find('.secondary').hide();
                } else if (toolbar.width() > MIN_TOOLBAR_WIDTH && groupButton.is(':hidden')) {
                    toolbar.find('.secondary').show();
                }
            });

            // set up the initial visibility
            if (toolbar.width() < MIN_TOOLBAR_WIDTH) {
                toolbar.find('.secondary').hide();
            }
        },
        /**
         * Reloads and clears any warnings.
         */
        reloadAndClearWarnings: function () {
            nf.Canvas.reload().done(function () {
                // update component visibility
                nf.Canvas.View.updateVisibility();

                // refresh the birdseye
                nf.Birdseye.refresh();

                // hide the refresh link on the canvas
                $('#stats-last-refreshed').removeClass('alert');
                $('#refresh-required-container').hide();

                // hide the refresh link on the settings
                $('#settings-last-refreshed').removeClass('alert');
                $('#settings-refresh-required-icon').hide();
            }).fail(function () {
                nf.Dialog.showOkDialog({
                    dialogContent: 'Unable to refresh the current group.',
                    overlayBackground: true
                });
            });
        }
    };
}());