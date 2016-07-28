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

nf.ControllerService = (function () {

    var config = {
        edit: 'edit',
        readOnly: 'read-only',
        serviceOnly: 'SERVICE_ONLY',
        serviceAndReferencingComponents: 'SERVICE_AND_REFERENCING_COMPONENTS'
    };

    /**
     * Handle any expected controller service configuration errors.
     *
     * @argument {object} xhr       The XmlHttpRequest
     * @argument {string} status    The status of the request
     * @argument {string} error     The error
     */
    var handleControllerServiceConfigurationError = function (xhr, status, error) {
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
                headerText: 'Controller Service'
            });
        } else {
            nf.Common.handleAjaxError(xhr, status, error);
        }
    };

    /**
     * Determines whether the user has made any changes to the controller service configuration
     * that needs to be saved.
     */
    var isSaveRequired = function () {
        var details = $('#controller-service-configuration').data('controllerServiceDetails');

        // determine if any controller service settings have changed

        if ($('#controller-service-name').val() !== details['name']) {
            return true;
        }
        if ($('#controller-service-comments').val() !== details['comments']) {
            return true;
        }

        // defer to the properties
        return $('#controller-service-properties').propertytable('isSaveRequired');
    };

    /**
     * Marshals the data that will be used to update the contr oller service's configuration.
     */
    var marshalDetails = function () {
        // properties
        var properties = $('#controller-service-properties').propertytable('marshalProperties');

        // create the controller service dto
        var controllerServiceDto = {};
        controllerServiceDto['id'] = $('#controller-service-id').text();
        controllerServiceDto['name'] = $('#controller-service-name').val();
        controllerServiceDto['comments'] = $('#controller-service-comments').val();

        // set the properties
        if ($.isEmptyObject(properties) === false) {
            controllerServiceDto['properties'] = properties;
        }

        // create the controller service entity
        var controllerServiceEntity = {};
        controllerServiceEntity['component'] = controllerServiceDto;

        // return the marshaled details
        return controllerServiceEntity;
    };

    /**
     * Validates the specified details.
     *
     * @argument {object} details       The details to validate
     */
    var validateDetails = function (details) {
        return true;
    };

    /**
     * Reloads the specified controller service. It's referencing and referenced
     * components are NOT reloaded.
     *
     * @param {jQuery} serviceTable
     * @param {string} id
     */
    var reloadControllerService = function (serviceTable, id) {
        // get the table and update the row accordingly
        var controllerServiceGrid = serviceTable.data('gridInstance');
        var controllerServiceData = controllerServiceGrid.getData();
        var controllerServiceEntity = controllerServiceData.getItemById(id);

        // this may happen if controller service A references another controller
        // service B that has been removed. attempting to enable/disable/remove A
        // will attempt to reload B which is no longer a known service
        if (nf.Common.isUndefined(controllerServiceEntity)) {
            return $.Deferred(function (deferred) {
                deferred.reject();
            }).promise();
        }

        return $.ajax({
            type: 'GET',
            url: controllerServiceEntity.uri,
            dataType: 'json'
        }).done(function (response) {
            renderControllerService(serviceTable, response);
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Renders the specified controller service.
     *
     * @param {object} serviceTable
     * @param {object} controllerServiceEntity
     */
    var renderControllerService = function (serviceTable, controllerServiceEntity) {
        // get the table and update the row accordingly
        var controllerServiceGrid = serviceTable.data('gridInstance');
        var controllerServiceData = controllerServiceGrid.getData();
        var currentControllerServiceEntity = controllerServiceData.getItemById(controllerServiceEntity.id);
        controllerServiceData.updateItem(controllerServiceEntity.id, $.extend({
            bulletins: currentControllerServiceEntity.bulletins
        }, controllerServiceEntity));
    };

    /**
     * Reloads the specified controller services and all of its referencing
     * and referenced components.
     *
     * @param {jQuery} serviceTable
     * @param {type} controllerService
     */
    var reloadControllerServiceAndReferencingComponents = function (serviceTable, controllerService) {
        reloadControllerService(serviceTable, controllerService.id).done(function(response) {
            reloadControllerServiceReferences(serviceTable, response.component);
        });
    };

    /**
     * Reloads components that reference this controller service as well as
     * other services that this controller service references.
     *
     * @param {jQuery} serviceTable
     * @param {object} controllerService
     */
    var reloadControllerServiceReferences = function (serviceTable, controllerService) {
        // reload all dependent processors if they are currently visible
        $.each(controllerService.referencingComponents, function (_, referencingComponentEntity) {
            // ensure we can read the referencing component prior to reloading
            if (referencingComponentEntity.permissions.canRead === false) {
                return;
            }

            var reference = referencingComponentEntity.component;
            if (reference.referenceType === 'Processor') {
                // reload the processor on the canvas if appropriate
                if (nf.Canvas.getGroupId() === reference.groupId) {
                    var processor = nf.Processor.get(reference.id);
                    nf.Processor.reload(processor.component);
                }

                // update the current active thread count
                $('div.' + reference.id + '-active-threads').text(reference.activeThreadCount);

                // update the current state of this processor
                var referencingComponentState = $('div.' + reference.id + '-state');
                if (referencingComponentState.length) {
                    updateReferencingSchedulableComponentState(referencingComponentState, reference);
                }
            } else if (reference.referenceType === 'ReportingTask') {
                // reload the referencing reporting tasks
                nf.ReportingTask.reload(reference.id);

                // update the current active thread count
                $('div.' + reference.id + '-active-threads').text(reference.activeThreadCount);

                // update the current state of this reporting task
                var referencingComponentState = $('div.' + reference.id + '-state');
                if (referencingComponentState.length) {
                    updateReferencingSchedulableComponentState(referencingComponentState, reference);
                }
            } else {
                // reload the referencing services
                reloadControllerService(serviceTable, reference.id);
                
                // update the current state of this service
                var referencingComponentState = $('div.' + reference.id + '-state');
                if (referencingComponentState.length) {
                    updateReferencingServiceState(referencingComponentState, reference);
                }

                // consider it's referencing components if appropriate
                if (reference.referenceCycle === false) {
                    reloadControllerServiceReferences(serviceTable, reference);
                }
            }
        });

        // see if this controller service references another controller service
        // in order to update the referenced service referencing components
        nf.ControllerService.reloadReferencedServices(serviceTable, controllerService);
    };   
    
    /**
     * Adds a border to the controller service referencing components if necessary.
     *
     * @argument {jQuery} referenceContainer
     */
    var updateReferencingComponentsBorder = function (referenceContainer) {
        // determine if it is too big
        var tooBig = referenceContainer.get(0).scrollHeight > referenceContainer.innerHeight() ||
            referenceContainer.get(0).scrollWidth > referenceContainer.innerWidth();

        // draw the border if necessary
        if (referenceContainer.is(':visible') && tooBig) {
            referenceContainer.css('border-width', '1px');
        } else {
            referenceContainer.css('border-width', '0px');
        }
    };

    /**
     * Updates the specified bulletinIcon with the specified bulletins if necessary.
     *
     * @param {array} bulletins
     * @param {jQuery} bulletinIcon
     */
    var updateBulletins = function (bulletins, bulletinIcon) {
        var currentBulletins = bulletinIcon.data('bulletins');

        // update the bulletins if necessary
        if (nf.Common.doBulletinsDiffer(currentBulletins, bulletins)) {
            bulletinIcon.data('bulletins', bulletins);

            // format the new bulletins
            var formattedBulletins = nf.Common.getFormattedBulletins(bulletins);

            // if there are bulletins update them
            if (bulletins.length > 0) {
                var list = nf.Common.formatUnorderedList(formattedBulletins);

                // update existing tooltip or initialize a new one if appropriate
                if (bulletinIcon.data('qtip')) {
                    bulletinIcon.qtip('option', 'content.text', list);
                } else {
                    bulletinIcon.addClass('has-bulletins').show().qtip($.extend({},
                        nf.CanvasUtils.config.systemTooltipConfig,
                        {
                            content: list
                        }));
                }
            } else if (bulletinIcon.data('qtip')) {
                bulletinIcon.removeClass('has-bulletins').removeData('bulletins').hide().qtip('api').destroy(true);
            }
        }
    };

    /**
     * Updates the referencingComponentState using the specified referencingComponent.
     *
     * @param {jQuery} referencingComponentState
     * @param {object} referencingComponent
     */
    var updateReferencingSchedulableComponentState = function (referencingComponentState, referencingComponent) {
        referencingComponentState.removeClass('disabled stopped running invalid').addClass(function () {
            var icon = $(this);

            var state = referencingComponent.state.toLowerCase();
            if (state === 'stopped' && !nf.Common.isEmpty(referencingComponent.validationErrors)) {
                state = 'invalid';

                // add tooltip for the warnings
                var list = nf.Common.formatUnorderedList(referencingComponent.validationErrors);
                if (icon.data('qtip')) {
                    icon.qtip('option', 'content.text', list);
                } else {
                    icon.qtip($.extend({},
                        nf.CanvasUtils.config.systemTooltipConfig,
                        {
                            content: list
                        }));
                }
            } else if (icon.data('qtip')) {
                icon.qtip('api').destroy(true);
            }
            return state;
        });
    };

    /**
     * Updates the referencingServiceState using the specified referencingService.
     *
     * @param {jQuery} referencingServiceState
     * @param {object} referencingService
     */
    var updateReferencingServiceState = function (referencingServiceState, referencingService) {
        referencingServiceState.removeClass('disabled enabled invalid').addClass(function () {
            var icon = $(this);

            var state = referencingService.state === 'ENABLED' ? 'enabled' : 'disabled';
            if (state === 'disabled' && !nf.Common.isEmpty(referencingService.validationErrors)) {
                state = 'invalid';

                // add tooltip for the warnings
                var list = nf.Common.formatUnorderedList(referencingService.validationErrors);
                if (icon.data('qtip')) {
                    icon.qtip('option', 'content.text', list);
                } else {
                    icon.qtip($.extend({},
                        nf.CanvasUtils.config.systemTooltipConfig, 
                        {
                            content: list
                        }));
                }
            } else if (icon.data('qtip')) {
                icon.qtip('api').destroy(true);
            }
            return state;
        });
    };

    /**
     * Updates the bulletins for all referencing components.
     *
     * @param {array} bulletins
     */
    var updateReferencingComponentBulletins = function (bulletins) {
        var bulletinsBySource = d3.nest()
            .key(function (d) {
                return d.sourceId;
            })
            .map(bulletins, d3.map);

        bulletinsBySource.forEach(function (sourceId, sourceBulletins) {
            $('div.' + sourceId + '-bulletins').each(function () {
                updateBulletins(sourceBulletins, $(this));
            });
        });
    };

    /**
     * Adds the specified reference for this controller service.
     *
     * @param {jQuery} serviceTable
     * @param {jQuery} referenceContainer 
     * @param {array} referencingComponents
     */
    var createReferencingComponents = function (serviceTable, referenceContainer, referencingComponents) {
        if (nf.Common.isEmpty(referencingComponents)) {
            referenceContainer.append('<div class="unset">No referencing components.</div>');
            return;
        }

        // toggles the visibility of a container
        var toggle = function (twist, container) {
            if (twist.hasClass('expanded')) {
                twist.removeClass('expanded').addClass('collapsed');
                container.hide();
            } else {
                twist.removeClass('collapsed').addClass('expanded');
                container.show();
            }
        };

        var referencingComponentIds = [];
        var processors = $('<ul class="referencing-component-listing clear"></ul>');
        var services = $('<ul class="referencing-component-listing clear"></ul>');
        var tasks = $('<ul class="referencing-component-listing clear"></ul>');
        var unauthorized = $('<ul class="referencing-component-listing clear"></ul>');
        $.each(referencingComponents, function (_, referencingComponentEntity) {
            // check the access policy for this referencing component
            if (referencingComponentEntity.permissions.canRead === false || referencingComponentEntity.permissions.canWrite === false) {
                var unauthorizedReferencingComponent = $('<div class="unset"></div>').text(referencingComponentEntity.id);
                unauthorized.append(unauthorizedReferencingComponent);
            } else {
                var referencingComponent = referencingComponentEntity.component;
                referencingComponentIds.push(referencingComponent.id);

                if (referencingComponent.referenceType === 'Processor') {
                    var processorLink = $('<span class="referencing-component-name link"></span>').text(referencingComponent.name).on('click', function () {
                        // show the component
                        nf.CanvasUtils.showComponent(referencingComponent.groupId, referencingComponent.id);

                        // close the dialog and shell
                        referenceContainer.closest('.dialog').modal('hide');
                        $('#shell-close-button').click();
                    });

                    // state
                    var processorState = $('<div class="referencing-component-state"></div>').addClass(referencingComponent.id + '-state');
                    updateReferencingSchedulableComponentState(processorState, referencingComponent);

                    // bulletin
                    var processorBulletins = $('<div class="referencing-component-bulletins"></div>').addClass(referencingComponent.id + '-bulletins');

                    // type
                    var processorType = $('<span class="referencing-component-type"></span>').text(nf.Common.substringAfterLast(referencingComponent.type, '.'));

                    // active thread count
                    var processorActiveThreadCount = $('<span class="referencing-component-active-thread-count"></span>').addClass(referencingComponent.id + '-active-threads');
                    if (nf.Common.isDefinedAndNotNull(referencingComponent.activeThreadCount) && referencingComponent.activeThreadCount > 0) {
                        processorActiveThreadCount.text('(' + referencingComponent.activeThreadCount + ')');
                    }

                    // processor
                    var processorItem = $('<li></li>').append(processorState).append(processorBulletins).append(processorLink).append(processorType).append(processorActiveThreadCount);
                    processors.append(processorItem);
                } else if (referencingComponent.referenceType === 'ControllerService') {
                    var serviceLink = $('<span class="referencing-component-name link"></span>').text(referencingComponent.name).on('click', function () {
                        var controllerServiceGrid = serviceTable.data('gridInstance');
                        var controllerServiceData = controllerServiceGrid.getData();

                        // select the selected row
                        var row = controllerServiceData.getRowById(referencingComponent.id);
                        controllerServiceGrid.setSelectedRows([row]);
                        controllerServiceGrid.scrollRowIntoView(row);

                        // close the dialog and shell
                        referenceContainer.closest('.dialog').modal('hide');
                    });

                    // container for this service's references
                    var referencingServiceReferencesContainer = $('<div class="referencing-component-references hidden"></div>');
                    var serviceTwist = $('<div class="service expansion-button collapsed pointer"></div>').on('click', function () {
                        if (serviceTwist.hasClass('collapsed')) {
                            var controllerServiceGrid = serviceTable.data('gridInstance');
                            var controllerServiceData = controllerServiceGrid.getData();
                            var referencingService = controllerServiceData.getItemById(referencingComponent.id);

                            // create the markup for the references
                            createReferencingComponents(serviceTable, referencingServiceReferencesContainer, referencingService.referencingComponents);
                        } else {
                            referencingServiceReferencesContainer.empty();
                        }

                        // toggle visibility
                        toggle(serviceTwist, referencingServiceReferencesContainer);

                        // update borders as necessary
                        updateReferencingComponentsBorder(referenceContainer);
                    });

                    // state
                    var serviceState = $('<div class="referencing-component-state"></div>').addClass(referencingComponent.id + '-state');
                    updateReferencingServiceState(serviceState, referencingComponent);

                    // bulletin
                    var serviceBulletins = $('<div class="referencing-component-bulletins"></div>').addClass(referencingComponent.id + '-bulletins');

                    // type
                    var serviceType = $('<span class="referencing-component-type"></span>').text(nf.Common.substringAfterLast(referencingComponent.type, '.'));

                    // service
                    var serviceItem = $('<li></li>').append(serviceTwist).append(serviceState).append(serviceBulletins).append(serviceLink).append(serviceType).append(referencingServiceReferencesContainer);

                    services.append(serviceItem);
                } else if (referencingComponent.referenceType === 'ReportingTask') {
                    var reportingTaskLink = $('<span class="referencing-component-name link"></span>').text(referencingComponent.name).on('click', function () {
                        var reportingTaskGrid = $('#reporting-tasks-table').data('gridInstance');
                        var reportingTaskData = reportingTaskGrid.getData();

                        // select the selected row
                        var row = reportingTaskData.getRowById(referencingComponent.id);
                        reportingTaskGrid.setSelectedRows([row]);
                        reportingTaskGrid.scrollRowIntoView(row);

                        // select the reporting task tab
                        $('#settings-tabs').find('li:last').click();

                        // close the dialog and shell
                        referenceContainer.closest('.dialog').modal('hide');
                    });

                    // state
                    var reportingTaskState = $('<div class="referencing-component-state"></div>').addClass(referencingComponent.id + '-state');
                    updateReferencingSchedulableComponentState(reportingTaskState, referencingComponent);

                    // bulletins
                    var reportingTaskBulletins = $('<div class="referencing-component-bulletins"></div>').addClass(referencingComponent.id + '-bulletins');

                    // type
                    var reportingTaskType = $('<span class="referencing-component-type"></span>').text(nf.Common.substringAfterLast(referencingComponent.type, '.'));

                    // active thread count
                    var reportingTaskActiveThreadCount = $('<span class="referencing-component-active-thread-count"></span>').addClass(referencingComponent.id + '-active-threads');
                    if (nf.Common.isDefinedAndNotNull(referencingComponent.activeThreadCount) && referencingComponent.activeThreadCount > 0) {
                        reportingTaskActiveThreadCount.text('(' + referencingComponent.activeThreadCount + ')');
                    }

                    // reporting task
                    var reportingTaskItem = $('<li></li>').append(reportingTaskState).append(reportingTaskBulletins).append(reportingTaskLink).append(reportingTaskType).append(reportingTaskActiveThreadCount);
                    tasks.append(reportingTaskItem);
                }
            }
        });

        // query for the bulletins
        queryBulletins(referencingComponentIds).done(function (response) {
            var bulletins = response.bulletinBoard.bulletins;
            updateReferencingComponentBulletins(bulletins);
        });

        // create the collapsable listing for each type
        var createReferenceBlock = function (titleText, list) {
            if (list.is(':empty')) {
                list.remove();
                return;
            }

            var twist = $('<div class="expansion-button expanded"></div>');
            var title = $('<span class="referencing-component-title"></span>').text(titleText);
            var count = $('<span class="referencing-component-count"></span>').text('(' + list.children().length + ')');

            // create the reference block
            $('<div class="referencing-component-block pointer unselectable"></div>').on('click', function () {
                // toggle this block
                toggle(twist, list);

                // update the border if necessary
                updateReferencingComponentsBorder(referenceContainer);
            }).append(twist).append(title).append(count).appendTo(referenceContainer);

            // add the listing
            list.appendTo(referenceContainer);
        };

        // create blocks for each type of component
        createReferenceBlock('Processors', processors);
        createReferenceBlock('Reporting Tasks', tasks);
        createReferenceBlock('Controller Services', services);
        createReferenceBlock('Unauthorized', unauthorized);
    };

    /**
     * Queries for bulletins for the specified components.
     *
     * @param {array} componentIds
     * @returns {deferred}
     */
    var queryBulletins = function (componentIds) {
        var ids = componentIds.join('|');

        return $.ajax({
            type: 'GET',
            url: '../nifi-api/flow/bulletin-board',
            data: {
                sourceId: ids
            },
            dataType: 'json'
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Sets whether the specified controller service is enabled.
     *
     * @param {jQuery} serviceTable
     * @param {object} controllerServiceEntity
     * @param {boolean} enabled
     * @param {function} pollCondition
     */
    var setEnabled = function (serviceTable, controllerServiceEntity, enabled, pollCondition) {
        // build the request entity
        var updateControllerServiceEntity = {
            'revision': nf.Client.getRevision(controllerServiceEntity),
            'component': {
                'id': controllerServiceEntity.id,
                'state': enabled ? 'ENABLED' : 'DISABLED'
            }
        };

        var updated = $.ajax({
            type: 'PUT',
            url: controllerServiceEntity.uri,
            data: JSON.stringify(updateControllerServiceEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            renderControllerService(serviceTable, response);
        }).fail(nf.Common.handleAjaxError);

        // wait until the polling of each service finished
        return $.Deferred(function (deferred) {
            updated.done(function () {
                var serviceUpdated = pollService(controllerServiceEntity, function (service, bulletins) {
                    if ($.isArray(bulletins)) {
                        if (enabled) {
                            updateBulletins(bulletins, $('#enable-controller-service-bulletins'));
                        } else {
                            updateBulletins(bulletins, $('#disable-controller-service-bulletins'));
                        }
                    }

                    // the condition is met once the service is (ENABLING or ENABLED)/DISABLED
                    if (enabled) {
                        return service.state === 'ENABLING' || service.state === 'ENABLED';
                    } else {
                        return service.state === 'DISABLED';
                    }
                }, function (service) {
                    return queryBulletins([service.id]);
                }, pollCondition);

                // once the service has updated, resolve and render the updated service
                serviceUpdated.done(function () {
                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }).fail(function () {
                deferred.reject();
            });
        }).promise();
    };

    /**
     * Gets the id's of all controller services referencing the specified controller service.
     *
     * @param {object} controllerService
     */
    var getReferencingControllerServiceIds = function (controllerService) {
        var ids = d3.set();
        ids.add(controllerService.id);

        var checkReferencingServices = function (referencingComponents) {
            $.each(referencingComponents, function (_, referencingComponentEntity) {
                var referencingComponent = referencingComponentEntity.component;
                if (referencingComponent.referenceType === 'ControllerService') {
                    // add the id
                    ids.add(referencingComponent.id);

                    // consider it's referencing components if appropriate
                    if (referencingComponent.referenceCycle === false) {
                        checkReferencingServices(referencingComponent.referencingComponents);
                    }
                }
            });
        };

        // check the referencing servcies
        checkReferencingServices(controllerService.referencingComponents);
        return ids;
    };

    /**
     * Updates the scheduled state of the processors/reporting tasks referencing
     * the specified controller service.
     *
     * @param {jQuery} serviceTable
     * @param {object} controllerServiceEntity
     * @param {boolean} running
     * @param {function} pollCondition
     */
    var updateReferencingSchedulableComponents = function (serviceTable, controllerServiceEntity, running, pollCondition) {
        var referenceEntity = {
            'id': controllerServiceEntity.id,
            'state': running ? 'RUNNING' : 'STOPPED',
            'referencingComponentRevisions': {}
        };

        // include the revision of each referencing component
        $.each(controllerServiceEntity.component.referencingComponents, function (_, referencingComponent) {
            referenceEntity.referencingComponentRevisions[referencingComponent.id] = nf.Client.getRevision(referencingComponent);
        });

        // issue the request to update the referencing components
        var updated = $.ajax({
            type: 'PUT',
            url: controllerServiceEntity.uri + '/references',
            data: JSON.stringify(referenceEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).fail(nf.Common.handleAjaxError);

        // Note: updated revisions will be retrieved after updateReferencingSchedulableComponents is invoked

        // wait unil the polling of each service finished
        return $.Deferred(function (deferred) {
            updated.done(function (response) {
                // update the controller service
                controllerServiceEntity.component.referencingComponents = response.controllerServiceReferencingComponents;

                // if we're just starting schedulable components we're done when the update is finished
                if (running) {
                    deferred.resolve();
                } else {
                    // identify all referencing services
                    var services = getReferencingControllerServiceIds(controllerServiceEntity.component);

                    // get the controller service grid
                    var controllerServiceGrid = serviceTable.data('gridInstance');
                    var controllerServiceData = controllerServiceGrid.getData();

                    // start polling for each controller service
                    var polling = [];
                    services.forEach(function (controllerServiceId) {
                        var referencingService = controllerServiceData.getItemById(controllerServiceId);
                        polling.push(stopReferencingSchedulableComponents(referencingService, pollCondition));
                    });

                    // wait until polling has finished
                    $.when.apply(window, polling).done(function () {
                        deferred.resolve();
                    }).fail(function () {
                        deferred.reject();
                    });
                }
            }).fail(function () {
                deferred.reject();
            });
        }).promise();
    };

    /**
     * Polls the specified services referencing components to see if the
     * specified condition is satisfied.
     *
     * @param {object} controllerServiceEntity
     * @param {function} completeCondition
     * @param {function} bulletinDeferred
     * @param {function} pollCondition
     */
    var pollService = function (controllerServiceEntity, completeCondition, bulletinDeferred, pollCondition) {
        var controllerService = controllerServiceEntity.component;

        // we want to keep polling until the condition is met
        return $.Deferred(function (deferred) {
            var current = 2;
            var getTimeout = function () {
                var val = current;

                // update the current timeout for the next time
                current = Math.min(current * 2, 4);

                return val * 1000;
            };

            // polls for the current status of the referencing components
            var poll = function () {
                var bulletins = bulletinDeferred(controllerService);
                var service = $.ajax({
                    type: 'GET',
                    url: controllerServiceEntity.uri,
                    dataType: 'json'
                });

                $.when(bulletins, service).done(function (bulletinResult, serviceResult) {
                    var bulletinResponse = bulletinResult[0];
                    var serviceResponse = serviceResult[0];
                    conditionMet(serviceResponse.component, bulletinResponse.bulletinBoard.bulletins);
                }).fail(function (xhr, status, error) {
                    deferred.reject();
                    nf.Common.handleAjaxError(xhr, status, error);
                });
            };

            // tests to if the condition has been met
            var conditionMet = function (service, bulletins) {
                if (completeCondition(service, bulletins)) {
                    deferred.resolve();
                } else {
                    if (typeof pollCondition === 'function' && pollCondition()) {
                        setTimeout(poll, getTimeout());
                    } else {
                        deferred.reject();
                    }
                }
            };

            // poll for the status of the referencing components
            bulletinDeferred(controllerService).done(function (response) {
                conditionMet(controllerService, response.bulletinBoard.bulletins);
            }).fail(function (xhr, status, error) {
                deferred.reject();
                nf.Common.handleAjaxError(xhr, status, error);
            });
        }).promise();
    };

    /**
     * Continues to poll the specified controller service until all referencing schedulable
     * components are stopped (not scheduled and 0 active threads).
     *
     * @param {object} controllerService
     * @param {function} pollCondition
     */
    var stopReferencingSchedulableComponents = function (controllerService, pollCondition) {
        // continue to poll the service until all schedulable components have stopped
        return pollService(controllerService, function (service, bulletins) {
            var referencingComponents = service.referencingComponents;

            var stillRunning = false;
            $.each(referencingComponents, function (_, referencingComponentEntity) {
                var referencingComponent = referencingComponentEntity.component;
                if (referencingComponent.referenceType === 'Processor' || referencingComponent.referenceType === 'ReportingTask') {
                    if (referencingComponent.state === 'RUNNING' || referencingComponent.activeThreadCount > 0) {
                        stillRunning = true;
                    }

                    // update the current active thread count
                    $('div.' + referencingComponent.id + '-active-threads').text(referencingComponent.activeThreadCount);

                    // update the current state of this component
                    var referencingComponentState = $('div.' + referencingComponent.id + '-state');
                    updateReferencingSchedulableComponentState(referencingComponentState, referencingComponent);
                }
            });

            // query for the bulletins
            updateReferencingComponentBulletins(bulletins);

            // condition is met once all referencing are not running
            return stillRunning === false;
        }, function (service) {
            var referencingSchedulableComponents = [];

            var referencingComponents = service.referencingComponents;
            $.each(referencingComponents, function (_, referencingComponentEntity) {
                var referencingComponent = referencingComponentEntity.component;
                if (referencingComponent.referenceType === 'Processor' || referencingComponent.referenceType === 'ReportingTask') {
                    referencingSchedulableComponents.push(referencingComponent.id);
                }
            });

            return queryBulletins(referencingSchedulableComponents);
        }, pollCondition);
    };

    /**
     * Continues to poll until all referencing services are enabled.
     *
     * @param {object} controllerService
     * @param {function} pollCondition
     */
    var enableReferencingServices = function (controllerService, pollCondition) {
        // continue to poll the service until all referencing services are enabled
        return pollService(controllerService, function (service, bulletins) {
            var referencingComponents = service.referencingComponents;

            var notEnabled = false;
            $.each(referencingComponents, function (_, referencingComponentEntity) {
                var referencingComponent = referencingComponentEntity.component;
                if (referencingComponent.referenceType === 'ControllerService') {
                    if (referencingComponent.state !== 'ENABLING' && referencingComponent.state !== 'ENABLED') {
                        notEnabled = true;
                    }

                    // update the state of the referencing service
                    var referencingServiceState = $('div.' + referencingComponent.id + '-state');
                    updateReferencingServiceState(referencingServiceState, referencingComponent);
                }
            });

            // query for the bulletins
            updateReferencingComponentBulletins(bulletins);

            // condition is met once all referencing are not disabled
            return notEnabled === false;
        }, function (service) {
            var referencingSchedulableComponents = [];

            var referencingComponents = service.referencingComponents;
            $.each(referencingComponents, function (_, referencingComponentEntity) {
                var referencingComponent = referencingComponentEntity.component;
                if (referencingComponent.referenceType === 'ControllerService') {
                    referencingSchedulableComponents.push(referencingComponent.id);
                }
            });

            return queryBulletins(referencingSchedulableComponents);
        }, pollCondition);
    };

    /**
     * Continues to poll until all referencing services are disabled.
     *
     * @param {object} controllerService
     * @param {function} pollCondition
     */
    var disableReferencingServices = function (controllerService, pollCondition) {
        // continue to poll the service until all referencing services are disabled
        return pollService(controllerService, function (service, bulletins) {
            var referencingComponents = service.referencingComponents;

            var notDisabled = false;
            $.each(referencingComponents, function (_, referencingComponentEntity) {
                var referencingComponent = referencingComponentEntity.component;
                if (referencingComponent.referenceType === 'ControllerService') {
                    if (referencingComponent.state !== 'DISABLED') {
                        notDisabled = true;
                    }

                    // update the state of the referencing service
                    var referencingServiceState = $('div.' + referencingComponent.id + '-state');
                    updateReferencingServiceState(referencingServiceState, referencingComponent);
                }
            });

            // query for the bulletins
            updateReferencingComponentBulletins(bulletins);

            // condition is met once all referencing are not enabled
            return notDisabled === false;
        }, function (service) {
            var referencingSchedulableComponents = [];

            var referencingComponents = service.referencingComponents;
            $.each(referencingComponents, function (_, referencingComponentEntity) {
                var referencingComponent = referencingComponentEntity.component;
                if (referencingComponent.referenceType === 'ControllerService') {
                    referencingSchedulableComponents.push(referencingComponent.id);
                }
            });

            return queryBulletins(referencingSchedulableComponents);
        }, pollCondition);
    };

    /**
     * Updates the referencing services with the specified state.
     *
     * @param {jQuery} serviceTable
     * @param {object} controllerServiceEntity
     * @param {boolean} enabled
     * @param {function} pollCondition
     */
    var updateReferencingServices = function (serviceTable, controllerServiceEntity, enabled, pollCondition) {
        // build the reference entity
        var referenceEntity = {
            'id': controllerServiceEntity.id,
            'state': enabled ? 'ENABLED' : 'DISABLED',
            'referencingComponentRevisions': {}
        };

        // include the revision of each referencing component
        $.each(controllerServiceEntity.component.referencingComponents, function (_, referencingComponent) {
            referenceEntity.referencingComponentRevisions[referencingComponent.id] = nf.Client.getRevision(referencingComponent);
        });

        // issue the request to update the referencing components
        var updated = $.ajax({
            type: 'PUT',
            url: controllerServiceEntity.uri + '/references',
            data: JSON.stringify(referenceEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).fail(nf.Common.handleAjaxError);

        // Note: updated revisions will be retrieved after updateReferencingServices is invoked

        // wait unil the polling of each service finished
        return $.Deferred(function (deferred) {
            updated.done(function (response) {
                // update the controller service
                controllerServiceEntity.component.referencingComponents = response.controllerServiceReferencingComponents;

                // identify all referencing services
                var services = getReferencingControllerServiceIds(controllerServiceEntity.component);

                // get the controller service grid
                var controllerServiceGrid = serviceTable.data('gridInstance');
                var controllerServiceData = controllerServiceGrid.getData();

                // start polling for each controller service
                var polling = [];
                services.forEach(function (controllerServiceId) {
                    var referencingService = controllerServiceData.getItemById(controllerServiceId);

                    if (enabled) {
                        polling.push(enableReferencingServices(referencingService, pollCondition));
                    } else {
                        polling.push(disableReferencingServices(referencingService, pollCondition));
                    }
                });

                $.when.apply(window, polling).done(function () {
                    deferred.resolve();
                }).fail(function () {
                    deferred.reject();
                });
            }).fail(function () {
                deferred.reject();
            });
        }).promise();
    };

    /**
     * Shows the dialog for disabling a controller service.
     *
     * @argument {object} controllerService The controller service to disable
     */
    var showDisableControllerServiceDialog = function (serviceTable, controllerService) {
        // populate the disable controller service dialog
        $('#disable-controller-service-id').text(controllerService.id);
        $('#disable-controller-service-name').text(controllerService.name);

        // load the controller referencing components list
        var referencingComponentsContainer = $('#disable-controller-service-referencing-components');
        createReferencingComponents(serviceTable, referencingComponentsContainer, controllerService.referencingComponents);

        var hasUnauthorized = false;
        $.each(controllerService.referencingComponents, function (_, referencingComponent) {
            if (referencingComponent.permissions.canRead === false || referencingComponent.permissions.canWrite === false) {
                hasUnauthorized = true;
                return false;
            }
        });

        // build the button model
        var buttons = [];

        if (hasUnauthorized === false) {
            buttons.push({
                buttonText: 'Disable',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        disableHandler(serviceTable);
                    }
                }
            });
        }

        buttons.push({
            buttonText: 'Cancel',
            color: {
                base: '#E3E8EB',
                hover: '#C7D2D7',
                text: '#004849'
            },
            handler: {
                click: closeModal
            }
        });

        // show the dialog
        $('#disable-controller-service-dialog').modal('setButtonModel', buttons).modal('show');

        // load the bulletins
        queryBulletins([controllerService.id]).done(function (response) {
            updateBulletins(response.bulletinBoard.bulletins, $('#disable-controller-service-bulletins'));
        });

        // update the border if necessary
        updateReferencingComponentsBorder(referencingComponentsContainer);
    };

    /**
     * Shows the dialog for enabling a controller service.
     *
     * @param {object} serviceTable
     * @param {object} controllerService
     */
    var showEnableControllerServiceDialog = function (serviceTable, controllerService) {
        // populate the disable controller service dialog
        $('#enable-controller-service-id').text(controllerService.id);
        $('#enable-controller-service-name').text(controllerService.name);

        // load the controller referencing components list
        var referencingComponentsContainer = $('#enable-controller-service-referencing-components');
        createReferencingComponents(serviceTable, referencingComponentsContainer, controllerService.referencingComponents);

        // build the button model
        var buttons = [{
            buttonText: 'Enable',
            color: {
                base: '#728E9B',
                hover: '#004849',
                text: '#ffffff'
            },
            handler: {
                click: function () {
                    enableHandler(serviceTable);
                }
            }
        }, {
            buttonText: 'Cancel',
            color: {
                base: '#E3E8EB',
                hover: '#C7D2D7',
                text: '#004849'
            },
            handler: {
                click: closeModal
            }
        }];

        // show the dialog
        $('#enable-controller-service-dialog').modal('setButtonModel', buttons).modal('show');
        
        // load the bulletins
        queryBulletins([controllerService.id]).done(function (response) {
            updateBulletins(response.bulletinBoard.bulletins, $('#enable-controller-service-bulletins'));
        });

        // update the border if necessary
        updateReferencingComponentsBorder(referencingComponentsContainer);
    };

    /**
     * Used to handle closing a modal dialog
     */
    var closeModal = function () {
        $(this).modal('hide');
    };

    /**
     * Handles the disable action of the disable controller service dialog.
     *
     * @param {jQuery} serviceTable
     */
    var disableHandler = function(serviceTable) {
        var disableDialog = $('#disable-controller-service-dialog');
        var canceled = false;

        // only provide a cancel option
        disableDialog.modal('setButtonModel', [{
            buttonText: 'Cancel',
            color: {
                base: '#E3E8EB',
                hover: '#C7D2D7',
                text: '#004849'
            },
            handler: {
                click: function () {
                    canceled = true;
                    disableDialog.modal('setButtonModel', []);
                    $('#disable-controller-service-dialog div.controller-service-canceling').show();
                }
            }
        }]);

        // show the progress
        $('#disable-controller-service-scope-container').hide();
        $('#disable-controller-service-progress-container').show();

        // get the controller service
        var controllerServiceId = $('#disable-controller-service-id').text();
        var controllerServiceGrid = serviceTable.data('gridInstance');
        var controllerServiceData = controllerServiceGrid.getData();
        var controllerServiceEntity = controllerServiceData.getItemById(controllerServiceId);
        var controllerService = controllerServiceEntity.component;

        // whether or not to continue polling
        var continuePolling = function () {
            return canceled === false;
        };

        // sets the close button on the dialog
        var setCloseButton = function () {
            $('#disable-controller-service-dialog div.controller-service-canceling').hide();
            disableDialog.modal('setButtonModel', [{
                buttonText: 'Close',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: closeModal
                }
            }]);
        };

        $('#disable-progress-label').text('Steps to disable ' + controllerService.name);
        var disableReferencingSchedulable = $('#disable-referencing-schedulable').addClass('ajax-loading');

        $.Deferred(function (deferred) {
            // stop all referencing schedulable components
            var stopped = updateReferencingSchedulableComponents(serviceTable, controllerServiceEntity, false, continuePolling);

            // once everything has stopped
            stopped.done(function () {
                disableReferencingSchedulable.removeClass('ajax-loading').addClass('ajax-complete');
                var disableReferencingServices = $('#disable-referencing-services').addClass('ajax-loading');

                // disable all referencing services
                var disabled = updateReferencingServices(serviceTable, controllerServiceEntity, false, continuePolling);

                // everything is disabled
                disabled.done(function () {
                    disableReferencingServices.removeClass('ajax-loading').addClass('ajax-complete');
                    var disableControllerService = $('#disable-controller-service').addClass('ajax-loading');

                    // disable this service
                    setEnabled(serviceTable, controllerServiceEntity, false, continuePolling).done(function () {
                        deferred.resolve();
                        disableControllerService.removeClass('ajax-loading').addClass('ajax-complete');
                    }).fail(function () {
                        deferred.reject();
                        disableControllerService.removeClass('ajax-loading').addClass('ajax-error');
                    });
                }).fail(function () {
                    deferred.reject();
                    disableReferencingServices.removeClass('ajax-loading').addClass('ajax-error');
                });
            }).fail(function () {
                deferred.reject();
                disableReferencingSchedulable.removeClass('ajax-loading').addClass('ajax-error');
            });
        }).always(function () {
            reloadControllerServiceAndReferencingComponents(serviceTable, controllerService);
            setCloseButton();

            // inform the user if the action was canceled
            if (canceled === true && $('#nf-ok-dialog').not(':visible')) {
                nf.Dialog.showOkDialog({
                    headerText: 'Controller Service',
                    dialogContent: 'The request to disable has been canceled. Parts of this request may have already completed. Please verify the state of this service and all referencing components.'
                });
            }
        });
    };

    /**
     * Handles the enable action of the enable controller service dialog.
     *
     * @param {jQuery} serviceTable
     */
    var enableHandler = function(serviceTable) {
        var enableDialog = $('#enable-controller-service-dialog');
        var canceled = false;

        // get the controller service
        var controllerServiceId = $('#enable-controller-service-id').text();
        var controllerServiceGrid = serviceTable.data('gridInstance');
        var controllerServiceData = controllerServiceGrid.getData();
        var controllerServiceEntity = controllerServiceData.getItemById(controllerServiceId);
        var controllerService = controllerServiceEntity.component;

        var hasUnauthorized = false;
        $.each(controllerService.referencingComponents, function (_, referencingComponent) {
            if (referencingComponent.permissions.canRead === false || referencingComponent.permissions.canWrite === false) {
                hasUnauthorized = true;
                return false;
            }
        });

        // determine if we want to also activate referencing components
        var scope = $('#enable-controller-service-scope').combo('getSelectedOption').value;

        // ensure appropriate access
        if (scope === config.serviceAndReferencingComponents && hasUnauthorized) {
            nf.Dialog.showOkDialog({
                headerText: 'Controller Service',
                dialogContent: 'Unable to enable due to unauthorized referencing components.'
            });
            return;
        }

        // update visibility
        if (scope === config.serviceOnly) {
            $('#enable-controller-service-progress li.referencing-component').hide();
        }

        // only provide a cancel option
        enableDialog.modal('setButtonModel', [{
            buttonText: 'Cancel',
            color: {
                base: '#E3E8EB',
                hover: '#C7D2D7',
                text: '#004849'
            },
            handler: {
                click: function () {
                    canceled = true;
                    enableDialog.modal('setButtonModel', []);
                    $('#enable-controller-service-dialog div.controller-service-canceling').show();
                }
            }
        }]);

        // show the progress
        $('#enable-controller-service-scope-container').hide();
        $('#enable-controller-service-progress-container').show();


        // whether or not to continue polling
        var continuePolling = function () {
            return canceled === false;
        };

        // sets the button to close
        var setCloseButton = function () {
            $('#enable-controller-service-dialog div.controller-service-canceling').hide();
            enableDialog.modal('setButtonModel', [{
                buttonText: 'Close',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: closeModal
                }
            }]);
        };

        $('#enable-progress-label').text('Steps to enable ' + controllerService.name);
        var enableControllerService = $('#enable-controller-service').addClass('ajax-loading');

        $.Deferred(function (deferred) {
            // enable this controller service
            var enable = setEnabled(serviceTable, controllerServiceEntity, true, continuePolling);

            if (scope === config.serviceAndReferencingComponents) {
                // once the service is enabled, activate all referencing components
                enable.done(function () {
                    enableControllerService.removeClass('ajax-loading').addClass('ajax-complete');
                    var enableReferencingServices = $('#enable-referencing-services').addClass('ajax-loading');

                    // enable the referencing services
                    var servicesEnabled = updateReferencingServices(serviceTable, controllerServiceEntity, true, continuePolling);

                    // once all the referencing services are enbled
                    servicesEnabled.done(function () {
                        enableReferencingServices.removeClass('ajax-loading').addClass('ajax-complete');
                        var enableReferencingSchedulable = $('#enable-referencing-schedulable').addClass('ajax-loading');

                        // start all referencing schedulable components
                        updateReferencingSchedulableComponents(serviceTable, controllerServiceEntity, true, continuePolling).done(function() {
                            deferred.resolve();
                            enableReferencingSchedulable.removeClass('ajax-loading').addClass('ajax-complete');
                        }).fail(function () {
                            deferred.reject();
                            enableReferencingSchedulable.removeClass('ajax-loading').addClass('ajax-error');
                        });
                    }).fail(function () {
                        deferred.reject();
                        enableReferencingServices.removeClass('ajax-loading').addClass('ajax-error');
                    });
                }).fail(function () {
                    deferred.reject();
                    enableControllerService.removeClass('ajax-loading').addClass('ajax-error');
                });
            } else {
                enable.done(function () {
                    deferred.resolve();
                    enableControllerService.removeClass('ajax-loading').addClass('ajax-complete');
                }).fail(function () {
                    deferred.reject();
                    enableControllerService.removeClass('ajax-loading').addClass('ajax-error');
                });
            }
        }).always(function () {
            reloadControllerServiceAndReferencingComponents(serviceTable, controllerService);
            setCloseButton();

            // inform the user if the action was canceled
            if (canceled === true && $('#nf-ok-dialog').not(':visible')) {
                nf.Dialog.showOkDialog({
                    headerText: 'Controller Service',
                    dialogContent: 'The request to enable has been canceled. Parts of this request may have already completed. Please verify the state of this service and all referencing components.'
                });
            }
        });
    };

    /**
     * Gets a property descriptor for the controller service currently being configured.
     *
     * @param {type} propertyName
     */
    var getControllerServicePropertyDescriptor = function (propertyName) {
        var controllerServiceEntity = $('#controller-service-configuration').data('controllerServiceDetails');
        return $.ajax({
            type: 'GET',
            url: controllerServiceEntity.uri + '/descriptors',
            data: {
                propertyName: propertyName
            },
            dataType: 'json'
        }).fail(nf.Common.handleAjaxError);
    };

    /**
     * Goes to a service configuration from the property table.
     *
     * @param {jQuery} serviceTable
     */
    var goToServiceFromProperty = function (serviceTable) {
        return $.Deferred(function (deferred) {
            // close all fields currently being edited
            $('#controller-service-properties').propertytable('saveRow');

            // determine if changes have been made
            if (isSaveRequired()) {
                // see if those changes should be saved
                nf.Dialog.showYesNoDialog({
                    headerText: 'Save',
                    dialogContent: 'Save changes before going to this Controller Service?',
                    noHandler: function () {
                        deferred.resolve();
                    },
                    yesHandler: function () {
                        var controllerServiceEntity = $('#controller-service-configuration').data('controllerServiceDetails');
                        saveControllerService(serviceTable, controllerServiceEntity).done(function () {
                            deferred.resolve();
                        }).fail(function () {
                            deferred.reject();
                        });
                    }
                });
            } else {
                deferred.resolve();
            }
        }).promise();
    };
    
    var saveControllerService = function (serviceTable, controllerServiceEntity) {
        // marshal the settings and properties and update the controller service
        var updatedControllerService = marshalDetails();

        // ensure details are valid as far as we can tell
        if (validateDetails(updatedControllerService)) {
            updatedControllerService['revision'] = nf.Client.getRevision(controllerServiceEntity);

            var previouslyReferencedServiceIds = [];
            $.each(identifyReferencedServiceDescriptors(controllerServiceEntity.component), function (_, descriptor) {
                var modifyingService = !nf.Common.isUndefined(updatedControllerService.component.properties) && !nf.Common.isUndefined(updatedControllerService.component.properties[descriptor.name]);
                var isCurrentlyConfigured = nf.Common.isDefinedAndNotNull(controllerServiceEntity.component.properties[descriptor.name]);

                // if we are attempting to update a controller service reference
                if (modifyingService && isCurrentlyConfigured) {

                    // record the current value if set
                    previouslyReferencedServiceIds.push(controllerServiceEntity.component.properties[descriptor.name]);
                }
            });

            // update the selected component
            return $.ajax({
                type: 'PUT',
                data: JSON.stringify(updatedControllerService),
                url: controllerServiceEntity.uri,
                dataType: 'json',
                contentType: 'application/json'
            }).done(function (response) {
                // reload the controller service
                renderControllerService(serviceTable, response);
                
                // reload all previously referenced controller services
                $.each(previouslyReferencedServiceIds, function(_, oldServiceReferenceId) {
                    reloadControllerService(serviceTable, oldServiceReferenceId);
                });
            }).fail(handleControllerServiceConfigurationError);
        } else {
            return $.Deferred(function (deferred) {
                deferred.reject();
            }).promise();
        }
    };

    /**
     * Identifies the descriptors that identify controller services.
     *
     * @param {object} component
     */
    var identifyReferencedServiceDescriptors = function (component) {
        var referencedServiceDescriptors = [];

        $.each(component.descriptors, function (_, descriptor) {
            if (nf.Common.isDefinedAndNotNull(descriptor.identifiesControllerService)) {
                referencedServiceDescriptors.push(descriptor);
            }
        });

        return referencedServiceDescriptors;
    };

    /**
     * Identifies descritpors that reference controller services.
     *
     * @param {object} component
     */
    var getReferencedServices = function (component) {
        var referencedServices = [];

        $.each(identifyReferencedServiceDescriptors(component), function (_, descriptor) {
            var referencedServiceId = component.properties[descriptor.name];

            // ensure the property is configured
            if (nf.Common.isDefinedAndNotNull(referencedServiceId) && $.trim(referencedServiceId).length > 0) {
                referencedServices.push(referencedServiceId);
            }
        });

        return referencedServices;
    };

    return {
        /**
         * Initializes the controller service configuration dialog.
         */
        init: function () {
            // initialize the configuration dialog tabs
            $('#controller-service-configuration-tabs').tabbs({
                tabStyle: 'tab',
                selectedTabStyle: 'selected-tab',
                scrollableTabContentStyle: 'scrollable',
                tabs: [{
                    name: 'Settings',
                    tabContentId: 'controller-service-standard-settings-tab-content'
                }, {
                    name: 'Properties',
                    tabContentId: 'controller-service-properties-tab-content'
                }, {
                    name: 'Comments',
                    tabContentId: 'controller-service-comments-tab-content'
                }],
                select: function () {
                    // remove all property detail dialogs
                    nf.UniversalCapture.removeAllPropertyDetailDialogs();

                    // update the property table size in case this is the first time its rendered
                    if ($(this).text() === 'Properties') {
                        $('#controller-service-properties').propertytable('resetTableSize');
                    }

                    // close all fields currently being edited
                    $('#controller-service-properties').propertytable('saveRow');

                    // show the border around the processor relationships if necessary
                    var referenceContainer = $('#controller-service-referencing-components');
                    updateReferencingComponentsBorder(referenceContainer);
                }
            });

            // initialize the conroller service configuration dialog
            $('#controller-service-configuration').modal({
                headerText: 'Configure Controller Service',
                scrollableContentStyle: 'scrollable',
                handler: {
                    close: function () {
                        // empty the referencing components list
                        var referencingComponents = $('#controller-service-referencing-components');
                        nf.Common.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        nf.Common.cleanUpTooltips(referencingComponents, 'div.referencing-component-bulletins');
                        referencingComponents.css('border-width', '0').empty();

                        // cancel any active edits
                        $('#controller-service-properties').propertytable('cancelEdit');

                        // clear the tables
                        $('#controller-service-properties').propertytable('clear');

                        // clear the comments
                        nf.Common.clearField('read-only-controller-service-comments');

                        // removed the cached controller service details
                        $('#controller-service-configuration').removeData('controllerServiceDetails');
                    },
                    open: function () {
                        nf.Common.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
                    }
                }
            });

            // initialize the disable service dialog
            $('#disable-controller-service-dialog').modal({
                headerText: 'Disable Controller Service',
                scrollableContentStyle: 'scrollable',
                handler: {
                    close: function () {
                        var disableDialog = $(this);

                        // reset visibility
                        $('#disable-controller-service-scope-container').show();
                        $('#disable-controller-service-progress-container').hide();

                        // clear the dialog
                        $('#disable-controller-service-id').text('');
                        $('#disable-controller-service-name').text('');

                        // bulletins
                        $('#disable-controller-service-bulletins').removeClass('has-bulletins').removeData('bulletins').hide();
                        nf.Common.cleanUpTooltips($('#disable-controller-service-service-container'), '#disable-controller-service-bulletins');

                        // reset progress
                        $('div.disable-referencing-components').removeClass('ajax-loading ajax-complete ajax-error');

                        // referencing components
                        var referencingComponents = $('#disable-controller-service-referencing-components');
                        nf.Common.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        nf.Common.cleanUpTooltips(referencingComponents, 'div.referencing-component-bulletins');
                        referencingComponents.css('border-width', '0').empty();
                    }
                }
            });

            // initialize the enable scope combo
            $('#enable-controller-service-scope').combo({
                options: [{
                    text: 'Service only',
                    value: config.serviceOnly,
                    description: 'Enable only this controller service'
                }, {
                    text: 'Service and referencing components',
                    value: config.serviceAndReferencingComponents,
                    description: 'Enable this controller service and enable/start all referencing components'
                }]
            });

            // initialize the enable service dialog
            $('#enable-controller-service-dialog').modal({
                headerText: 'Enable Controller Service',
                scrollableContentStyle: 'scrollable',
                handler: {
                    close: function () {
                        var enableDialog = $(this);

                        // reset visibility
                        $('#enable-controller-service-scope-container').show();
                        $('#enable-controller-service-progress-container').hide();
                        $('#enable-controller-service-progress li.referencing-component').show();

                        // clear the dialog
                        $('#enable-controller-service-id').text('');
                        $('#enable-controller-service-name').text('');

                        // bulletins
                        $('#enable-controller-service-bulletins').removeClass('has-bulletins').removeData('bulletins').hide();
                        nf.Common.cleanUpTooltips($('#enable-controller-service-service-container'), '#enable-controller-service-bulletins');

                        // reset progress
                        $('div.enable-referencing-components').removeClass('ajax-loading ajax-complete ajax-error');

                        // referencing components
                        var referencingComponents = $('#enable-controller-service-referencing-components');
                        nf.Common.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        nf.Common.cleanUpTooltips(referencingComponents, 'div.referencing-component-bulletins');
                        referencingComponents.css('border-width', '0').empty();
                    }
                }
            });
        },

        /**
         * Shows the configuration dialog for the specified controller service.
         *
         * @argument {jQuery} serviceTable                 The controller service table
         * @argument {object} controllerServiceEntity      The controller service
         */
        showConfiguration: function (serviceTable, controllerServiceEntity) {
            var controllerServiceDialog = $('#controller-service-configuration');
            if (controllerServiceDialog.data('mode') !== config.edit) {
                // update the visibility
                $('#controller-service-configuration .controller-service-read-only').hide();
                $('#controller-service-configuration .controller-service-editable').show();

                // initialize the property table
                $('#controller-service-properties').propertytable('destroy').propertytable({
                    readOnly: false,
                    dialogContainer: '#new-controller-service-property-container',
                    descriptorDeferred: getControllerServicePropertyDescriptor,
                    goToServiceDeferred: function () {
                        return goToServiceFromProperty(serviceTable);
                    }
                });

                // update the mode
                controllerServiceDialog.data('mode', config.edit);
            }

            // reload the service in case the property descriptors have changed
            var reloadService = $.ajax({
                type: 'GET',
                url: controllerServiceEntity.uri,
                dataType: 'json'
            });

            // get the controller service history
            var loadHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/history/components/' + encodeURIComponent(controllerServiceEntity.id),
                dataType: 'json'
            });

            // once everything is loaded, show the dialog
            $.when(reloadService, loadHistory).done(function (serviceResponse, historyResponse) {
                // get the updated controller service
                controllerServiceEntity = serviceResponse[0];
                var controllerService = controllerServiceEntity.component;

                // get the controller service history
                var controllerServiceHistory = historyResponse[0].componentHistory;

                // record the controller service details
                controllerServiceDialog.data('controllerServiceDetails', controllerServiceEntity);

                // populate the controller service settings
                nf.Common.populateField('controller-service-id', controllerService['id']);
                nf.Common.populateField('controller-service-type', nf.Common.substringAfterLast(controllerService['type'], '.'));
                $('#controller-service-name').val(controllerService['name']);
                $('#controller-service-comments').val(controllerService['comments']);

                // get the reference container
                var referenceContainer = $('#controller-service-referencing-components');

                // load the controller referencing components list
                createReferencingComponents(serviceTable, referenceContainer, controllerService.referencingComponents);

                var buttons = [{
                    buttonText: 'Apply',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            // close all fields currently being edited
                            $('#controller-service-properties').propertytable('saveRow');

                                // save the controller service
                                saveControllerService(serviceTable, controllerServiceEntity).done(function (response) {
                                    reloadControllerServiceReferences(serviceTable, response.component);
                                    
                                    // close the details panel
                                    controllerServiceDialog.modal('hide');
                                });
                            }
                        }
                    }, {
                        buttonText: 'Cancel',
                        color: {
                            base: '#E3E8EB',
                            hover: '#C7D2D7',
                            text: '#004849'
                        },
                        handler: {
                            click: function () {
                                controllerServiceDialog.modal('hide');
                            }
                        }
                    }];

                // determine if we should show the advanced button
                if (nf.Common.isDefinedAndNotNull(controllerService.customUiUrl) && controllerService.customUiUrl !== '') {
                    buttons.push({
                        buttonText: 'Advanced',
                        clazz: 'fa fa-cog button-icon',
                        color: {
                            base: '#E3E8EB',
                            hover: '#C7D2D7',
                            text: '#004849'
                        },
                        handler: {
                            click: function () {
                                var openCustomUi = function () {
                                    // reset state and close the dialog manually to avoid hiding the faded background
                                    controllerServiceDialog.modal('hide');

                                    // close the settings dialog since the custom ui is also opened in the shell
                                    $('#shell-close-button').click();

                                    // show the custom ui
                                    nf.CustomUi.showCustomUi(controllerServiceEntity, controllerService.customUiUrl, true).done(function () {
                                        // once the custom ui is closed, reload the controller service
                                        reloadControllerServiceAndReferencingComponents(serviceTable, controllerService);
                                        
                                        // show the settings
                                        nf.Settings.showSettings();
                                    });
                                };

                                // close all fields currently being edited
                                $('#controller-service-properties').propertytable('saveRow');

                                // determine if changes have been made
                                if (isSaveRequired()) {
                                    // see if those changes should be saved
                                    nf.Dialog.showYesNoDialog({
                                        headerText: 'Save',
                                        dialogContent: 'Save changes before opening the advanced configuration?',
                                        noHandler: openCustomUi,
                                        yesHandler: function () {
                                            saveControllerService(serviceTable, controllerServiceEntity).done(function () {
                                                // open the custom ui
                                                openCustomUi();
                                            });
                                        }
                                    });
                                } else {
                                    // if there were no changes, simply open the custom ui
                                    openCustomUi();
                                }
                            }
                        }
                    });
                }

                // set the button model
                controllerServiceDialog.modal('setButtonModel', buttons);

                // load the property table
                $('#controller-service-properties')
                    .propertytable('setGroupId', controllerService.parentGroupId)
                    .propertytable('loadProperties', controllerService.properties, controllerService.descriptors, controllerServiceHistory.propertyHistory);

                // show the details
                controllerServiceDialog.modal('show');

                // show the border if necessary
                updateReferencingComponentsBorder(referenceContainer);

                $('#controller-service-properties').propertytable('resetTableSize');
            }).fail(nf.Common.handleAjaxError);
        },

        /**
         * Shows the controller service details in a read only dialog.
         *
         * @param {jQuery} serviceTable
         * @param {object} controllerServiceEntity
         */
        showDetails: function(serviceTable, controllerServiceEntity) {
            var controllerServiceDialog = $('#controller-service-configuration');
            if (controllerServiceDialog.data('mode') !== config.readOnly) {
                // update the visibility
                $('#controller-service-configuration .controller-service-read-only').show();
                $('#controller-service-configuration .controller-service-editable').hide();

                // initialize the property table
                $('#controller-service-properties').propertytable('destroy').propertytable({
                    readOnly: true
                });

                // update the mode
                controllerServiceDialog.data('mode', config.readOnly);
            }

            // reload the service in case the property descriptors have changed
            var reloadService = $.ajax({
                type: 'GET',
                url: controllerServiceEntity.uri,
                dataType: 'json'
            });

            // get the controller service history
            var loadHistory = $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/history/components/' + encodeURIComponent(controllerServiceEntity.id),
                dataType: 'json'
            });

            // once everything is loaded, show the dialog
            $.when(reloadService, loadHistory).done(function (serviceResponse, historyResponse) {
                // get the updated controller service
                controllerServiceEntity = serviceResponse[0];
                var controllerService = controllerServiceEntity.component;

                // get the controller service history
                var controllerServiceHistory = historyResponse[0].componentHistory;

                // record the controller service details
                controllerServiceDialog.data('controllerServiceDetails', controllerServiceEntity);

                // populate the controller service settings
                nf.Common.populateField('controller-service-id', controllerService['id']);
                nf.Common.populateField('controller-service-type', nf.Common.substringAfterLast(controllerService['type'], '.'));
                nf.Common.populateField('read-only-controller-service-name', controllerService['name']);
                nf.Common.populateField('read-only-controller-service-comments', controllerService['comments']);

                // get the reference container
                var referenceContainer = $('#controller-service-referencing-components');

                // load the controller referencing components list
                createReferencingComponents(serviceTable, referenceContainer, controllerService.referencingComponents);
                
                var buttons = [{
                    buttonText: 'Ok',
                    color: {
                        base: '#728E9B',
                        hover: '#004849',
                        text: '#ffffff'
                    },
                    handler: {
                        click: function () {
                            // hide the dialog
                            controllerServiceDialog.modal('hide');
                        }
                    }
                }];

                // determine if we should show the advanced button
                if (nf.Common.isDefinedAndNotNull(nf.CustomUi) && nf.Common.isDefinedAndNotNull(controllerService.customUiUrl) && controllerService.customUiUrl !== '') {
                    buttons.push({
                        buttonText: 'Advanced',
                        clazz: 'fa fa-cog button-icon',
                        color: {
                            base: '#E3E8EB',
                            hover: '#C7D2D7',
                            text: '#004849'
                        },
                        handler: {
                            click: function () {
                                // reset state and close the dialog manually to avoid hiding the faded background
                                controllerServiceDialog.modal('hide');

                                // close the settings dialog since the custom ui is also opened in the shell
                                $('#shell-close-button').click();

                                // show the custom ui
                                nf.CustomUi.showCustomUi(controllerServiceEntity, controllerService.customUiUrl, false).done(function () {
                                    nf.Settings.showSettings();
                                });
                            }
                        }
                    });
                }

                // show the dialog
                controllerServiceDialog.modal('setButtonModel', buttons);

                // load the property table
                $('#controller-service-properties').propertytable('loadProperties', controllerService.properties, controllerService.descriptors, controllerServiceHistory.propertyHistory);

                // show the details
                controllerServiceDialog.modal('show');

                // show the border if necessary
                updateReferencingComponentsBorder(referenceContainer);

                $('#controller-service-properties').propertytable('resetTableSize');
            });
        },

        /**
         * Enables the specified controller service.
         *
         * @param {jQuery} serviceTable
         * @param {object} controllerServiceEntity
         */
        enable: function(serviceTable, controllerServiceEntity) {
            showEnableControllerServiceDialog(serviceTable, controllerServiceEntity.component);
        },

        /**
         * Disables the specified controller service.
         *
         * @param {jQuery} serviceTable
         * @param {object} controllerServiceEntity
         */
        disable: function(serviceTable, controllerServiceEntity) {
            showDisableControllerServiceDialog(serviceTable, controllerServiceEntity.component);
        },

        /**
         * Reloads the services that the specified comonent references. This is
         * necessary because the specified component state is reflected in the
         * referenced service referencing components.
         *
         * @param {jQuery} serviceTable
         * @param {object} component
         */
        reloadReferencedServices: function(serviceTable, component) {
            $.each(getReferencedServices(component), function (_, referencedServiceId) {
                reloadControllerService(serviceTable, referencedServiceId);
            });
        },

        /**
         * Deletes the specified controller service.
         *
         * @param {jQuery} serviceTable
         * @param {object} controllerServiceEntity
         */
        remove: function(serviceTable, controllerServiceEntity) {
            // prompt for removal?

            var revision = nf.Client.getRevision(controllerServiceEntity);
            $.ajax({
                type: 'DELETE',
                url: controllerServiceEntity.uri + '?' + $.param({
                    version: revision.version,
                    clientId: revision.clientId
                }),
                dataType: 'json'
            }).done(function (response) {
                // remove the service
                var controllerServiceGrid = serviceTable.data('gridInstance');
                var controllerServiceData = controllerServiceGrid.getData();
                controllerServiceData.deleteItem(controllerServiceEntity.id);

                // reload the as necessary
                reloadControllerServiceReferences(serviceTable, controllerServiceEntity.component);
            }).fail(nf.Common.handleAjaxError);
        }
    };
}());
