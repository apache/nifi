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

/* global define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'd3',
                'nf.ErrorHandler',
                'nf.Common',
                'nf.Dialog',
                'nf.Storage',
                'nf.Client',
                'nf.Settings',
                'nf.UniversalCapture',
                'nf.CustomUi',
                'nf.CanvasUtils',
                'nf.Processor'],
            function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfSettings, nfUniversalCapture, nfCustomUi, nfCanvasUtils, nfProcessor) {
                return (nf.ControllerService = factory($, d3, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfSettings, nfUniversalCapture, nfCustomUi, nfCanvasUtils, nfProcessor));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.ControllerService =
            factory(require('jquery'),
                require('d3'),
                require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Storage'),
                require('nf.Client'),
                require('nf.Settings'),
                require('nf.UniversalCapture'),
                require('nf.CustomUi'),
                require('nf.CanvasUtils'),
                require('nf.Processor')));
    } else {
        nf.ControllerService = factory(root.$,
            root.d3,
            root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.Storage,
            root.nf.Client,
            root.nf.Settings,
            root.nf.UniversalCapture,
            root.nf.CustomUi,
            root.nf.CanvasUtils,
            root.nf.Processor);
    }
}(this, function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfSettings, nfUniversalCapture, nfCustomUi, nfCanvasUtils, nfProcessor) {
    'use strict';

    var nfControllerServices, nfReportingTask;

    var config = {
        edit: 'edit',
        readOnly: 'read-only',
        serviceOnly: 'SERVICE_ONLY',
        serviceAndReferencingComponents: 'SERVICE_AND_REFERENCING_COMPONENTS',
        urls: {
            api: '../nifi-api'
        }
    };

    /**
     * Determines whether the user has made any changes to the controller service configuration
     * that needs to be saved.
     */
    var isSaveRequired = function () {
        var entity = $('#controller-service-configuration').data('controllerServiceDetails');

        // determine if any controller service settings have changed

        if ($('#controller-service-name').val() !== entity.component['name']) {
            return true;
        }
        if ($('#controller-service-comments').val() !== entity.component['comments']) {
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
        controllerServiceEntity['disconnectedNodeAcknowledged'] = nfStorage.isDisconnectionAcknowledged();
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
     * Reloads the specified controller service if we have read permissions. It's referencing and referenced
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
        // will attempt to reload B which is no longer a known service. also ensure
        // we have permissions to reload the service
        if (nfCommon.isUndefined(controllerServiceEntity) || controllerServiceEntity.permissions.canRead === false) {
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
        }).fail(nfErrorHandler.handleAjaxError);
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
            type: 'ControllerService',
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
        reloadControllerService(serviceTable, controllerService.id).done(function (response) {
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
                if (nfCanvasUtils.getGroupId() === reference.groupId) {
                    nfProcessor.reload(reference.id);
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
                nfReportingTask.reload(reference.id);

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
        nfControllerService.reloadReferencedServices(serviceTable, controllerService);
    };

    /**
     * Adds a border to the controller service referencing components if necessary.
     *
     * @argument {jQuery} referenceContainer
     */
    var updateReferencingComponentsBorder = function (referenceContainer) {
        // determine if it is too big
        var tooBig = referenceContainer.get(0).scrollHeight > Math.round(referenceContainer.innerHeight()) ||
            referenceContainer.get(0).scrollWidth > Math.round(referenceContainer.innerWidth());

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
        if (nfCommon.doBulletinsDiffer(currentBulletins, bulletins)) {
            bulletinIcon.data('bulletins', bulletins);

            // format the new bulletins
            var formattedBulletins = nfCommon.getFormattedBulletins(bulletins);

            // if there are bulletins update them
            if (bulletins.length > 0) {
                var list = nfCommon.formatUnorderedList(formattedBulletins);

                // update existing tooltip or initialize a new one if appropriate
                if (bulletinIcon.data('qtip')) {
                    bulletinIcon.qtip('option', 'content.text', list);
                } else {
                    bulletinIcon.addClass('has-bulletins').show().qtip($.extend({},
                        nfCanvasUtils.config.systemTooltipConfig,
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
            if (state === 'stopped' && !nfCommon.isEmpty(referencingComponent.validationErrors)) {
                state = 'invalid';

                // add tooltip for the warnings
                var list = nfCommon.formatUnorderedList(referencingComponent.validationErrors);
                if (icon.data('qtip')) {
                    icon.qtip('option', 'content.text', list);
                } else {
                    icon.qtip($.extend({},
                        nfCanvasUtils.config.systemTooltipConfig,
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
            if (state === 'disabled' && !nfCommon.isEmpty(referencingService.validationErrors)) {
                state = 'invalid';

                // add tooltip for the warnings
                var list = nfCommon.formatUnorderedList(referencingService.validationErrors);
                if (icon.data('qtip')) {
                    icon.qtip('option', 'content.text', list);
                } else {
                    icon.qtip($.extend({},
                        nfCanvasUtils.config.systemTooltipConfig,
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

        bulletinsBySource.each(function (sourceBulletins, sourceId) {
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
     * @param {object} controllerServiceEntity
     */
    var createReferencingComponents = function (serviceTable, referenceContainer, controllerServiceEntity) {

        if (controllerServiceEntity.permissions.canRead === false) {
            referenceContainer.append('<div class="unset">Unauthorized</div>');
            return;
        }

        var referencingComponents = controllerServiceEntity.component.referencingComponents;
        if (nfCommon.isEmpty(referencingComponents)) {
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
            if (referencingComponentEntity.permissions.canRead === false) {
                var unauthorizedReferencingComponent = $('<div class="unset"></div>').text(referencingComponentEntity.id);
                unauthorized.append(unauthorizedReferencingComponent);
            } else {
                var referencingComponent = referencingComponentEntity.component;
                referencingComponentIds.push(referencingComponent.id);

                if (referencingComponent.referenceType === 'Processor') {
                    var processorLink = $('<span class="referencing-component-name link"></span>').text(referencingComponent.name).on('click', function () {
                        // show the component
                        nfCanvasUtils.showComponent(referencingComponent.groupId, referencingComponent.id);

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
                    var processorType = $('<span class="referencing-component-type"></span>').text(referencingComponent.type);

                    // active thread count
                    var processorActiveThreadCount = $('<span class="referencing-component-active-thread-count"></span>').addClass(referencingComponent.id + '-active-threads');
                    if (nfCommon.isDefinedAndNotNull(referencingComponent.activeThreadCount) && referencingComponent.activeThreadCount > 0) {
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
                            // create the markup for the references
                            if (referencingComponent.referenceCycle === true) {
                                referencingServiceReferencesContainer.append('<div class="unset">Reference cycle detected.</div>');
                            } else {
                                var controllerServiceGrid = serviceTable.data('gridInstance');
                                var controllerServiceData = controllerServiceGrid.getData();

                                // get the controller service and expand its referencing services
                                getControllerService(referencingComponent.id, controllerServiceData).done(function (controllerServiceEntity) {
                                    createReferencingComponents(serviceTable, referencingServiceReferencesContainer, controllerServiceEntity);
                                });
                            }
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
                    var serviceType = $('<span class="referencing-component-type"></span>').text(referencingComponent.type);

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
                    var reportingTaskType = $('<span class="referencing-component-type"></span>').text(nfCommon.substringAfterLast(referencingComponent.type, '.'));

                    // active thread count
                    var reportingTaskActiveThreadCount = $('<span class="referencing-component-active-thread-count"></span>').addClass(referencingComponent.id + '-active-threads');
                    if (nfCommon.isDefinedAndNotNull(referencingComponent.activeThreadCount) && referencingComponent.activeThreadCount > 0) {
                        reportingTaskActiveThreadCount.text('(' + referencingComponent.activeThreadCount + ')');
                    }

                    // reporting task
                    var reportingTaskItem = $('<li></li>').append(reportingTaskState).append(reportingTaskBulletins).append(reportingTaskLink).append(reportingTaskType).append(reportingTaskActiveThreadCount);
                    tasks.append(reportingTaskItem);
                }
            }
        });

        // query for the bulletins
        nfCanvasUtils.queryBulletins(referencingComponentIds).done(function (response) {
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
            'revision': nfClient.getRevision(controllerServiceEntity),
            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
            'state': enabled ? 'ENABLED' : 'DISABLED'
        };

        var updated = $.ajax({
            type: 'PUT',
            url: controllerServiceEntity.uri + '/run-status',
            data: JSON.stringify(updateControllerServiceEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).done(function (response) {
            renderControllerService(serviceTable, response);
        }).fail(nfErrorHandler.handleAjaxError);

        // wait until the polling of each service finished
        return $.Deferred(function (deferred) {
            updated.done(function () {
                if (controllerServiceEntity.permissions.canRead === false) {
                    // Can not use GET request to wait for completion. Just resolve blindly. The PUT request has been finished anyway.
                    deferred.resolve();
                    return;
                }

                var serviceUpdated = pollService(controllerServiceEntity, function (latestControllerServiceEntity, bulletins) {
                    if ($.isArray(bulletins)) {
                        if (enabled) {
                            updateBulletins(bulletins, $('#enable-controller-service-bulletins'));
                        } else {
                            updateBulletins(bulletins, $('#disable-controller-service-bulletins'));
                        }
                    }

                    // the condition is met once the service is (ENABLING or ENABLED)/DISABLED
                    var runStatus = latestControllerServiceEntity.status.runStatus;
                    if (enabled) {
                        return runStatus === 'ENABLING' || runStatus === 'ENABLED';
                    } else {
                        return runStatus === 'DISABLED';
                    }
                }, function (service) {
                    return nfCanvasUtils.queryBulletins([controllerServiceEntity.id]);
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

        // check the referencing services
        checkReferencingServices(controllerService.referencingComponents);
        return ids;
    };

    /**
     * Gathers all referencing component revisions.
     *
     * @param referencingComponents
     * @param referencingComponentRevisions
     * @param serviceOnly - true includes only services, false includes only schedulable components
     */
    var getReferencingComponentRevisions = function (referencingComponents, referencingComponentRevisions, serviceOnly) {
        // include the revision of each referencing component
        $.each(referencingComponents, function (_, referencingComponentEntity) {
            var referencingComponent = referencingComponentEntity.component;

            if (serviceOnly) {
                if (referencingComponent.referenceType === 'ControllerService') {
                    referencingComponentRevisions[referencingComponentEntity.id] = nfClient.getRevision(referencingComponentEntity);
                }
            } else {
                if (referencingComponent.referenceType !== 'ControllerService') {
                    referencingComponentRevisions[referencingComponentEntity.id] = nfClient.getRevision(referencingComponentEntity);
                }
            }

            // recurse
            getReferencingComponentRevisions(referencingComponent.referencingComponents, referencingComponentRevisions, serviceOnly);
        });
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
        var referencingRevisions = {};
        getReferencingComponentRevisions(controllerServiceEntity.component.referencingComponents, referencingRevisions, false);

        var referenceEntity = {
            'id': controllerServiceEntity.id,
            'state': running ? 'RUNNING' : 'STOPPED',
            'referencingComponentRevisions': referencingRevisions,
            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
        };

        // issue the request to update the referencing components
        var updated = $.ajax({
            type: 'PUT',
            url: controllerServiceEntity.uri + '/references',
            data: JSON.stringify(referenceEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).fail(nfErrorHandler.handleAjaxError);

        // Note: updated revisions will be retrieved after updateReferencingSchedulableComponents is invoked

        // wait until the polling of each service finished
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
                    services.each(function (controllerServiceId) {
                        getControllerService(controllerServiceId, controllerServiceData).done(function(controllerServiceEntity) {
                            polling.push(stopReferencingSchedulableComponents(controllerServiceEntity, pollCondition));
                        });
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
     * Gets the controller service with the specified id. If the service is present in the specified
     * controllerServiceData it will be used. Otherwise it will query for it.
     *
     * @param controllerServiceId   service id
     * @param controllerServiceData service table data
     * @returns {deferred} the controller service entity
     */
    var getControllerService = function (controllerServiceId, controllerServiceData) {
        var controllerServiceEntity = controllerServiceData.getItemById(controllerServiceId);
        if (nfCommon.isDefinedAndNotNull(controllerServiceEntity)) {
            return $.Deferred(function (deferred) {
                deferred.resolve(controllerServiceEntity);
            });
        } else {
            return $.ajax({
                type: 'GET',
                url: '../nifi-api/controller-services/' + encodeURIComponent(controllerServiceId),
                dataType: 'json'
            }).fail(nfErrorHandler.handleAjaxError);
        }
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

                $.when(bulletins, service).done(function (bulletinResponse, serviceResult) {
                    var latestControllerServiceEntity = serviceResult[0];
                    conditionMet(latestControllerServiceEntity, bulletinResponse.bulletinBoard.bulletins);
                }).fail(function (xhr, status, error) {
                    deferred.reject();
                    nfErrorHandler.handleAjaxError(xhr, status, error);
                });
            };

            // tests to if the condition has been met
            var conditionMet = function (latestControllerServiceEntity, bulletins) {
                if (completeCondition(latestControllerServiceEntity, bulletins)) {
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
                conditionMet(controllerServiceEntity, response.bulletinBoard.bulletins);
            }).fail(function (xhr, status, error) {
                deferred.reject();
                nfErrorHandler.handleAjaxError(xhr, status, error);
            });
        }).promise();
    };

    /**
     * Continues to poll the specified controller service until all referencing schedulable
     * components are stopped (not scheduled and 0 active threads).
     *
     * @param {object} controllerServiceEntity
     * @param {function} pollCondition
     */
    var stopReferencingSchedulableComponents = function (controllerServiceEntity, pollCondition) {
        // continue to poll the service until all schedulable components have stopped
        return pollService(controllerServiceEntity, function (latestControllerServiceEntity, bulletins) {
            var referencingComponents = latestControllerServiceEntity.component.referencingComponents;

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

            return nfCanvasUtils.queryBulletins(referencingSchedulableComponents);
        }, pollCondition);
    };

    /**
     * Continues to poll until all referencing services are enabled.
     *
     * @param {object} controllerServiceEntity
     * @param {function} pollCondition
     */
    var enableReferencingServices = function (controllerServiceEntity, pollCondition) {
        // continue to poll the service until all referencing services are enabled
        return pollService(controllerServiceEntity, function (latestControllerServiceEntity, bulletins) {
            var referencingComponents = latestControllerServiceEntity.component.referencingComponents;

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

            return nfCanvasUtils.queryBulletins(referencingSchedulableComponents);
        }, pollCondition);
    };

    /**
     * Continues to poll until all referencing services are disabled.
     *
     * @param {object} controllerServiceEntity
     * @param {function} pollCondition
     */
    var disableReferencingServices = function (controllerServiceEntity, pollCondition) {
        // continue to poll the service until all referencing services are disabled
        return pollService(controllerServiceEntity, function (latestControllerServiceEntity, bulletins) {
            var referencingComponents = latestControllerServiceEntity.component.referencingComponents;

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

            return nfCanvasUtils.queryBulletins(referencingSchedulableComponents);
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
        var referencingRevisions = {};
        getReferencingComponentRevisions(controllerServiceEntity.component.referencingComponents, referencingRevisions, true);

        // build the reference entity
        var referenceEntity = {
            'id': controllerServiceEntity.id,
            'state': enabled ? 'ENABLED' : 'DISABLED',
            'referencingComponentRevisions': referencingRevisions,
            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
        };

        // issue the request to update the referencing components
        var updated = $.ajax({
            type: 'PUT',
            url: controllerServiceEntity.uri + '/references',
            data: JSON.stringify(referenceEntity),
            dataType: 'json',
            contentType: 'application/json'
        }).fail(nfErrorHandler.handleAjaxError);

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
                services.each(function (controllerServiceId) {
                    getControllerService(controllerServiceId, controllerServiceData).done(function(controllerServiceEntity) {
                        if (enabled) {
                            polling.push(enableReferencingServices(controllerServiceEntity, pollCondition));
                        } else {
                            polling.push(disableReferencingServices(controllerServiceEntity, pollCondition));
                        }
                    });
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
    var showDisableControllerServiceDialog = function (serviceTable, controllerServiceEntity) {
        // populate the disable controller service dialog
        $('#disable-controller-service-id').text(controllerServiceEntity.id);
        $('#disable-controller-service-name').text(nfCommon.getComponentName(controllerServiceEntity));

        // load the controller referencing components list
        var referencingComponentsContainer = $('#disable-controller-service-referencing-components');
        createReferencingComponents(serviceTable, referencingComponentsContainer, controllerServiceEntity);

        // build the button model
        var buttons = [{
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
        $('#disable-controller-service-dialog').modal('setButtonModel', buttons).modal('show');

        // load the bulletins
        nfCanvasUtils.queryBulletins([controllerServiceEntity.id]).done(function (response) {
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
    var showEnableControllerServiceDialog = function (serviceTable, controllerServiceEntity) {
        // populate the disable controller service dialog
        $('#enable-controller-service-id').text(controllerServiceEntity.id);
        $('#enable-controller-service-name').text(nfCommon.getComponentName(controllerServiceEntity));

        // load the controller referencing components list
        var referencingComponentsContainer = $('#enable-controller-service-referencing-components');
        createReferencingComponents(serviceTable, referencingComponentsContainer, controllerServiceEntity);

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
        nfCanvasUtils.queryBulletins([controllerServiceEntity.id]).done(function (response) {
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
    var disableHandler = function (serviceTable) {
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

        // ensure we have access to all referencing components before attempting the sequence
        if (hasUnauthorizedReferencingComponent(controllerServiceEntity)) {
            setCloseButton();

            nfDialog.showOkDialog({
                headerText: 'Controller Service',
                dialogContent: controllerServiceEntity.permissions.canRead === false
                                    // Unknown references.
                                    ? 'Unable to disable due to a lack of read permission to see referencing components.'
                                    // Unauthorized references.
                                    : 'Unable to disable due to unauthorized referencing components.'
            });
            return;
        }

        $('#disable-progress-label').text('Steps to disable ' + nfCommon.getComponentName(controllerServiceEntity));
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
            if (controllerServiceEntity.permissions.canRead === true) {
                reloadControllerServiceAndReferencingComponents(serviceTable, controllerService);
            }
            setCloseButton();

            // inform the user if the action was canceled
            if (canceled === true && $('#nf-ok-dialog').not(':visible')) {
                nfDialog.showOkDialog({
                    headerText: 'Controller Service',
                    dialogContent: 'The request to disable has been canceled. Parts of this request may have already completed. Please verify the state of this service and all referencing components.'
                });
            }
        });
    };

    /**
     * Determines if any of the specified referencing components are not authorized to enable/disable.
     *
     * @param {object} ControllerServiceEntity having referencingComponents referencing components
     * @returns {boolean}
     */
    var hasUnauthorizedReferencingComponent = function (controllerServiceEntity) {

        if (controllerServiceEntity.permissions.canRead === false
                || (controllerServiceEntity.permissions.canWrite === false && controllerServiceEntity.operatePermissions.canWrite === false)) {
            return true;
        }

        var hasUnauthorized = false;
        var referencingComponents = controllerServiceEntity.component.referencingComponents;
        $.each(referencingComponents, function (_, referencingComponentEntity) {
            if (hasUnauthorizedReferencingComponent(referencingComponentEntity)) {
                hasUnauthorized = true;
                return false;
            }
        });

        return hasUnauthorized;
    };

    /**
     * Handles the enable action of the enable controller service dialog.
     *
     * @param {jQuery} serviceTable
     */
    var enableHandler = function (serviceTable) {
        var enableDialog = $('#enable-controller-service-dialog');
        var canceled = false;

        // get the controller service
        var controllerServiceId = $('#enable-controller-service-id').text();
        var controllerServiceGrid = serviceTable.data('gridInstance');
        var controllerServiceData = controllerServiceGrid.getData();
        var controllerServiceEntity = controllerServiceData.getItemById(controllerServiceId);
        var controllerService = controllerServiceEntity.component;

        // determine if we want to also activate referencing components
        var scope = $('#enable-controller-service-scope').combo('getSelectedOption').value;

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

        // ensure appropriate access
        if (scope === config.serviceAndReferencingComponents && hasUnauthorizedReferencingComponent(controllerServiceEntity)) {
            setCloseButton();

            nfDialog.showOkDialog({
                headerText: 'Controller Service',
                dialogContent: controllerServiceEntity.permissions.canRead === false
                                    // Unknown references.
                                    ? 'Unable to enable due to a lack of read permission to see referencing components.'
                                    // Unauthorized references.
                                    : 'Unable to enable due to unauthorized referencing components.'
            });
            return;
        }

        $('#enable-progress-label').text('Steps to enable ' + nfCommon.getComponentName(controllerServiceEntity));
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
                        updateReferencingSchedulableComponents(serviceTable, controllerServiceEntity, true, continuePolling).done(function () {
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
            if (controllerServiceEntity.permissions.canRead === true) {
                reloadControllerServiceAndReferencingComponents(serviceTable, controllerService);
            }
            setCloseButton();

            // inform the user if the action was canceled
            if (canceled === true && $('#nf-ok-dialog').not(':visible')) {
                nfDialog.showOkDialog({
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
        }).fail(nfErrorHandler.handleAjaxError);
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
                nfDialog.showYesNoDialog({
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
            updatedControllerService['revision'] = nfClient.getRevision(controllerServiceEntity);

            var previouslyReferencedServiceIds = [];
            $.each(identifyReferencedServiceDescriptors(controllerServiceEntity.component), function (_, descriptor) {
                var modifyingService = !nfCommon.isUndefined(updatedControllerService.component.properties) && !nfCommon.isUndefined(updatedControllerService.component.properties[descriptor.name]);
                var isCurrentlyConfigured = nfCommon.isDefinedAndNotNull(controllerServiceEntity.component.properties[descriptor.name]);

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
                $.each(previouslyReferencedServiceIds, function (_, oldServiceReferenceId) {
                    reloadControllerService(serviceTable, oldServiceReferenceId);
                });
            }).fail(nfErrorHandler.handleConfigurationUpdateAjaxError);
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
            if (nfCommon.isDefinedAndNotNull(descriptor.identifiesControllerService)) {
                referencedServiceDescriptors.push(descriptor);
            }
        });

        return referencedServiceDescriptors;
    };

    /**
     * Identifies descriptors that reference controller services.
     *
     * @param {object} component
     */
    var getReferencedServices = function (component) {
        var referencedServices = [];

        $.each(identifyReferencedServiceDescriptors(component), function (_, descriptor) {
            var referencedServiceId = component.properties[descriptor.name];

            // ensure the property is configured
            if (nfCommon.isDefinedAndNotNull(referencedServiceId) && $.trim(referencedServiceId).length > 0) {
                referencedServices.push(referencedServiceId);
            }
        });

        return referencedServices;
    };

    /**
     * Track the current table
     */
    var currentTable;

    var nfControllerService = {
        /**
         * Initializes the controller service configuration dialog.
         */
        init: function (nfControllerServicesRef, nfReportingTaskRef) {
            nfControllerServices = nfControllerServicesRef;
            nfReportingTask = nfReportingTaskRef;

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
                    nfUniversalCapture.removeAllPropertyDetailDialogs();

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
                        nfCommon.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        nfCommon.cleanUpTooltips(referencingComponents, 'div.referencing-component-bulletins');
                        referencingComponents.css('border-width', '0').empty();

                        // cancel any active edits
                        $('#controller-service-properties').propertytable('cancelEdit');

                        // clear the tables
                        $('#controller-service-properties').propertytable('clear');

                        // clear the comments
                        nfCommon.clearField('read-only-controller-service-comments');

                        // clear the compatible apis
                        $('#controller-service-compatible-apis').empty();

                        // removed the cached controller service details
                        $('#controller-service-configuration').removeData('controllerServiceDetails');
                    },
                    open: function () {
                        nfCommon.toggleScrollable($('#' + this.find('.tab-container').attr('id') + '-content').get(0));
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
                        nfCommon.cleanUpTooltips($('#disable-controller-service-service-container'), '#disable-controller-service-bulletins');

                        // reset progress
                        $('div.disable-referencing-components').removeClass('ajax-loading ajax-complete ajax-error');

                        // referencing components
                        var referencingComponents = $('#disable-controller-service-referencing-components');
                        nfCommon.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        nfCommon.cleanUpTooltips(referencingComponents, 'div.referencing-component-bulletins');
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
                        nfCommon.cleanUpTooltips($('#enable-controller-service-service-container'), '#enable-controller-service-bulletins');

                        // reset progress
                        $('div.enable-referencing-components').removeClass('ajax-loading ajax-complete ajax-error');

                        // referencing components
                        var referencingComponents = $('#enable-controller-service-referencing-components');
                        nfCommon.cleanUpTooltips(referencingComponents, 'div.referencing-component-state');
                        nfCommon.cleanUpTooltips(referencingComponents, 'div.referencing-component-bulletins');
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
            if (nfCommon.isUndefined(currentTable)) {
                currentTable = serviceTable;
            }
            var controllerServiceDialog = $('#controller-service-configuration');

            controllerServiceDialog.find('.dialog-header .dialog-header-text').text('Configure Controller Service');
            if (controllerServiceDialog.data('mode') !== config.edit || currentTable !== serviceTable) {
                // update the visibility
                $('#controller-service-configuration .controller-service-read-only').hide();
                $('#controller-service-configuration .controller-service-editable').show();

                // initialize the property table
                $('#controller-service-properties').propertytable('destroy').propertytable({
                    readOnly: false,
                    supportsGoTo: true,
                    dialogContainer: '#new-controller-service-property-container',
                    descriptorDeferred: getControllerServicePropertyDescriptor,
                    controllerServiceCreatedDeferred: function (response) {
                        var controllerServicesUri;

                        // calculate the correct uri
                        var createdControllerService = response.component;
                        if (nfCommon.isDefinedAndNotNull(createdControllerService.parentGroupId)) {
                            controllerServicesUri = config.urls.api + '/flow/process-groups/' + encodeURIComponent(createdControllerService.parentGroupId) + '/controller-services';
                        } else {
                            controllerServicesUri = config.urls.api + '/flow/controller/controller-services';
                        }

                        // load the controller services accordingly
                        return nfControllerServices.loadControllerServices(controllerServicesUri, serviceTable);
                    },
                    goToServiceDeferred: function () {
                        return goToServiceFromProperty(serviceTable);
                    }
                });

                // update the mode
                controllerServiceDialog.data('mode', config.edit);
            }

            //track the current table
            currentTable = serviceTable;

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
                nfCommon.populateField('controller-service-id', controllerService['id']);
                nfCommon.populateField('controller-service-type', nfCommon.formatType(controllerService));
                nfCommon.populateField('controller-service-bundle', nfCommon.formatBundle(controllerService['bundle']));
                $('#controller-service-name').val(controllerService['name']);
                $('#controller-service-comments').val(controllerService['comments']);

                // set the implemented apis
                if (!nfCommon.isEmpty(controllerService['controllerServiceApis'])) {
                    var formattedControllerServiceApis = nfCommon.getFormattedServiceApis(controllerService['controllerServiceApis']);
                    var serviceTips = nfCommon.formatUnorderedList(formattedControllerServiceApis);
                    $('#controller-service-compatible-apis').append(serviceTips);
                } else {
                    $('#controller-service-compatible-apis').append('<span class="unset">None</span>');
                }

                // get the reference container
                var referenceContainer = $('#controller-service-referencing-components');

                // load the controller referencing components list
                createReferencingComponents(serviceTable, referenceContainer, controllerServiceEntity);

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
                if (nfCommon.isDefinedAndNotNull(controllerService.customUiUrl) && controllerService.customUiUrl !== '') {
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
                                    nfCustomUi.showCustomUi(controllerServiceEntity, controllerService.customUiUrl, true).done(function () {
                                        // once the custom ui is closed, reload the controller service
                                        reloadControllerServiceAndReferencingComponents(serviceTable, controllerService);

                                        // show the settings
                                        nfSettings.showSettings();
                                    });
                                };

                                // close all fields currently being edited
                                $('#controller-service-properties').propertytable('saveRow');

                                // determine if changes have been made
                                if (isSaveRequired()) {
                                    // see if those changes should be saved
                                    nfDialog.showYesNoDialog({
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
            }).fail(nfErrorHandler.handleAjaxError);
        },

        /**
         * Shows the controller service details in a read only dialog.
         *
         * @param {jQuery} serviceTable
         * @param {object} controllerServiceEntity
         */
        showDetails: function (serviceTable, controllerServiceEntity) {
            var controllerServiceDialog = $('#controller-service-configuration');

            controllerServiceDialog.find('.dialog-header .dialog-header-text').text('Controller Service Details');
            if (controllerServiceDialog.data('mode') !== config.readOnly) {
                // update the visibility
                $('#controller-service-configuration .controller-service-read-only').show();
                $('#controller-service-configuration .controller-service-editable').hide();

                // initialize the property table
                $('#controller-service-properties').propertytable('destroy').propertytable({
                    supportsGoTo: true,
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
                nfCommon.populateField('controller-service-id', controllerService['id']);
                nfCommon.populateField('controller-service-type', nfCommon.formatType(controllerService));
                nfCommon.populateField('controller-service-bundle', nfCommon.formatBundle(controllerService['bundle']));
                nfCommon.populateField('read-only-controller-service-name', controllerService['name']);
                nfCommon.populateField('read-only-controller-service-comments', controllerService['comments']);

                // set the implemented apis
                if (!nfCommon.isEmpty(controllerService['controllerServiceApis'])) {
                    var formattedControllerServiceApis = nfCommon.getFormattedServiceApis(controllerService['controllerServiceApis']);
                    var serviceTips = nfCommon.formatUnorderedList(formattedControllerServiceApis);
                    $('#controller-service-compatible-apis').append(serviceTips);
                } else {
                    $('#controller-service-compatible-apis').append('<span class="unset">None</span>');
                }

                // get the reference container
                var referenceContainer = $('#controller-service-referencing-components');

                // load the controller referencing components list
                createReferencingComponents(serviceTable, referenceContainer, controllerServiceEntity);

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
                if (nfCommon.isDefinedAndNotNull(nfCustomUi) && nfCommon.isDefinedAndNotNull(controllerService.customUiUrl) && controllerService.customUiUrl !== '') {
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
                                nfCustomUi.showCustomUi(controllerServiceEntity, controllerService.customUiUrl, false).done(function () {
                                    nfSettings.showSettings();
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
        enable: function (serviceTable, controllerServiceEntity) {
            showEnableControllerServiceDialog(serviceTable, controllerServiceEntity);
        },

        /**
         * Disables the specified controller service.
         *
         * @param {jQuery} serviceTable
         * @param {object} controllerServiceEntity
         */
        disable: function (serviceTable, controllerServiceEntity) {
            showDisableControllerServiceDialog(serviceTable, controllerServiceEntity);
        },

        /**
         * Reloads the services that the specified comonent references. This is
         * necessary because the specified component state is reflected in the
         * referenced service referencing components.
         *
         * @param {jQuery} serviceTable
         * @param {object} component
         */
        reloadReferencedServices: function (serviceTable, component) {
            $.each(getReferencedServices(component), function (_, referencedServiceId) {
                reloadControllerService(serviceTable, referencedServiceId);
            });
        },

        /**
         * Prompts the user before attempting to delete the specified controller service.
         *
         * @param {jQuery} serviceTable
         * @param {object} controllerServiceEntity
         */
        promptToDeleteController: function (serviceTable, controllerServiceEntity) {
            // prompt for deletion
            nfDialog.showYesNoDialog({
                headerText: 'Delete Controller Service',
                dialogContent: 'Delete controller service \'' + nfCommon.escapeHtml(controllerServiceEntity.component.name) + '\'?',
                yesHandler: function () {
                    nfControllerService.remove(serviceTable, controllerServiceEntity);
                }
            });
        },

        /**
         * Deletes the specified controller service.
         *
         * @param {jQuery} serviceTable
         * @param {object} controllerServiceEntity
         */
        remove: function (serviceTable, controllerServiceEntity) {
            // prompt for removal?

            var revision = nfClient.getRevision(controllerServiceEntity);
            $.ajax({
                type: 'DELETE',
                url: controllerServiceEntity.uri + '?' + $.param({
                    'version': revision.version,
                    'clientId': revision.clientId,
                    'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged()
                }),
                dataType: 'json'
            }).done(function (response) {
                // remove the service
                var controllerServiceGrid = serviceTable.data('gridInstance');
                var controllerServiceData = controllerServiceGrid.getData();
                controllerServiceData.deleteItem(controllerServiceEntity.id);

                // reload the as necessary
                if (controllerServiceEntity.permissions.canRead) {
                    reloadControllerServiceReferences(serviceTable, controllerServiceEntity.component);
                }
            }).fail(nfErrorHandler.handleAjaxError);
        }
    };

    return nfControllerService;
}));
