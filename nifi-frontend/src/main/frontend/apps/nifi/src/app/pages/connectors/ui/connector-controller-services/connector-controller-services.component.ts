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

import { Component, OnDestroy, computed, inject, signal } from '@angular/core';
import { Store } from '@ngrx/store';
import { combineLatest, distinctUntilChanged, filter } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ComponentType, isDefinedAndNotNull, TextTip } from '@nifi/shared';
import { BreadcrumbEntity, ControllerServiceEntity } from '../../../../state/shared';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { ErrorContextKey } from '../../../../state/error';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { DocumentationRequest } from '../../../../state/documentation';
import { BreadcrumbRouteGenerator } from '../../../../ui/common/breadcrumbs/breadcrumbs.component';
import {
    loadConnectorControllerServices,
    openViewControllerServiceDialog,
    resetConnectorControllerServicesState,
    selectConnectorControllerService
} from '../../state/connector-controller-services/connector-controller-services.actions';
import {
    selectConnectorControllerServicesState,
    selectConnectorIdFromRoute,
    selectControllerServiceIdFromRoute,
    selectProcessGroupIdFromRoute
} from '../../state/connector-controller-services/connector-controller-services.selectors';

/**
 * A read-only view of controller services scoped to a connector's process group.
 * Users can view configuration, documentation, and component state but cannot
 * edit, enable, disable, delete, or otherwise mutate any controller service.
 *
 * Route: /connectors/:id/canvas/:processGroupId/controller-services
 */
@Component({
    selector: 'connector-controller-services',
    templateUrl: './connector-controller-services.component.html',
    styleUrls: ['./connector-controller-services.component.scss'],
    standalone: false
})
export class ConnectorControllerServicesComponent implements OnDestroy {
    private store = inject(Store);

    serviceState = this.store.selectSignal(selectConnectorControllerServicesState);
    selectedServiceId$ = this.store.select(selectControllerServiceIdFromRoute);
    currentUser$ = this.store.select(selectCurrentUser);
    flowConfiguration$ = this.store.select(selectFlowConfiguration).pipe(isDefinedAndNotNull());

    protected readonly TextTip = TextTip;
    protected readonly ErrorContextKey = ErrorContextKey;

    private currentConnectorId = '';
    private currentProcessGroupId = '';

    showCurrentScopeOnly = signal(false);

    isInitialLoading = computed(() => {
        const state = this.serviceState();
        return state.status === 'loading' && !state.hasAttemptedLoad;
    });

    filteredControllerServices = computed(() => {
        const state = this.serviceState();
        if (!state) {
            return [];
        }

        const services = state.controllerServices;
        if (!this.showCurrentScopeOnly()) {
            return services;
        }

        const filterFn = this.definedByCurrentGroup(state.breadcrumb);
        return services.filter(filterFn);
    });

    shouldShowFilter = computed(() => {
        const state = this.serviceState();
        if (!state || !state.breadcrumb) {
            return false;
        }
        return !!state.breadcrumb.parentBreadcrumb;
    });

    constructor() {
        combineLatest([this.store.select(selectConnectorIdFromRoute), this.store.select(selectProcessGroupIdFromRoute)])
            .pipe(
                filter(([connectorId, processGroupId]) => connectorId != null && processGroupId != null),
                distinctUntilChanged(
                    ([prevConnectorId, prevProcessGroupId], [currConnectorId, currProcessGroupId]) =>
                        prevConnectorId === currConnectorId && prevProcessGroupId === currProcessGroupId
                ),
                takeUntilDestroyed()
            )
            .subscribe(([connectorId, processGroupId]) => {
                this.currentConnectorId = connectorId!;
                this.currentProcessGroupId = processGroupId!;
                this.store.dispatch(
                    loadConnectorControllerServices({
                        request: {
                            connectorId: connectorId!,
                            processGroupId: processGroupId!
                        }
                    })
                );
            });
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetConnectorControllerServicesState());
    }

    refreshControllerServiceListing(): void {
        this.store.dispatch(
            loadConnectorControllerServices({
                request: {
                    connectorId: this.currentConnectorId,
                    processGroupId: this.currentProcessGroupId
                }
            })
        );
    }

    formatScope(breadcrumb: BreadcrumbEntity | null): (entity: ControllerServiceEntity) => string {
        if (!breadcrumb) {
            return () => '';
        }

        const breadcrumbs: BreadcrumbEntity[] = [];
        let currentBreadcrumb: BreadcrumbEntity | undefined = breadcrumb;
        while (currentBreadcrumb != null) {
            breadcrumbs.push(currentBreadcrumb);
            currentBreadcrumb = currentBreadcrumb.parentBreadcrumb;
        }

        return (entity: ControllerServiceEntity): string => {
            const entityBreadcrumb = breadcrumbs.find((bc) => bc.id === entity.parentGroupId);
            if (entityBreadcrumb) {
                if (entityBreadcrumb.permissions.canRead) {
                    return entityBreadcrumb.breadcrumb.name;
                }
                return entityBreadcrumb.id;
            }
            return '';
        };
    }

    definedByCurrentGroup(breadcrumb: BreadcrumbEntity | null): (entity: ControllerServiceEntity) => boolean {
        if (!breadcrumb) {
            return () => true;
        }
        return (entity: ControllerServiceEntity): boolean => breadcrumb.id === entity.parentGroupId;
    }

    /**
     * Connector controller services are strictly read-only -- the table should
     * never offer mutating actions regardless of the underlying parent group's
     * permissions.
     */
    canModifyParent(): (entity: ControllerServiceEntity) => boolean {
        return () => false;
    }

    selectControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            selectConnectorControllerService({
                request: {
                    connectorId: this.currentConnectorId,
                    processGroupId: this.currentProcessGroupId,
                    serviceId: entity.id
                }
            })
        );
    }

    viewControllerServiceDocumentation(entity: ControllerServiceEntity): void {
        const request: DocumentationRequest = {
            parameters: {
                componentType: ComponentType.ControllerService,
                type: entity.component.type,
                group: entity.component.bundle.group,
                artifact: entity.component.bundle.artifact,
                version: entity.component.bundle.version
            },
            backNavigation: {
                route: [
                    '/connectors',
                    this.currentConnectorId,
                    'canvas',
                    this.currentProcessGroupId,
                    'controller-services',
                    entity.id
                ],
                routeBoundary: ['/documentation'],
                context: 'Controller Service'
            }
        };

        this.store.dispatch(navigateToComponentDocumentation({ request }));
    }

    configureControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(openViewControllerServiceDialog({ controllerService: entity }));
    }

    /**
     * View component state for a stateful controller service. The connector
     * canvas dispatches with connectorId so the component-state effect targets
     * the connector-scoped state endpoint instead of the framework endpoint.
     */
    viewStateControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            getComponentStateAndOpenDialog({
                request: {
                    componentName: entity.component.name,
                    componentId: entity.id,
                    componentType: ComponentType.ControllerService,
                    canClear: entity.component.state === 'DISABLED',
                    connectorId: this.currentConnectorId
                }
            })
        );
    }

    /**
     * Generate a route generator for breadcrumb navigation back into the
     * connector canvas.
     */
    getBreadcrumbRouteGenerator(): BreadcrumbRouteGenerator {
        const connectorId = this.currentConnectorId;
        return (processGroupId: string) => ['/connectors', connectorId, 'canvas', processGroupId];
    }

    toggleFilter(): void {
        this.showCurrentScopeOnly.set(!this.showCurrentScopeOnly());
    }

    getFilterTooltip(breadcrumb: BreadcrumbEntity | null): string {
        const processGroupName =
            breadcrumb?.permissions.canRead && breadcrumb?.breadcrumb.name ? breadcrumb.breadcrumb.name : 'this group';
        return `Only show the Controller Services defined in the current Process Group: '${processGroupName}'`;
    }
}
