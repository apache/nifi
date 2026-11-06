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
import { ComponentType, ControllerServiceEntity, isDefinedAndNotNull, NiFiCommon, TextTip } from '@nifi/shared';
import {
    BreadcrumbEntity,
    ClearBulletinsRequest,
    DisableControllerServiceDialogRequest,
    EditControllerServiceDialogRequest,
    FetchComponentVersionsRequest,
    SetEnableControllerServiceDialogRequest
} from '../../../../state/shared';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { ErrorContextKey } from '../../../../state/error';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { DocumentationRequest } from '../../../../state/documentation';
import { BreadcrumbRouteGenerator } from '../../../../ui/common/breadcrumbs/breadcrumbs.component';
import {
    clearConnectorControllerServiceBulletins,
    loadConnectorControllerServices,
    navigateToAdvancedConnectorControllerServiceUi,
    navigateToConnectorControllerService,
    openChangeConnectorControllerServiceVersionDialog,
    openConfigureConnectorControllerServiceDialog,
    openDisableConnectorControllerServiceDialog,
    openEnableConnectorControllerServiceDialog,
    openNewConnectorControllerServiceDialog,
    openViewControllerServiceDialog,
    promptConnectorControllerServiceDeletion,
    resetConnectorControllerServicesState,
    selectConnectorControllerService,
    startConnectorControllerServicesEntityPolling,
    stopConnectorControllerServicesEntityPolling
} from '../../state/connector-controller-services/connector-controller-services.actions';
import {
    selectConnectorControllerServicesState,
    selectConnectorIdFromRoute,
    selectControllerServiceIdFromRoute,
    selectProcessGroupIdFromRoute
} from '../../state/connector-controller-services/connector-controller-services.selectors';
import { selectConnectorCanvasEntity } from '../../state/connector-canvas-entity/connector-canvas-entity.selectors';
import { loadConnectorEntity } from '../../state/connector-canvas-entity/connector-canvas-entity.actions';
import { DeleteControllerServiceRequest } from '../../../../state/flow-shared';

/**
 * Controller services scoped to a connector's process group. Mutations are
 * available only while the connector is in troubleshooting.
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
    private nifiCommon = inject(NiFiCommon);

    serviceState = this.store.selectSignal(selectConnectorControllerServicesState);
    selectedServiceId$ = this.store.select(selectControllerServiceIdFromRoute);
    currentUser$ = this.store.select(selectCurrentUser);
    flowConfiguration$ = this.store.select(selectFlowConfiguration).pipe(isDefinedAndNotNull());
    private connectorEntity = this.store.selectSignal(selectConnectorCanvasEntity);
    readonly inTroubleshooting = computed(() => this.connectorEntity()?.component.state === 'TROUBLESHOOTING');

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
                this.store.dispatch(loadConnectorEntity({ connectorId: connectorId! }));
                this.store.dispatch(startConnectorControllerServicesEntityPolling({ connectorId: connectorId! }));
            });
    }

    ngOnDestroy(): void {
        this.store.dispatch(stopConnectorControllerServicesEntityPolling());
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

    canModifyParent(): (entity: ControllerServiceEntity) => boolean {
        const state = this.serviceState();
        if (!this.inTroubleshooting() || !state.breadcrumb) {
            return () => false;
        }
        const breadcrumbs = new Map<string, BreadcrumbEntity>();
        let breadcrumb: BreadcrumbEntity | undefined = state.breadcrumb;
        while (breadcrumb) {
            breadcrumbs.set(breadcrumb.id, breadcrumb);
            breadcrumb = breadcrumb.parentBreadcrumb;
        }
        return (entity) => !!entity.parentGroupId && !!breadcrumbs.get(entity.parentGroupId)?.permissions.canWrite;
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
        if (!entity.permissions.canRead || !entity.component) {
            return;
        }
        const component = entity.component;
        const request: DocumentationRequest = {
            parameters: {
                componentType: ComponentType.ControllerService,
                type: component.type,
                group: component.bundle.group,
                artifact: component.bundle.artifact,
                version: component.bundle.version
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
        if (!entity.permissions.canRead || !entity.component) {
            return;
        }
        if (this.inTroubleshooting()) {
            const request: EditControllerServiceDialogRequest = {
                id: entity.id,
                controllerService: entity,
                readonly: false
            };
            this.store.dispatch(openConfigureConnectorControllerServiceDialog({ request }));
        } else {
            this.store.dispatch(openViewControllerServiceDialog({ controllerService: entity }));
        }
    }

    goToControllerService(entity: ControllerServiceEntity): void {
        if (entity.parentGroupId) {
            this.store.dispatch(
                navigateToConnectorControllerService({
                    processGroupId: entity.parentGroupId,
                    serviceId: entity.id
                })
            );
        }
    }

    openNewControllerServiceDialog(): void {
        this.store.dispatch(openNewConnectorControllerServiceDialog());
    }

    enableControllerService(entity: ControllerServiceEntity): void {
        if (!entity.permissions.canRead || !entity.component) {
            return;
        }
        const request: SetEnableControllerServiceDialogRequest = {
            id: entity.id,
            controllerService: entity
        };
        this.store.dispatch(openEnableConnectorControllerServiceDialog({ request }));
    }

    disableControllerService(entity: ControllerServiceEntity): void {
        if (!entity.permissions.canRead || !entity.component) {
            return;
        }
        const request: DisableControllerServiceDialogRequest = {
            id: entity.id,
            controllerService: entity
        };
        this.store.dispatch(openDisableConnectorControllerServiceDialog({ request }));
    }

    changeControllerServiceVersion(entity: ControllerServiceEntity): void {
        if (!entity.permissions.canRead || !entity.component) {
            return;
        }
        const component = entity.component;
        const request: FetchComponentVersionsRequest = {
            id: entity.id,
            uri: entity.uri,
            revision: entity.revision,
            type: component.type,
            bundle: component.bundle
        };
        this.store.dispatch(openChangeConnectorControllerServiceVersionDialog({ request }));
    }

    deleteControllerService(entity: ControllerServiceEntity): void {
        if (!entity.permissions.canRead || !entity.component) {
            return;
        }
        const request: DeleteControllerServiceRequest = { controllerService: entity };
        this.store.dispatch(promptConnectorControllerServiceDeletion({ request }));
    }

    clearBulletinsControllerService(entity: ControllerServiceEntity): void {
        const fromTimestamp = this.nifiCommon.getMostRecentBulletinTimestamp(entity.bulletins ?? []);
        if (fromTimestamp === null) {
            return;
        }
        const request: ClearBulletinsRequest = {
            uri: entity.uri,
            componentId: entity.id,
            componentType: ComponentType.ControllerService,
            fromTimestamp
        };
        this.store.dispatch(clearConnectorControllerServiceBulletins({ request }));
    }

    openAdvancedUi(entity: ControllerServiceEntity): void {
        this.store.dispatch(navigateToAdvancedConnectorControllerServiceUi({ id: entity.id }));
    }

    /**
     * View component state for a stateful controller service. The connector
     * canvas dispatches with connectorId so the component-state effect targets
     * the connector-scoped state endpoint instead of the framework endpoint.
     */
    viewStateControllerService(entity: ControllerServiceEntity): void {
        if (!entity.component) {
            return;
        }
        this.store.dispatch(
            getComponentStateAndOpenDialog({
                request: {
                    componentName: entity.component.name,
                    componentId: entity.id,
                    componentType: ComponentType.ControllerService,
                    canClear: this.inTroubleshooting() && entity.component.state === 'DISABLED',
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
