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

import { Component, OnDestroy, inject, signal, computed } from '@angular/core';
import { Store } from '@ngrx/store';
import { filter, Observable, switchMap, take, tap } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
    selectControllerServiceIdFromRoute,
    selectControllerServicesState,
    selectProcessGroupIdFromRoute,
    selectService,
    selectSingleEditedService
} from '../../state/controller-services/controller-services.selectors';
import { ControllerServicesState } from '../../state/controller-services';
import {
    clearControllerServiceBulletins,
    loadControllerServices,
    navigateToAdvancedServiceUi,
    navigateToEditService,
    navigateToManageComponentPolicies,
    navigateToService,
    openChangeControllerServiceVersionDialog,
    openConfigureControllerServiceDialog,
    openDisableControllerServiceDialog,
    openEnableControllerServiceDialog,
    openNewControllerServiceDialog,
    promptControllerServiceDeletion,
    resetControllerServicesState,
    selectControllerService
} from '../../state/controller-services/controller-services.actions';
import { initialState } from '../../state/controller-services/controller-services.reducer';
import { ComponentType, isDefinedAndNotNull, NiFiCommon, TextTip } from '@nifi/shared';
import { ControllerServiceEntity } from '../../../../state/shared';
import { BreadcrumbEntity } from '../../state/shared';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { NiFiState } from '../../../../state';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { FlowConfiguration } from '../../../../state/flow-configuration';
import { DocumentationRequest } from '../../../../state/documentation';

@Component({
    selector: 'controller-services',
    templateUrl: './controller-services.component.html',
    styleUrls: ['./controller-services.component.scss'],
    standalone: false
})
export class ControllerServices implements OnDestroy {
    private store = inject<Store<NiFiState>>(Store);
    private nifiCommon = inject(NiFiCommon);

    // Convert observables to signals
    serviceState = this.store.selectSignal(selectControllerServicesState);
    selectedServiceId$ = this.store.select(selectControllerServiceIdFromRoute);
    currentUser$ = this.store.select(selectCurrentUser);
    flowConfiguration$: Observable<FlowConfiguration> = this.store
        .select(selectFlowConfiguration)
        .pipe(isDefinedAndNotNull());

    private currentProcessGroupId!: string;

    // Expose TextTip for template
    protected readonly TextTip = TextTip;

    // Filter state
    showCurrentScopeOnly = signal(false);

    // Computed filtered controller services using signals
    filteredControllerServices = computed(() => {
        const serviceState = this.serviceState();
        if (!serviceState) {
            return [];
        }

        const services = serviceState.controllerServices;

        if (!this.showCurrentScopeOnly()) {
            return services;
        }

        // Filter to show only services defined in the current process group
        const filterFn = this.definedByCurrentGroup(serviceState.breadcrumb);
        return services.filter(filterFn);
    });

    // Computed property to determine if we should show the filter
    shouldShowFilter = computed(() => {
        const serviceState = this.serviceState();
        if (!serviceState) {
            return false;
        }

        // Don't show filter if we're at the root process group (no parent breadcrumb)
        return !!serviceState.breadcrumb.parentBreadcrumb;
    });

    constructor() {
        // load the controller services using the process group id from the route
        this.store
            .select(selectProcessGroupIdFromRoute)
            .pipe(
                filter((processGroupId) => processGroupId != null),
                tap((processGroupId) => (this.currentProcessGroupId = processGroupId)),
                takeUntilDestroyed()
            )
            .subscribe((processGroupId) => {
                this.store.dispatch(
                    loadControllerServices({
                        request: {
                            processGroupId
                        }
                    })
                );
            });

        this.store
            .select(selectSingleEditedService)
            .pipe(
                filter((id: string) => id != null),
                switchMap((id: string) =>
                    this.store.select(selectService(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((entity) => {
                if (entity) {
                    this.store.dispatch(
                        openConfigureControllerServiceDialog({
                            request: {
                                id: entity.id,
                                controllerService: entity
                            }
                        })
                    );
                }
            });
    }

    isInitialLoading(state: ControllerServicesState): boolean {
        // using the current timestamp to detect the initial load event
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    openNewControllerServiceDialog(): void {
        this.store.dispatch(openNewControllerServiceDialog());
    }

    refreshControllerServiceListing(): void {
        this.store.dispatch(
            loadControllerServices({
                request: {
                    processGroupId: this.currentProcessGroupId
                }
            })
        );
    }

    formatScope(breadcrumb: BreadcrumbEntity): (entity: ControllerServiceEntity) => string {
        const breadcrumbs: BreadcrumbEntity[] = [];

        let currentBreadcrumb: BreadcrumbEntity | undefined = breadcrumb;
        while (currentBreadcrumb != null) {
            breadcrumbs.push(currentBreadcrumb);
            currentBreadcrumb = currentBreadcrumb.parentBreadcrumb;
        }

        return (entity: ControllerServiceEntity): string => {
            const entityBreadcrumb: BreadcrumbEntity | undefined = breadcrumbs.find(
                (bc) => bc.id === entity.parentGroupId
            );

            if (entityBreadcrumb) {
                if (entityBreadcrumb.permissions.canRead) {
                    return entityBreadcrumb.breadcrumb.name;
                }

                return entityBreadcrumb.id;
            }

            return '';
        };
    }

    definedByCurrentGroup(breadcrumbs: BreadcrumbEntity): (entity: ControllerServiceEntity) => boolean {
        return (entity: ControllerServiceEntity): boolean => {
            return breadcrumbs.id === entity.parentGroupId;
        };
    }

    viewControllerServiceDocumentation(entity: ControllerServiceEntity): void {
        const request: DocumentationRequest = {
            parameters: {
                componentType: ComponentType.ControllerService,
                type: entity.component.type,
                group: entity.component.bundle.group,
                artifact: entity.component.bundle.artifact,
                version: entity.component.bundle.version
            }
        };

        if (entity.parentGroupId) {
            request.backNavigation = {
                route: ['/process-groups', entity.parentGroupId, 'controller-services', entity.id],
                routeBoundary: ['/documentation'],
                context: 'Controller Service'
            };
        }

        this.store.dispatch(
            navigateToComponentDocumentation({
                request
            })
        );
    }

    configureControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            navigateToEditService({
                id: entity.id
            })
        );
    }

    openAdvancedUi(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            navigateToAdvancedServiceUi({
                id: entity.id
            })
        );
    }

    navigateToControllerService(entity: ControllerServiceEntity): void {
        if (entity.parentGroupId) {
            this.store.dispatch(
                navigateToService({
                    request: {
                        id: entity.id,
                        processGroupId: entity.parentGroupId
                    }
                })
            );
        }
    }

    navigateToManageComponentPolicies(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            navigateToManageComponentPolicies({
                id: entity.id
            })
        );
    }

    enableControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            openEnableControllerServiceDialog({
                request: {
                    id: entity.id,
                    controllerService: entity
                }
            })
        );
    }

    disableControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            openDisableControllerServiceDialog({
                request: {
                    id: entity.id,
                    controllerService: entity
                }
            })
        );
    }

    viewStateControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            getComponentStateAndOpenDialog({
                request: {
                    componentType: ComponentType.ControllerService,
                    componentId: entity.id,
                    componentName: entity.component.name,
                    canClear: entity.component.state === 'DISABLED'
                }
            })
        );
    }

    changeControllerServiceVersion(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            openChangeControllerServiceVersionDialog({
                request: {
                    id: entity.id,
                    bundle: entity.component.bundle,
                    uri: entity.uri,
                    type: entity.component.type,
                    revision: entity.revision
                }
            })
        );
    }

    deleteControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            promptControllerServiceDeletion({
                request: {
                    controllerService: entity
                }
            })
        );
    }

    canModifyParent(breadcrumb: BreadcrumbEntity): (entity: ControllerServiceEntity) => boolean {
        const breadcrumbs: BreadcrumbEntity[] = [];

        let currentBreadcrumb: BreadcrumbEntity | undefined = breadcrumb;
        while (currentBreadcrumb != null) {
            breadcrumbs.push(currentBreadcrumb);
            currentBreadcrumb = currentBreadcrumb.parentBreadcrumb;
        }

        return (entity: ControllerServiceEntity): boolean => {
            const entityBreadcrumb: BreadcrumbEntity | undefined = breadcrumbs.find(
                (bc) => bc.id === entity.parentGroupId
            );

            if (entityBreadcrumb) {
                return entityBreadcrumb.permissions.canWrite;
            }

            return false;
        };
    }

    selectControllerService(entity: ControllerServiceEntity): void {
        // this service listing shows all services in the current group and any
        // ancestor group. in this context we don't want the user to navigate away
        // from this group, so we are using the current process group id
        this.store.dispatch(
            selectControllerService({
                request: {
                    processGroupId: this.currentProcessGroupId,
                    id: entity.id
                }
            })
        );
    }

    clearBulletinsControllerService(entity: ControllerServiceEntity): void {
        // Get the most recent bulletin timestamp from the entity's bulletins
        // This will be reconstructed from the time-only string to a full timestamp
        const fromTimestamp = this.nifiCommon.getMostRecentBulletinTimestamp(entity.bulletins || []);
        if (fromTimestamp === null) {
            return; // no bulletins to clear
        }

        this.store.dispatch(
            clearControllerServiceBulletins({
                request: {
                    uri: entity.uri,
                    fromTimestamp,
                    componentId: entity.id,
                    componentType: ComponentType.ControllerService
                }
            })
        );
    }

    // Filter methods
    toggleFilter(): void {
        this.showCurrentScopeOnly.set(!this.showCurrentScopeOnly());
    }

    getFilterTooltip(breadcrumb: BreadcrumbEntity): string {
        const processGroupName = breadcrumb.permissions.canRead ? breadcrumb.breadcrumb.name : breadcrumb.id;
        return `Only show the Controller Services defined in the current Process Group: '${processGroupName}'`;
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetControllerServicesState());
    }
}
