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

import { Component, OnDestroy } from '@angular/core';
import { Store } from '@ngrx/store';
import { filter, switchMap, take, tap } from 'rxjs';
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
    loadControllerServices,
    navigateToEditService,
    openConfigureControllerServiceDialog,
    openDisableControllerServiceDialog,
    openEnableControllerServiceDialog,
    openNewControllerServiceDialog,
    promptControllerServiceDeletion,
    resetControllerServicesState,
    selectControllerService
} from '../../state/controller-services/controller-services.actions';
import { initialState } from '../../state/controller-services/controller-services.reducer';
import { ControllerServiceEntity } from '../../../../state/shared';
import { BreadcrumbEntity } from '../../state/shared';

@Component({
    selector: 'controller-services',
    templateUrl: './controller-services.component.html',
    styleUrls: ['./controller-services.component.scss']
})
export class ControllerServices implements OnDestroy {
    serviceState$ = this.store.select(selectControllerServicesState);
    selectedServiceId$ = this.store.select(selectControllerServiceIdFromRoute);

    private currentProcessGroupId!: string;

    constructor(private store: Store<ControllerServicesState>) {
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

    configureControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            navigateToEditService({
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

    deleteControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            promptControllerServiceDeletion({
                request: {
                    controllerService: entity
                }
            })
        );
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

    ngOnDestroy(): void {
        this.store.dispatch(resetControllerServicesState());
    }
}
