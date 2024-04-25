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

import { Component, Input } from '@angular/core';
import {
    copy,
    deleteComponents,
    disableComponents,
    disableCurrentProcessGroup,
    enableComponents,
    enableCurrentProcessGroup,
    getParameterContextsAndOpenGroupComponentsDialog,
    navigateToEditComponent,
    navigateToEditCurrentProcessGroup,
    navigateToManageComponentPolicies,
    paste,
    setOperationCollapsed,
    startComponents,
    startCurrentProcessGroup,
    stopComponents,
    stopCurrentProcessGroup
} from '../../../../state/flow/flow.actions';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../state';
import { CanvasUtils } from '../../../../service/canvas-utils.service';
import { initialState } from '../../../../state/flow/flow.reducer';
import { Storage } from '../../../../../../service/storage.service';
import {
    CopyComponentRequest,
    DeleteComponentRequest,
    DisableComponentRequest,
    EnableComponentRequest,
    MoveComponentRequest,
    StartComponentRequest,
    StopComponentRequest
} from '../../../../state/flow';

import { BreadcrumbEntity } from '../../../../state/shared';
import { ComponentType } from '../../../../../../state/shared';
import { MatButtonModule } from '@angular/material/button';
import * as d3 from 'd3';
import { CanvasView } from '../../../../service/canvas-view.service';
import { Client } from '../../../../../../service/client.service';

@Component({
    selector: 'operation-control',
    standalone: true,
    templateUrl: './operation-control.component.html',
    imports: [MatButtonModule],
    styleUrls: ['./operation-control.component.scss']
})
export class OperationControl {
    private static readonly CONTROL_VISIBILITY_KEY: string = 'graph-control-visibility';
    private static readonly OPERATION_KEY: string = 'operation-control';

    @Input() shouldDockWhenCollapsed!: boolean;
    @Input() breadcrumbEntity!: BreadcrumbEntity;

    operationCollapsed: boolean = initialState.operationCollapsed;

    constructor(
        private store: Store<CanvasState>,
        public canvasUtils: CanvasUtils,
        private canvasView: CanvasView,
        private client: Client,
        private storage: Storage
    ) {
        try {
            const item: { [key: string]: boolean } | null = this.storage.getItem(
                OperationControl.CONTROL_VISIBILITY_KEY
            );
            if (item) {
                this.operationCollapsed = !item[OperationControl.OPERATION_KEY];
                this.store.dispatch(setOperationCollapsed({ operationCollapsed: this.operationCollapsed }));
            }
        } catch (e) {
            // likely could not parse item... ignoring
        }
    }

    toggleCollapsed(): void {
        this.operationCollapsed = !this.operationCollapsed;
        this.store.dispatch(setOperationCollapsed({ operationCollapsed: this.operationCollapsed }));

        // update the current value in storage
        let item: { [key: string]: boolean } | null = this.storage.getItem(OperationControl.CONTROL_VISIBILITY_KEY);
        if (item == null) {
            item = {};
        }

        item[OperationControl.OPERATION_KEY] = !this.operationCollapsed;
        this.storage.setItem(OperationControl.CONTROL_VISIBILITY_KEY, item);
    }

    getContextIcon(selection: d3.Selection<any, any, any, any>): string {
        if (selection.size() === 0) {
            if (this.breadcrumbEntity.parentBreadcrumb == null) {
                return 'icon-drop';
            } else {
                return 'icon-group';
            }
        } else if (selection.size() > 1) {
            return 'icon-drop';
        }

        if (this.canvasUtils.isProcessor(selection)) {
            return 'icon-processor';
        } else if (this.canvasUtils.isInputPort(selection)) {
            return 'icon-port-in';
        } else if (this.canvasUtils.isOutputPort(selection)) {
            return 'icon-port-out';
        } else if (this.canvasUtils.isFunnel(selection)) {
            return 'icon-funnel';
        } else if (this.canvasUtils.isLabel(selection)) {
            return 'icon-label';
        } else if (this.canvasUtils.isProcessGroup(selection)) {
            return 'icon-group';
        } else if (this.canvasUtils.isRemoteProcessGroup(selection)) {
            return 'icon-group-remote';
        } else {
            return 'icon-connect';
        }
    }

    getContextName(selection: d3.Selection<any, any, any, any>): string {
        if (selection.size() === 0) {
            if (this.breadcrumbEntity.permissions.canRead) {
                return this.breadcrumbEntity.breadcrumb.name;
            } else {
                return this.breadcrumbEntity.id;
            }
        } else if (selection.size() > 1) {
            return 'Multiple components selected';
        }

        const selectionData: any = selection.datum();
        if (!selectionData.permissions.canRead) {
            return selectionData.id;
        } else {
            if (this.canvasUtils.isLabel(selection)) {
                return 'label';
            } else if (this.canvasUtils.isConnection(selection)) {
                return 'connection';
            } else {
                return selectionData.component.name;
            }
        }
    }

    getContextType(selection: d3.Selection<any, any, any, any>): string {
        if (selection.size() === 0) {
            return 'Process Group';
        } else if (selection.size() > 1) {
            return '';
        }

        if (this.canvasUtils.isProcessor(selection)) {
            return 'Processor';
        } else if (this.canvasUtils.isInputPort(selection)) {
            return 'Input Port';
        } else if (this.canvasUtils.isOutputPort(selection)) {
            return 'Output Port';
        } else if (this.canvasUtils.isFunnel(selection)) {
            return 'Funnel';
        } else if (this.canvasUtils.isLabel(selection)) {
            return 'Label';
        } else if (this.canvasUtils.isProcessGroup(selection)) {
            return 'Process Group';
        } else if (this.canvasUtils.isRemoteProcessGroup(selection)) {
            return 'Remote Process Group';
        } else {
            return 'Connection';
        }
    }

    getContextId(selection: d3.Selection<any, any, any, any>): string {
        if (selection.size() === 0) {
            return this.breadcrumbEntity.id;
        } else if (selection.size() > 1) {
            return '';
        }

        const selectionData: any = selection.datum();
        return selectionData.id;
    }

    canConfigure(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.isConfigurable(selection);
    }

    configure(selection: d3.Selection<any, any, any, any>): void {
        if (selection.empty()) {
            this.store.dispatch(navigateToEditCurrentProcessGroup());
        } else {
            const selectionData = selection.datum();
            this.store.dispatch(
                navigateToEditComponent({
                    request: {
                        type: selectionData.type,
                        id: selectionData.id
                    }
                })
            );
        }
    }

    supportsManagedAuthorizer(): boolean {
        return this.canvasUtils.supportsManagedAuthorizer();
    }

    canManageAccess(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.canManagePolicies(selection);
    }

    manageAccess(selection: d3.Selection<any, any, any, any>): void {
        if (selection.empty()) {
            this.store.dispatch(
                navigateToManageComponentPolicies({
                    request: {
                        resource: 'process-groups',
                        id: this.breadcrumbEntity.id
                    }
                })
            );
        } else {
            const selectionData = selection.datum();
            const componentType: ComponentType = selectionData.type;

            let resource = 'process-groups';
            switch (componentType) {
                case ComponentType.Processor:
                    resource = 'processors';
                    break;
                case ComponentType.InputPort:
                    resource = 'input-ports';
                    break;
                case ComponentType.OutputPort:
                    resource = 'output-ports';
                    break;
                case ComponentType.Funnel:
                    resource = 'funnels';
                    break;
                case ComponentType.Label:
                    resource = 'labels';
                    break;
                case ComponentType.RemoteProcessGroup:
                    resource = 'remote-process-groups';
                    break;
            }

            this.store.dispatch(
                navigateToManageComponentPolicies({
                    request: {
                        resource,
                        id: selectionData.id
                    }
                })
            );
        }
    }

    canEnable(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.canEnable(selection);
    }

    enable(selection: d3.Selection<any, any, any, any>): void {
        if (selection.empty()) {
            // attempting to enable the current process group
            this.store.dispatch(enableCurrentProcessGroup());
        } else {
            const components: EnableComponentRequest[] = [];
            const enableable = this.canvasUtils.filterEnable(selection);
            enableable.each((d: any) => {
                components.push({
                    id: d.id,
                    uri: d.uri,
                    type: d.type,
                    revision: this.client.getRevision(d),
                    errorStrategy: 'snackbar'
                });
            });
            this.store.dispatch(
                enableComponents({
                    request: {
                        components
                    }
                })
            );
        }
    }

    canDisable(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.canDisable(selection);
    }

    disable(selection: d3.Selection<any, any, any, any>): void {
        if (selection.empty()) {
            // attempting to disable the current process group
            this.store.dispatch(disableCurrentProcessGroup());
        } else {
            const components: DisableComponentRequest[] = [];
            const disableable = this.canvasUtils.filterDisable(selection);
            disableable.each((d: any) => {
                components.push({
                    id: d.id,
                    uri: d.uri,
                    type: d.type,
                    revision: this.client.getRevision(d),
                    errorStrategy: 'snackbar'
                });
            });
            this.store.dispatch(
                disableComponents({
                    request: {
                        components
                    }
                })
            );
        }
    }

    canStart(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.areAnyRunnable(selection);
    }

    start(selection: d3.Selection<any, any, any, any>): void {
        if (selection.empty()) {
            // attempting to start the current process group
            this.store.dispatch(startCurrentProcessGroup());
        } else {
            const components: StartComponentRequest[] = [];
            const startable = this.canvasUtils.getStartable(selection);
            startable.each((d: any) => {
                components.push({
                    id: d.id,
                    uri: d.uri,
                    type: d.type,
                    revision: this.client.getRevision(d),
                    errorStrategy: 'snackbar'
                });
            });
            this.store.dispatch(
                startComponents({
                    request: {
                        components
                    }
                })
            );
        }
    }

    canStop(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.areAnyStoppable(selection);
    }

    stop(selection: d3.Selection<any, any, any, any>): void {
        if (selection.empty()) {
            // attempting to start the current process group
            this.store.dispatch(stopCurrentProcessGroup());
        } else {
            const components: StopComponentRequest[] = [];
            const stoppable = this.canvasUtils.getStoppable(selection);
            stoppable.each((d: any) => {
                components.push({
                    id: d.id,
                    uri: d.uri,
                    type: d.type,
                    revision: this.client.getRevision(d),
                    errorStrategy: 'snackbar'
                });
            });
            this.store.dispatch(
                stopComponents({
                    request: {
                        components
                    }
                })
            );
        }
    }

    canCopy(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.isCopyable(selection);
    }

    copy(selection: d3.Selection<any, any, any, any>): void {
        const components: CopyComponentRequest[] = [];
        selection.each((d) => {
            components.push({
                id: d.id,
                type: d.type,
                uri: d.uri,
                entity: d
            });
        });

        const origin = this.canvasUtils.getOrigin(selection);
        const dimensions = this.canvasView.getSelectionBoundingClientRect(selection);

        this.store.dispatch(
            copy({
                request: {
                    components,
                    origin,
                    dimensions
                }
            })
        );
    }

    canPaste(): boolean {
        return this.canvasUtils.isPastable();
    }

    paste(): void {
        this.store.dispatch(
            paste({
                request: {}
            })
        );
    }

    canGroup(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.isDisconnected(selection);
    }

    group(selection: d3.Selection<any, any, any, any>): void {
        const moveComponents: MoveComponentRequest[] = [];
        selection.each(function (d: any) {
            moveComponents.push({
                id: d.id,
                type: d.type,
                uri: d.uri,
                entity: d
            });
        });

        // move the selection into the group
        this.store.dispatch(
            getParameterContextsAndOpenGroupComponentsDialog({
                request: {
                    moveComponents,
                    position: this.canvasUtils.getOrigin(selection)
                }
            })
        );
    }

    canColor(selection: d3.Selection<any, any, any, any>): boolean {
        // TODO
        return false;
    }

    color(selection: d3.Selection<any, any, any, any>): void {
        // TODO
    }

    canDelete(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.areDeletable(selection);
    }

    delete(selection: d3.Selection<any, any, any, any>): void {
        if (selection.size() === 1) {
            const selectionData = selection.datum();
            this.store.dispatch(
                deleteComponents({
                    request: [
                        {
                            id: selectionData.id,
                            type: selectionData.type,
                            uri: selectionData.uri,
                            entity: selectionData
                        }
                    ]
                })
            );
        } else {
            const requests: DeleteComponentRequest[] = [];
            selection.each(function (d: any) {
                requests.push({
                    id: d.id,
                    type: d.type,
                    uri: d.uri,
                    entity: d
                });
            });
            this.store.dispatch(
                deleteComponents({
                    request: requests
                })
            );
        }
    }
}
