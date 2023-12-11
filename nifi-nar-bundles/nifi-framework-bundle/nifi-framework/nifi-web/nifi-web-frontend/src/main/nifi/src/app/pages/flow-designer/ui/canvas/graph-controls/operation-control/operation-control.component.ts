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
    deleteComponents,
    getParameterContextsAndOpenGroupComponentsDialog,
    navigateToEditComponent,
    navigateToEditCurrentProcessGroup,
    setOperationCollapsed
} from '../../../../state/flow/flow.actions';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../state';
import { CanvasUtils } from '../../../../service/canvas-utils.service';
import { initialState } from '../../../../state/flow/flow.reducer';
import { Storage } from '../../../../../../service/storage.service';
import { DeleteComponentRequest, MoveComponentRequest } from '../../../../state/flow';
import { NgIf } from '@angular/common';
import { BreadcrumbEntity } from '../../../../state/shared';

@Component({
    selector: 'operation-control',
    standalone: true,
    templateUrl: './operation-control.component.html',
    imports: [NgIf],
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
        private storage: Storage
    ) {
        try {
            const item = this.storage.getItem(OperationControl.CONTROL_VISIBILITY_KEY);
            if (item) {
                this.operationCollapsed = item[OperationControl.OPERATION_KEY] === false;
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
        let item = this.storage.getItem(OperationControl.CONTROL_VISIBILITY_KEY);
        if (item == null) {
            item = {};
        }

        item[OperationControl.OPERATION_KEY] = !this.operationCollapsed;
        this.storage.setItem(OperationControl.CONTROL_VISIBILITY_KEY, item);
    }

    getContextIcon(selection: any): string {
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

    getContextName(selection: any): string {
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

    getContextType(selection: any): string {
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

    getContextId(selection: any): string {
        if (selection.size() === 0) {
            return this.breadcrumbEntity.id;
        } else if (selection.size() > 1) {
            return '';
        }

        const selectionData: any = selection.datum();
        return selectionData.id;
    }

    canConfigure(selection: any): boolean {
        return this.canvasUtils.isConfigurable(selection);
    }

    configure(selection: any): void {
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

    canManageAccess(selection: any): boolean {
        // TODO
        return false;
    }

    manageAccess(selection: any): void {
        // TODO
    }

    canEnable(selection: any): boolean {
        // TODO - canEnable
        return false;
    }

    enable(selection: any): void {
        // TODO - enable
    }

    canDisable(selection: any): boolean {
        // TODO - canDisable
        return false;
    }

    disable(selection: any): void {
        // TODO - disable
    }

    canStart(selection: any): boolean {
        // TODO - isRunnable
        return false;
    }

    start(selection: any): void {
        // TODO - start
    }

    canStop(selection: any): boolean {
        // TODO - isStoppable
        return false;
    }

    stop(selection: any): void {
        // TODO - stop
    }

    canCopy(selection: any): boolean {
        // TODO - isCopyable
        return false;
    }

    copy(selection: any): void {
        // TODO - copy
    }

    canPaste(selection: any): boolean {
        // TODO - isPastable
        return false;
    }

    paste(selection: any): void {
        // TODO - paste
    }

    canGroup(selection: any): boolean {
        return this.canvasUtils.isDisconnected(selection);
    }

    group(selection: any): void {
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

    canColor(selection: any): boolean {
        // TODO
        return false;
    }

    color(selection: any): void {
        // TODO
    }

    canDelete(selection: any): boolean {
        return this.canvasUtils.areDeletable(selection);
    }

    delete(selection: any): void {
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
