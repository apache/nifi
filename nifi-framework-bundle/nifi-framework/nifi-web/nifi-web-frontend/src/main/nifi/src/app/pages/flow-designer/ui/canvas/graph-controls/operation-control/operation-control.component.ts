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
import { setOperationCollapsed } from '../../../../state/flow/flow.actions';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../state';
import { CanvasUtils } from '../../../../service/canvas-utils.service';
import { initialState } from '../../../../state/flow/flow.reducer';
import { Storage } from '../../../../../../service/storage.service';

import { BreadcrumbEntity } from '../../../../state/shared';
import { MatButtonModule } from '@angular/material/button';
import * as d3 from 'd3';
import { CanvasView } from '../../../../service/canvas-view.service';
import { Client } from '../../../../../../service/client.service';
import { CanvasActionsService } from '../../../../service/canvas-actions.service';
import { ComponentContext } from '../../../../../../ui/common/component-context/component-context.component';
import { ComponentType } from '../../../../../../state/shared';

@Component({
    selector: 'operation-control',
    standalone: true,
    templateUrl: './operation-control.component.html',
    imports: [MatButtonModule, ComponentContext],
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
        private storage: Storage,
        private canvasActionsService: CanvasActionsService
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

    getContextType(selection: d3.Selection<any, any, any, any>): ComponentType | null {
        if (selection.size() === 0) {
            return ComponentType.ProcessGroup;
        } else if (selection.size() > 1) {
            return null;
        }

        if (this.canvasUtils.isProcessor(selection)) {
            return ComponentType.Processor;
        } else if (this.canvasUtils.isInputPort(selection)) {
            return ComponentType.InputPort;
        } else if (this.canvasUtils.isOutputPort(selection)) {
            return ComponentType.OutputPort;
        } else if (this.canvasUtils.isFunnel(selection)) {
            return ComponentType.Funnel;
        } else if (this.canvasUtils.isLabel(selection)) {
            return ComponentType.Label;
        } else if (this.canvasUtils.isProcessGroup(selection)) {
            return ComponentType.ProcessGroup;
        } else if (this.canvasUtils.isRemoteProcessGroup(selection)) {
            return ComponentType.RemoteProcessGroup;
        } else {
            return ComponentType.Connection;
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
        return this.canvasActionsService.getConditionFunction('configure')(selection);
    }

    configure(selection: d3.Selection<any, any, any, any>): void {
        this.canvasActionsService.getActionFunction('configure')(selection);
    }

    supportsManagedAuthorizer(): boolean {
        return this.canvasUtils.supportsManagedAuthorizer();
    }

    canManageAccess(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasActionsService.getConditionFunction('manageAccess')(selection);
    }

    manageAccess(selection: d3.Selection<any, any, any, any>): void {
        this.canvasActionsService.getActionFunction('manageAccess')(selection, {
            processGroupId: this.breadcrumbEntity.id
        });
    }

    canEnable(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasActionsService.getConditionFunction('enable')(selection);
    }

    enable(selection: d3.Selection<any, any, any, any>): void {
        this.canvasActionsService.getActionFunction('enable')(selection);
    }

    canDisable(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasActionsService.getConditionFunction('disable')(selection);
    }

    disable(selection: d3.Selection<any, any, any, any>): void {
        this.canvasActionsService.getActionFunction('disable')(selection);
    }

    canStart(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasUtils.areAnyRunnable(selection);
    }

    start(selection: d3.Selection<any, any, any, any>): void {
        this.canvasActionsService.getActionFunction('start')(selection);
    }

    canStop(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasActionsService.getConditionFunction('stop')(selection);
    }

    stop(selection: d3.Selection<any, any, any, any>): void {
        this.canvasActionsService.getActionFunction('stop')(selection);
    }

    canCopy(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasActionsService.getConditionFunction('copy')(selection);
    }

    copy(selection: d3.Selection<any, any, any, any>): void {
        this.canvasActionsService.getActionFunction('copy')(selection);
    }

    canPaste(): boolean {
        return this.canvasActionsService.getConditionFunction('paste')(d3.select(null));
    }

    paste(): void {
        return this.canvasActionsService.getActionFunction('paste')(d3.select(null));
    }

    canGroup(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasActionsService.getConditionFunction('group')(selection);
    }

    group(selection: d3.Selection<any, any, any, any>): void {
        this.canvasActionsService.getActionFunction('group')(selection);
    }

    canColor(selection: d3.Selection<any, any, any, any>): boolean {
        // TODO
        return false;
    }

    color(selection: d3.Selection<any, any, any, any>): void {
        // TODO
    }

    canDelete(selection: d3.Selection<any, any, any, any>): boolean {
        return this.canvasActionsService.getConditionFunction('delete')(selection);
    }

    delete(selection: d3.Selection<any, any, any, any>): void {
        this.canvasActionsService.getActionFunction('delete')(selection);
    }
}
