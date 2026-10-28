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

import { Component, computed, inject, input, OnInit, output } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatTooltipModule } from '@angular/material/tooltip';
import { ComponentContext, ComponentType, Permissions, Revision, Storage } from '@nifi/shared';
import { BreadcrumbEntity } from '../../../state/shared';
import { isNamedComponentTarget, SelectionTarget } from '../canvas/utils/selection.utils';
import {
    areAllDisableable,
    areAllEnableable,
    areAnyRunnable,
    areAnyStoppable,
    canRunOnce
} from '../canvas/utils/run-status.utils';

@Component({
    selector: 'canvas-operation-control',
    templateUrl: './operation-control.component.html',
    styleUrl: './operation-control.component.scss',
    imports: [MatButtonModule, MatExpansionModule, MatTooltipModule, ComponentContext]
})
export class CanvasOperationControl implements OnInit {
    private static readonly CONTROL_VISIBILITY_KEY = 'graph-control-visibility';

    private storage = inject(Storage);

    selection = input<SelectionTarget[]>([]);
    breadcrumbEntity = input<BreadcrumbEntity | null>(null);
    canvasPermissions = input<Permissions | null>(null);
    currentProcessGroupId = input<string | null>(null);
    processGroupRevision = input<Revision | null>(null);
    storageKey = input<string>('operation-control');

    enable = output<SelectionTarget[]>();
    disable = output<SelectionTarget[]>();
    startComponents = output<SelectionTarget[]>();
    stop = output<SelectionTarget[]>();
    runOnce = output<SelectionTarget[]>();

    operationCollapsed = true;

    private effectiveSelection = computed<SelectionTarget[]>(() => {
        const selection = this.selection();
        if (selection.length > 0) {
            return selection;
        }
        const id = this.currentProcessGroupId();
        if (!id) {
            return [];
        }
        return [
            {
                entity: {
                    id,
                    uri: '',
                    revision: this.processGroupRevision() ?? { version: 0 },
                    permissions: this.canvasPermissions() ?? { canRead: false, canWrite: false },
                    position: { x: 0, y: 0 },
                    runningCount: 0,
                    stoppedCount: 0,
                    invalidCount: 0,
                    disabledCount: 0,
                    activeRemotePortCount: 0,
                    inactiveRemotePortCount: 0,
                    upToDateCount: 0,
                    locallyModifiedCount: 0,
                    staleCount: 0,
                    locallyModifiedAndStaleCount: 0,
                    syncFailureCount: 0,
                    localInputPortCount: 0,
                    localOutputPortCount: 0,
                    publicInputPortCount: 0,
                    publicOutputPortCount: 0,
                    inputPortCount: 0,
                    outputPortCount: 0,
                    resolvedExecutionEngine: 'STANDARD'
                },
                ui: { componentType: ComponentType.ProcessGroup }
            }
        ];
    });

    canEnable = computed(() => areAllEnableable(this.effectiveSelection()));
    canDisable = computed(() => areAllDisableable(this.effectiveSelection()));
    canStart = computed(() => areAnyRunnable(this.effectiveSelection()));
    canStop = computed(() => areAnyStoppable(this.effectiveSelection()));
    canRunOnce = computed(() => canRunOnce(this.effectiveSelection()));

    contextName = computed(() => {
        const selection = this.selection();
        const breadcrumb = this.breadcrumbEntity();
        if (selection.length === 0) {
            if (breadcrumb?.permissions.canRead) {
                return breadcrumb.breadcrumb.name;
            }
            return breadcrumb?.id ?? this.currentProcessGroupId() ?? '';
        }
        if (selection.length > 1) {
            return 'Multiple components selected';
        }
        const selected = selection[0];
        if (!selected.entity.permissions.canRead) {
            return selected.entity.id;
        }
        if (selected.ui.componentType === ComponentType.Connection) {
            return 'connection';
        }
        if (selected.ui.componentType === ComponentType.Label) {
            return 'label';
        }
        if (isNamedComponentTarget(selected)) {
            return selected.entity.component?.name ?? selected.entity.id;
        }
        return selected.entity.id;
    });

    contextType = computed((): ComponentType | null => {
        const selection = this.selection();
        if (selection.length === 0) {
            return ComponentType.ProcessGroup;
        }
        if (selection.length > 1) {
            return null;
        }
        return selection[0].ui.componentType;
    });

    contextId = computed(() => {
        const selection = this.selection();
        if (selection.length === 0) {
            return this.breadcrumbEntity()?.id ?? this.currentProcessGroupId() ?? '';
        }
        if (selection.length > 1) {
            return '';
        }
        return selection[0].entity.id;
    });

    ngOnInit(): void {
        this.restoreCollapsedState();
    }

    toggleCollapsed(collapsed: boolean): void {
        this.operationCollapsed = collapsed;
        this.persistCollapsedState(collapsed);
    }

    private restoreCollapsedState(): void {
        try {
            const item: { [key: string]: boolean } | null = this.storage.getItem(
                CanvasOperationControl.CONTROL_VISIBILITY_KEY
            );
            if (item) {
                this.operationCollapsed = !item[this.storageKey()];
            }
        } catch (_e) {
            // Storage is unavailable or corrupt; retain the default collapsed state.
        }
    }

    private persistCollapsedState(collapsed: boolean): void {
        let item: { [key: string]: boolean } | null = this.storage.getItem(
            CanvasOperationControl.CONTROL_VISIBILITY_KEY
        );
        if (item == null) {
            item = {};
        }
        item[this.storageKey()] = !collapsed;
        this.storage.setItem(CanvasOperationControl.CONTROL_VISIBILITY_KEY, item);
    }
}
