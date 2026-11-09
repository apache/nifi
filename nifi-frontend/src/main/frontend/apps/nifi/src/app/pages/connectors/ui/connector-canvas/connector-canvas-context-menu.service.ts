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

import { Injectable, computed, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { canOperateConnector, ComponentType, isConnectorActionAllowed } from '@nifi/shared';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';
import * as EmptyQueueActions from '../../../../state/empty-queue/empty-queue.actions';
import { ContextMenuContext, isProcessorDatum } from '../../../../ui/common/canvas/canvas.types';
import {
    ContextMenuDefinition,
    ContextMenuDefinitionProvider,
    ContextMenuItemDefinition
} from '../../../../ui/common/context-menu/context-menu.component';
import { SelectionTarget } from '../../../../ui/common/canvas/utils/selection.utils';
import {
    areAllDisableable,
    areAllEnableable,
    areAnyRunnable,
    areAnyStoppable,
    canRunOnce,
    canTerminate
} from '../../../../ui/common/canvas/utils/run-status.utils';
import * as ConnectorCanvasActions from '../../state/connector-canvas/connector-canvas.actions';
import { selectConnectorCanvasPermissions } from '../../state/connector-canvas/connector-canvas.selectors';
import * as ConnectorCanvasEntityActions from '../../state/connector-canvas-entity/connector-canvas-entity.actions';
import {
    selectConnectorCanvasEntity,
    selectConnectorCanvasEntitySaving
} from '../../state/connector-canvas-entity/connector-canvas-entity.selectors';
import { areDeletable } from '../../../../ui/common/canvas/utils/deletion.utils';
import { ConnectorCanvasCommands } from './connector-canvas-commands.service';

/**
 * View-side state the menu cannot derive from the store. The page component
 * tracks the route-resolved connector and process group and owns the canvas view child.
 */
export interface ConnectorCanvasContextMenuHost {
    connectorId(): string;
    processGroupId(): string | null;
    canNavigateToParent(): boolean;
    centerOnSelection(): void;
}

/**
 * Context menu provider for the connector canvas. Component-scoped so each
 * `<connector-canvas>` owns its own context and host.
 */
@Injectable()
export class ConnectorCanvasContextMenu implements ContextMenuDefinitionProvider {
    private store = inject(Store);
    private commands = inject(ConnectorCanvasCommands);

    private host: ConnectorCanvasContextMenuHost | null = null;
    private currentContext: ContextMenuContext | null = null;

    private canvasPermissions = this.store.selectSignal(selectConnectorCanvasPermissions);
    private connectorEntity = this.store.selectSignal(selectConnectorCanvasEntity);
    private entitySaving = this.store.selectSignal(selectConnectorCanvasEntitySaving);
    private currentUser = this.store.selectSignal(selectCurrentUser);

    inTroubleshooting = computed(() => this.connectorEntity()?.component.state === 'TROUBLESHOOTING');
    canEdit = computed(() => this.inTroubleshooting() && this.canvasPermissions()?.canWrite === true);
    canAccessProvenance = computed(() => this.currentUser().provenancePermissions.canRead);

    setHost(host: ConnectorCanvasContextMenuHost): void {
        this.host = host;
    }

    setContext(context: ContextMenuContext): void {
        this.currentContext = context;
    }

    getMenu(menuId: string): ContextMenuDefinition | undefined {
        if (menuId === 'download') {
            return {
                id: menuId,
                menuItems: [
                    {
                        text: 'Without External Services',
                        clazz: 'fa',
                        action: () => this.downloadFlowAction(false)
                    },
                    {
                        text: 'With External Services',
                        clazz: 'fa',
                        action: () => this.downloadFlowAction(true)
                    }
                ]
            };
        }

        if (menuId !== 'root') {
            return undefined;
        }

        const context = this.currentContext;
        let menuItems: ContextMenuItemDefinition[] = [];

        if (context?.targetType === 'canvas') {
            menuItems = this.getCanvasMenuItems();
        } else if (context?.targetType === 'component') {
            menuItems = this.getComponentMenuItems(context);
        }

        return { id: menuId, menuItems };
    }

    filterMenuItem(menuItem: ContextMenuItemDefinition): boolean {
        return menuItem.condition ? menuItem.condition(null) : true;
    }

    menuItemClicked(menuItem: ContextMenuItemDefinition, event: MouseEvent): void {
        menuItem.action?.(null, event);
    }

    hasDeletableSelection(targets: SelectionTarget[]): boolean {
        return areDeletable(targets, this.host?.processGroupId() ?? null);
    }

    private getCanvasMenuItems(): ContextMenuItemDefinition[] {
        return [
            {
                text: 'Start',
                clazz: 'fa fa-play',
                condition: () => this.inTroubleshooting() && this.processGroupId() !== null,
                action: () => this.commands.startComponents([])
            },
            {
                text: 'Stop',
                clazz: 'fa fa-stop',
                condition: () => this.inTroubleshooting() && this.processGroupId() !== null,
                action: () => this.commands.stopComponents([])
            },
            {
                text: 'Enable',
                clazz: 'fa fa-flash',
                condition: () => this.inTroubleshooting() && this.processGroupId() !== null,
                action: () => this.commands.enableComponents([])
            },
            {
                text: 'Disable',
                clazz: 'icon icon-enable-false',
                condition: () => this.inTroubleshooting() && this.processGroupId() !== null,
                action: () => this.commands.disableComponents([])
            },
            { isSeparator: true },
            {
                text: 'Refresh',
                clazz: 'fa fa-refresh',
                condition: () => true,
                action: () => this.refreshAction(),
                shortcut: { control: true, code: 'R' }
            },
            {
                text: 'Leave Group',
                clazz: 'fa fa-level-up',
                condition: () => this.canNavigateToParent(),
                action: () => this.leaveGroupAction(),
                shortcut: { code: 'ESC' }
            },
            { isSeparator: true },
            {
                text: 'Controller Services',
                clazz: 'fa fa-list',
                condition: () => !!this.processGroupId(),
                action: () => this.controllerServicesAction(this.processGroupId()!)
            },
            {
                text: 'Download Flow Definition',
                clazz: 'fa',
                subMenuId: 'download',
                condition: () => this.canDownloadFlow()
            },
            { isSeparator: true },
            {
                text: 'Empty All Queues',
                clazz: 'fa fa-minus-circle',
                condition: () => !!this.processGroupId(),
                action: () => this.emptyAllQueuesAction(this.processGroupId()!)
            },
            { isSeparator: true },
            {
                text: 'Drain',
                clazz: 'fa',
                condition: () => this.canDrain(),
                action: () => this.drainConnectorAction()
            },
            {
                text: 'Cancel Drain',
                clazz: 'fa',
                condition: () => this.canCancelDrain(),
                action: () => this.cancelDrainConnectorAction()
            }
        ];
    }

    private getComponentMenuItems(context: ContextMenuContext): ContextMenuItemDefinition[] {
        const clicked = context.clickedComponent;
        const isSingleSelection = context.selectedComponents.length <= 1;
        const isProcessGroup = clicked?.ui.componentType === ComponentType.ProcessGroup;
        const isConnection = clicked?.ui.componentType === ComponentType.Connection;
        const isFunnel = clicked?.ui.componentType === ComponentType.Funnel;
        const isProvenanceTarget =
            !!clicked &&
            isSingleSelection &&
            !isProcessGroup &&
            !isConnection &&
            clicked.ui.componentType !== ComponentType.RemoteProcessGroup &&
            clicked.ui.componentType !== ComponentType.Label;
        const isViewConfigurationTarget =
            !!clicked && isSingleSelection && !isFunnel && clicked.entity.permissions.canRead === true;
        const operationCandidates = (
            clicked && !context.selectedComponents.some((selected) => selected.entity.id === clicked.entity.id)
                ? [clicked]
                : context.selectedComponents
        ) as SelectionTarget[];

        return [
            {
                text: 'Start',
                clazz: 'fa fa-play',
                condition: () => this.inTroubleshooting() && areAnyRunnable(operationCandidates),
                action: () => this.commands.startComponents(operationCandidates)
            },
            {
                text: 'Stop',
                clazz: 'fa fa-stop',
                condition: () => this.inTroubleshooting() && areAnyStoppable(operationCandidates),
                action: () => this.commands.stopComponents(operationCandidates)
            },
            {
                text: 'Enable',
                clazz: 'fa fa-flash',
                condition: () => this.inTroubleshooting() && areAllEnableable(operationCandidates),
                action: () => this.commands.enableComponents(operationCandidates)
            },
            {
                text: 'Disable',
                clazz: 'icon icon-enable-false',
                condition: () => this.inTroubleshooting() && areAllDisableable(operationCandidates),
                action: () => this.commands.disableComponents(operationCandidates)
            },
            {
                text: 'Run Once',
                clazz: 'fa fa-caret-right',
                condition: () => this.inTroubleshooting() && canRunOnce(operationCandidates),
                action: () => this.commands.runOnce(operationCandidates)
            },
            {
                text: 'Terminate',
                clazz: 'fa fa-hourglass-end',
                condition: () => this.inTroubleshooting() && canTerminate(operationCandidates),
                action: () => this.commands.terminateThreads(operationCandidates)
            },
            {
                text: 'Advanced',
                clazz: 'fa fa-cog',
                condition: () =>
                    this.inTroubleshooting() &&
                    !!clicked &&
                    isProcessorDatum(clicked) &&
                    isSingleSelection &&
                    clicked.entity.permissions.canRead === true &&
                    clicked.entity.component?.extensionMissing !== true &&
                    !!clicked.entity.component?.config?.customUiUrl,
                action: () => this.commands.openAdvancedProcessor(clicked!.entity.id)
            },
            { isSeparator: true },
            {
                text: 'View Configuration',
                clazz: 'fa fa-gear',
                condition: () => isViewConfigurationTarget,
                action: () => this.viewConfigurationAction(clicked!.entity, clicked!.ui.componentType)
            },
            {
                text: 'Enter Group',
                clazz: 'fa fa-sign-in',
                condition: () => isProcessGroup && isSingleSelection,
                action: () => this.enterGroupAction(clicked!.entity.id)
            },
            { isSeparator: true },
            {
                text: 'View Data Provenance',
                clazz: 'icon icon-provenance',
                condition: () => isProvenanceTarget && this.canAccessProvenance(),
                action: () => this.viewDataProvenanceAction(clicked!.entity.id, clicked!.ui.componentType)
            },
            {
                text: 'View State',
                clazz: 'fa fa-tasks',
                condition: () =>
                    clicked != null &&
                    isProcessorDatum(clicked) &&
                    isSingleSelection &&
                    clicked.entity.component?.persistsState === true &&
                    clicked.entity.permissions.canRead === true &&
                    clicked.entity.permissions.canWrite === true,
                action: () => this.viewProcessorStateAction(clicked!.entity)
            },
            { isSeparator: true },
            {
                text: 'List Queue',
                clazz: 'fa fa-list',
                condition: () => isConnection && isSingleSelection,
                action: () => this.listQueueAction(clicked!.entity.id)
            },
            {
                text: 'Empty Queue',
                clazz: 'fa fa-minus-circle',
                condition: () => isConnection && isSingleSelection,
                action: () => this.emptyQueueAction(clicked!.entity.id)
            },
            {
                text: 'Empty All Queues',
                clazz: 'fa fa-minus-circle',
                condition: () => isProcessGroup && isSingleSelection,
                action: () => this.emptyAllQueuesAction(clicked!.entity.id)
            },
            {
                text: 'Controller Services',
                clazz: 'fa fa-list',
                condition: () => isProcessGroup && isSingleSelection,
                action: () => this.controllerServicesAction(clicked!.entity.id)
            },
            {
                text: 'Download Flow Definition',
                clazz: 'fa',
                subMenuId: 'download',
                condition: () => this.canDownloadFlow()
            },
            { isSeparator: true },
            {
                text: 'Center In View',
                clazz: 'fa fa-crosshairs',
                condition: () => true,
                action: () => this.centerInViewAction()
            },
            { isSeparator: true },
            {
                text: 'Delete',
                clazz: 'fa fa-trash',
                condition: () => this.canEdit() && this.hasDeletableSelection(operationCandidates),
                action: () => this.commands.delete(operationCandidates),
                shortcut: { code: 'DEL' }
            }
        ];
    }

    // =========================================================================
    // Actions shared by the context menu, keyboard shortcuts, and the page
    // =========================================================================

    refreshAction(): void {
        const connectorId = this.connectorId();
        const processGroupId = this.processGroupId();
        if (connectorId && processGroupId) {
            this.store.dispatch(ConnectorCanvasActions.loadConnectorFlow({ connectorId, processGroupId }));
        }
    }

    leaveGroupAction(): void {
        if (this.canNavigateToParent()) {
            this.store.dispatch(ConnectorCanvasActions.leaveProcessGroup());
        }
    }

    enterGroupAction(processGroupId: string): void {
        this.store.dispatch(ConnectorCanvasActions.enterProcessGroup({ request: { id: processGroupId } }));
    }

    viewConfigurationAction(entity: any, componentType: ComponentType): void {
        this.store.dispatch(
            ConnectorCanvasActions.viewComponentConfiguration({
                request: { entity, componentType }
            })
        );
    }

    viewDataProvenanceAction(componentId: string, componentType: ComponentType): void {
        this.store.dispatch(
            ConnectorCanvasActions.navigateToProvenanceForComponent({ id: componentId, componentType })
        );
    }

    viewProcessorStateAction(processorEntity: any): void {
        this.store.dispatch(
            getComponentStateAndOpenDialog({
                request: {
                    componentName: processorEntity.component.name,
                    componentId: processorEntity.id,
                    componentType: ComponentType.Processor,
                    canClear: this.canClearProcessorState(processorEntity),
                    connectorId: this.connectorId()
                }
            })
        );
    }

    centerInViewAction(): void {
        this.host?.centerOnSelection();
    }

    controllerServicesAction(processGroupId: string): void {
        this.store.dispatch(ConnectorCanvasActions.navigateToControllerServices({ processGroupId }));
    }

    downloadFlowAction(includeReferencedServices: boolean): void {
        const connectorId = this.connectorId();
        const processGroupId = this.getDownloadProcessGroupId();
        if (connectorId && processGroupId) {
            this.store.dispatch(
                ConnectorCanvasActions.downloadFlow({
                    request: {
                        connectorId,
                        processGroupId,
                        includeReferencedServices
                    }
                })
            );
        }
    }

    canDrain(): boolean {
        const entity = this.connectorEntity();
        if (!entity || this.entitySaving()) {
            return false;
        }
        return canOperateConnector(entity) && isConnectorActionAllowed(entity, 'DRAIN_FLOWFILES');
    }

    canCancelDrain(): boolean {
        const entity = this.connectorEntity();
        if (!entity || this.entitySaving()) {
            return false;
        }
        return canOperateConnector(entity) && isConnectorActionAllowed(entity, 'CANCEL_DRAIN_FLOWFILES');
    }

    drainConnectorAction(): void {
        const entity = this.connectorEntity();
        if (entity) {
            this.store.dispatch(ConnectorCanvasEntityActions.promptDrainConnector({ connector: entity }));
        }
    }

    cancelDrainConnectorAction(): void {
        const entity = this.connectorEntity();
        if (entity) {
            this.store.dispatch(ConnectorCanvasEntityActions.cancelConnectorDrain({ connector: entity }));
        }
    }

    listQueueAction(connectionId: string): void {
        this.store.dispatch(ConnectorCanvasActions.navigateToQueueListing({ request: { connectionId } }));
    }

    emptyQueueAction(connectionId: string): void {
        this.store.dispatch(
            EmptyQueueActions.promptEmptyQueueRequest({
                request: { connectionId, source: 'connector-canvas' }
            })
        );
    }

    emptyAllQueuesAction(processGroupId: string): void {
        this.store.dispatch(
            EmptyQueueActions.promptEmptyQueuesRequest({
                request: { processGroupId, source: 'connector-canvas' }
            })
        );
    }

    private connectorId(): string {
        return this.host?.connectorId() ?? '';
    }

    private processGroupId(): string | null {
        return this.host?.processGroupId() ?? null;
    }

    private canNavigateToParent(): boolean {
        return this.host?.canNavigateToParent() ?? false;
    }

    private canClearProcessorState(processorEntity: any): boolean {
        const runStatus = processorEntity.status?.aggregateSnapshot?.runStatus;
        const activeThreadCount = processorEntity.status?.aggregateSnapshot?.activeThreadCount || 0;
        return runStatus !== 'Running' && activeThreadCount === 0;
    }

    private canDownloadFlow(): boolean {
        return this.connectorEntity()?.permissions.canRead === true && this.getDownloadProcessGroupId() !== null;
    }

    private getDownloadProcessGroupId(): string | null {
        const context = this.currentContext;
        if (context?.targetType === 'canvas') {
            return this.processGroupId();
        }

        if (context?.targetType === 'component') {
            const clicked = context.clickedComponent;
            if (context.selectedComponents.length <= 1 && clicked?.ui.componentType === ComponentType.ProcessGroup) {
                return clicked.entity.id;
            }
        }

        return null;
    }
}
