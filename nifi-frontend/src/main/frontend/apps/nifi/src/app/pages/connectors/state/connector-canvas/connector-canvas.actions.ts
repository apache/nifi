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

import { createAction, props } from '@ngrx/store';
import {
    ComponentType,
    ConnectionEntity,
    FunnelEntity,
    LabelEntity,
    PortEntity,
    ProcessGroupEntity,
    ProcessorEntity,
    RemoteProcessGroupEntity,
    Position,
    RevisionRequest
} from '@nifi/shared';
import {
    BreadcrumbEntity,
    EditConnectionDialogRequest,
    ParameterContextEntity,
    UpdateConnectionRequest
} from '../../../../state/shared';
import { ErrorContext, ErrorContextKey } from '../../../../state/error';
import type { ConnectorCanvasComponentEntity } from './index';
import { ConnectionEndpointReconnectDestination } from '../../../../state/flow-shared';
import {
    CreateConnection,
    CreateConnectionDialogRequest,
    CreateConnectionRequest
} from '../../../../state/flow-shared';

/**
 * Selected component for route-based selection
 */
export interface SelectedComponent {
    id: string;
    componentType: ComponentType;
}

/**
 * Request for selecting components
 */
export interface SelectComponentsRequest {
    components: SelectedComponent[];
}

export interface DownloadConnectorFlowRequest {
    connectorId: string;
    processGroupId: string;
    includeReferencedServices: boolean;
}

export const loadConnectorFlow = createAction(
    '[Connector Canvas] Load Connector Flow',
    props<{ connectorId: string; processGroupId: string }>()
);

export const markAuthoritativeSnapshotPending = createAction('[Connector Canvas] Mark Authoritative Snapshot Pending');

export const loadConnectorFlowSuccess = createAction(
    '[Connector Canvas] Load Connector Flow Success',
    props<{
        connectorId: string;
        processGroupId: string | null;
        parentProcessGroupId: string | null;
        breadcrumb: BreadcrumbEntity | null;
        labels: LabelEntity[];
        funnels: FunnelEntity[];
        inputPorts: PortEntity[];
        outputPorts: PortEntity[];
        remoteProcessGroups: RemoteProcessGroupEntity[];
        processGroups: ProcessGroupEntity[];
        processors: ProcessorEntity[];
        connections: ConnectionEntity[];
        reconciliationSnapshot: {
            addedCache: string[];
            removedCache: string[];
        };
        connectedStateChanged: boolean;
    }>()
);

export const loadConnectorFlowFailure = createAction(
    '[Connector Canvas] Load Connector Flow Failure',
    props<{ errorContext: ErrorContext; isRefresh: boolean; connectedStateChanged: boolean }>()
);

export const loadConnectorFlowComplete = createAction('[Connector Canvas] Load Connector Flow Complete');

/**
 * Reload the currently displayed connector flow.
 *
 * The polling effect dispatches this action; the corresponding effect throttles
 * and resolves the connector and process group identifiers from state before
 * delegating to {@link loadConnectorFlow}. Keeping the indirection makes the
 * polling source independent of which connector and process group are currently
 * mounted, mirroring the flow-designer's reloadFlow pattern.
 */
export const reloadConnectorFlow = createAction('[Connector Canvas] Reload Connector Flow');

/**
 * Begin periodic refresh of the connector flow on the currently mounted canvas.
 *
 * The component dispatches this action in ngOnInit; the polling effect drives a
 * 30 second interval, gates on document visibility and on the absence of an
 * in-flight load, and dispatches {@link reloadConnectorFlow}.
 */
export const startConnectorCanvasPolling = createAction('[Connector Canvas] Start Connector Canvas Polling');

/**
 * Stop the periodic refresh started by {@link startConnectorCanvasPolling}.
 *
 * Dispatched by the component in ngOnDestroy (and may be dispatched explicitly
 * by other effects that need to suppress polling around a destructive operation).
 */
export const stopConnectorCanvasPolling = createAction('[Connector Canvas] Stop Connector Canvas Polling');

/**
 * Load the parameter context bound to the current process group within the connector.
 *
 * `errorContext` identifies which page-scoped error banner should surface a failure.
 * The triggering effect (loadConnectorParameterContextOnLoadSuccess$) sets this based
 * on whether the load was kicked off by the canvas or the controller-services page,
 * so a failure renders on the banner the user is actually looking at.
 */
export const loadConnectorParameterContext = createAction(
    '[Connector Canvas] Load Connector Parameter Context',
    props<{ connectorId: string; processGroupId: string; errorContext: ErrorContextKey }>()
);

export const loadConnectorParameterContextSuccess = createAction(
    '[Connector Canvas] Load Connector Parameter Context Success',
    props<{ parameterContext: ParameterContextEntity | null }>()
);

export const loadConnectorParameterContextFailure = createAction(
    '[Connector Canvas] Load Connector Parameter Context Failure',
    props<{ errorContext: ErrorContext }>()
);

export const enterProcessGroup = createAction(
    '[Connector Canvas] Enter Process Group',
    props<{ request: { id: string } }>()
);

export const leaveProcessGroup = createAction('[Connector Canvas] Leave Process Group');

/**
 * Selection actions (trigger route updates)
 */
export const selectComponents = createAction(
    '[Connector Canvas] Select Components',
    props<{ request: SelectComponentsRequest }>()
);

export const deselectAllComponents = createAction('[Connector Canvas] Deselect All Components');

/**
 * Navigation action (internal - updates router)
 */
export const navigateWithoutTransform = createAction(
    '[Connector Canvas] Navigate Without Transform',
    props<{ url: string[] }>()
);

/**
 * skipTransform is used when handling URL events for component selection. Since the
 * URL is the source of truth, we set skipTransform when the URL changes due to user
 * selection on the canvas. However, we do not want the transform skipped when using
 * a deep link or leaving a process group -- in those cases the viewport should center
 * on the selected component(s).
 */
export const setSkipTransform = createAction(
    '[Connector Canvas] Set Skip Transform',
    props<{ skipTransform: boolean }>()
);

export const navigateToProvenanceForComponent = createAction(
    '[Connector Canvas] Navigate To Provenance For Component',
    props<{ id: string; componentType: ComponentType }>()
);

/**
 * Navigate to the controller services view for a process group within a connector.
 * The target process group must be supplied by the caller; the canvas-background
 * caller passes the current route's process group, and component-menu callers pass
 * the id of the right-clicked process group.
 */
export const navigateToControllerServices = createAction(
    '[Connector Canvas] Navigate To Controller Services',
    props<{ processGroupId: string }>()
);

/**
 * Navigate to a specific controller service within a process group, deep linking
 * to the controller services view with the service pre-selected.
 */
export const navigateToControllerService = createAction(
    '[Connector Canvas] Navigate To Controller Service',
    props<{ processGroupId: string; serviceId: string }>()
);

/**
 * Navigate to the queue listing page for a connection.
 * Uses back navigation to return to the connector canvas.
 */
export const navigateToQueueListing = createAction(
    '[Connector Canvas] Navigate To Queue Listing',
    props<{ request: { connectionId: string } }>()
);

/**
 * View the read-only configuration of a canvas component.
 * Triggers an effect that opens the appropriate EditXyz dialog in read-only mode.
 */
export const viewComponentConfiguration = createAction(
    '[Connector Canvas] View Component Configuration',
    props<{ request: { entity: ConnectorCanvasComponentEntity; componentType: ComponentType } }>()
);

export interface BulkPositionUpdate {
    componentId: string;
    componentType: ComponentType;
    revision: RevisionRequest;
    position?: Position;
    bends?: Position[];
    labelIndex?: number;
}

export interface BulkPositionResult {
    componentId: string;
    componentType: ComponentType;
    success: boolean;
    component?: ConnectorCanvasComponentEntity;
    error?: string;
}

export interface MoveCanvasComponentRef {
    id: string;
    type: ComponentType;
    revision: RevisionRequest;
}

export const createConnectionRequested = createAction(
    '[Connector Canvas] Create Connection Requested',
    props<{ processGroupId: string; request: CreateConnectionRequest }>()
);

export const openCreateConnectionDialog = createAction(
    '[Connector Canvas] Open Create Connection Dialog',
    props<{ request: CreateConnectionDialogRequest }>()
);

export const submitCreateConnection = createAction(
    '[Connector Canvas] Submit Create Connection',
    props<{ processGroupId: string; request: CreateConnection }>()
);

export const createConnectionSuccess = createAction(
    '[Connector Canvas] Create Connection Success',
    props<{ connection: ConnectionEntity }>()
);

export const createConnectionFailure = createAction(
    '[Connector Canvas] Create Connection Failure',
    props<{ error: string }>()
);

export const createConnectionDialogClosed = createAction('[Connector Canvas] Create Connection Dialog Closed');

export const updateComponentPositionsBulk = createAction(
    '[Connector Canvas] Update Component Positions Bulk',
    props<{ processGroupId: string; updates: BulkPositionUpdate[]; delta: Position }>()
);

export const updateComponentPositionSuccess = createAction(
    '[Connector Canvas] Update Component Position Success',
    props<{
        componentId: string;
        componentType: ComponentType;
        component: ConnectorCanvasComponentEntity;
    }>()
);

export const updateComponentPositionFailure = createAction(
    '[Connector Canvas] Update Component Position Failure',
    props<{ componentId: string; componentType: ComponentType; error: string }>()
);

export const updateComponentPositionsBulkComplete = createAction(
    '[Connector Canvas] Update Component Positions Bulk Complete',
    props<{ successes: BulkPositionResult[]; failures: BulkPositionResult[] }>()
);

export const updateConnectionBendPoints = createAction(
    '[Connector Canvas] Update Connection Bend Points',
    props<{
        processGroupId: string;
        update: {
            id: string;
            bends: Position[];
            labelIndex?: number;
            revision: RevisionRequest;
        };
    }>()
);

export const updateConnectionBendPointsSuccess = createAction(
    '[Connector Canvas] Update Connection Bend Points Success',
    props<{ connectionId: string; connection: ConnectionEntity }>()
);

export const updateConnectionBendPointsFailure = createAction(
    '[Connector Canvas] Update Connection Bend Points Failure',
    props<{ connectionId: string; error: string }>()
);

export const updateConnectionLabelIndex = createAction(
    '[Connector Canvas] Update Connection Label Index',
    props<{
        processGroupId: string;
        connectionId: string;
        labelIndex: number;
        revision: RevisionRequest;
    }>()
);

export const updateConnectionLabelIndexSuccess = createAction(
    '[Connector Canvas] Update Connection Label Index Success',
    props<{ connectionId: string; connection: ConnectionEntity }>()
);

export const updateConnectionLabelIndexFailure = createAction(
    '[Connector Canvas] Update Connection Label Index Failure',
    props<{ connectionId: string; error: string }>()
);

export const updateConnectionDestination = createAction(
    '[Connector Canvas] Update Connection Destination',
    props<{
        processGroupId: string;
        connectionId: string;
        revision: RevisionRequest;
        newDestination: ConnectionEndpointReconnectDestination;
        bends?: Position[];
    }>()
);

export const updateConnectionDestinationSuccess = createAction(
    '[Connector Canvas] Update Connection Destination Success',
    props<{ connectionId: string; connection: ConnectionEntity }>()
);

export const updateConnectionDestinationFailure = createAction(
    '[Connector Canvas] Update Connection Destination Failure',
    props<{
        connectionId: string;
        error: string;
        /**
         * True when the failure came from a save submitted through the reconnect
         * dialog, which stays open for retry. The canvas keeps the optimistic
         * endpoint until that dialog closes.
         */
        dialogOpen?: boolean;
    }>()
);

export const openReconnectConnectionDialog = createAction(
    '[Connector Canvas] Open Reconnect Connection Dialog',
    props<{ request: EditConnectionDialogRequest }>()
);

export const saveEditedConnection = createAction(
    '[Connector Canvas] Save Edited Connection',
    props<{ request: UpdateConnectionRequest }>()
);

export const editConnectionDialogClosed = createAction(
    '[Connector Canvas] Edit Connection Dialog Closed',
    props<{ connectionId: string; cancelled: boolean }>()
);

export const resizeLabel = createAction(
    '[Connector Canvas] Resize Label',
    props<{
        processGroupId: string;
        labelId: string;
        dimensions: { width: number; height: number };
        revision: RevisionRequest;
    }>()
);

export const resizeLabelSuccess = createAction(
    '[Connector Canvas] Resize Label Success',
    props<{ labelId: string; label: LabelEntity }>()
);

export const resizeLabelFailure = createAction(
    '[Connector Canvas] Resize Label Failure',
    props<{ labelId: string; error: string }>()
);

export const moveCanvasComponents = createAction(
    '[Connector Canvas] Move Canvas Components',
    props<{ processGroupId: string; targetGroupId: string; components: MoveCanvasComponentRef[] }>()
);

export const moveCanvasComponentsSuccess = createAction(
    '[Connector Canvas] Move Canvas Components Success',
    props<{ targetGroupId: string; components: MoveCanvasComponentRef[] }>()
);

export const moveCanvasComponentsFailure = createAction(
    '[Connector Canvas] Move Canvas Components Failure',
    props<{ components: MoveCanvasComponentRef[]; error: string }>()
);

export const downloadFlow = createAction(
    '[Connector Canvas] Download Flow',
    props<{ request: DownloadConnectorFlowRequest }>()
);

export const resetConnectorCanvasState = createAction('[Connector Canvas] Reset State');
