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

import { ComponentRef, Type } from '@angular/core';
import { Overlay, OverlayRef, PositionStrategy } from '@angular/cdk/overlay';
import { ComponentPortal } from '@angular/cdk/portal';
import * as d3 from 'd3';
import { NiFiCommon } from '@nifi/shared';

export interface CanvasTooltipSession {
    type: Type<unknown>;
    data: unknown;
    overlayRef: OverlayRef | null;
    positionStrategy: PositionStrategy | null;
    tooltipReference: ComponentRef<unknown> | null;
    closeTimer: number;
    openTimer: number;
}

export type CanvasTooltipSessionMap = WeakMap<Element, CanvasTooltipSession>;

const TOOLTIP_POSITIONS = [
    {
        originX: 'end' as const,
        originY: 'bottom' as const,
        overlayX: 'start' as const,
        overlayY: 'top' as const,
        offsetX: 8,
        offsetY: 8
    }
];

function clearOpenTimer(session: CanvasTooltipSession): void {
    if (session.openTimer > 0) {
        window.clearTimeout(session.openTimer);
        session.openTimer = -1;
    }
}

function clearCloseTimer(session: CanvasTooltipSession): void {
    if (session.closeTimer > 0) {
        window.clearTimeout(session.closeTimer);
        session.closeTimer = -1;
    }
}

function disposeSession(session: CanvasTooltipSession): void {
    clearOpenTimer(session);
    clearCloseTimer(session);
    session.overlayRef?.detach();
    session.overlayRef?.dispose();
    session.overlayRef = null;
    session.tooltipReference = null;
    session.positionStrategy?.detach?.();
    session.positionStrategy?.dispose();
    session.positionStrategy = null;
}

function refreshOpenTooltip(session: CanvasTooltipSession): void {
    if (session.overlayRef?.hasAttached() && session.tooltipReference) {
        session.tooltipReference.setInput('data', session.data);
        session.tooltipReference.changeDetectorRef.detectChanges();
        session.overlayRef.updatePosition();
    }
}

function openTooltip(overlay: Overlay, element: Element, session: CanvasTooltipSession): void {
    session.positionStrategy = overlay
        .position()
        .flexibleConnectedTo(element)
        .withPositions(TOOLTIP_POSITIONS)
        .withPush(true);
    session.overlayRef = overlay.create({ positionStrategy: session.positionStrategy });
    session.tooltipReference = session.overlayRef.attach(new ComponentPortal(session.type));
    session.tooltipReference.setInput('data', session.data);
    session.tooltipReference.changeDetectorRef.detectChanges();

    const tooltipElement: HTMLElement = session.tooltipReference.location.nativeElement;
    tooltipElement.addEventListener('mouseenter', () => clearCloseTimer(session));
    tooltipElement.addEventListener('mouseleave', () => disposeSession(session));
}

/**
 * Binds hover listeners that open a canvas overlay tooltip. If the tooltip is already open,
 * the next bind (for example from a poll) updates its `data` input in place. A tooltip that is
 * waiting to open uses the data from the latest bind.
 */
export function bindCanvasTooltip<C>(
    overlay: Overlay,
    sessions: CanvasTooltipSessionMap,
    type: Type<C>,
    selection: d3.Selection<any, any, any, any>,
    tooltipData: unknown
): void {
    selection.each((_d, i, nodes) => {
        const element = nodes[i];
        if (!(element instanceof Element)) {
            return;
        }
        let session = sessions.get(element);
        if (!session) {
            session = {
                type,
                data: tooltipData,
                overlayRef: null,
                positionStrategy: null,
                tooltipReference: null,
                closeTimer: -1,
                openTimer: -1
            };
            sessions.set(element, session);
            return;
        }
        if (session.type !== type) {
            disposeSession(session);
            session.type = type;
        }
        session.data = tooltipData;
        refreshOpenTooltip(session);
    });

    selection
        .on('mouseenter', (event: MouseEvent) => {
            const element = event.currentTarget as Element;
            const session = sessions.get(element);
            if (!session) {
                return;
            }
            clearCloseTimer(session);
            if (session.overlayRef || session.openTimer > 0) {
                return;
            }
            session.openTimer = window.setTimeout(() => {
                session.openTimer = -1;
                openTooltip(overlay, element, session);
            }, NiFiCommon.TOOLTIP_DELAY_OPEN_MILLIS);
        })
        .on('mouseleave', (event: MouseEvent) => {
            const session = sessions.get(event.currentTarget as Element);
            if (!session) {
                return;
            }
            clearOpenTimer(session);
            clearCloseTimer(session);
            session.closeTimer = window.setTimeout(() => {
                disposeSession(session);
            }, NiFiCommon.TOOLTIP_DELAY_OPEN_MILLIS);
        });
}

/**
 * Removes hover listeners and closes any open tooltip for the selection.
 */
export function unbindCanvasTooltip(
    sessions: CanvasTooltipSessionMap,
    selection: d3.Selection<any, any, any, any>
): void {
    selection.each((_d, i, nodes) => {
        const element = nodes[i];
        if (!(element instanceof Element)) {
            return;
        }
        const session = sessions.get(element);
        if (session) {
            disposeSession(session);
            sessions.delete(element);
        }
    });
    selection.on('mouseenter', null).on('mouseleave', null);
}
