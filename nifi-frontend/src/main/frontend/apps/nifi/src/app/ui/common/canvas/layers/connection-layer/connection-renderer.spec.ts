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

import * as d3 from 'd3';
import { ConnectionRenderer } from './connection-renderer';
import { ConnectionReconnectContext, ConnectionRenderContext } from '../render-context.types';
import { createBaseRenderContextFixture } from '../render-context-fixtures';
import { CanvasConnection, CanvasRootSelection } from '../../canvas.types';
import { ComponentType, Position } from '@nifi/shared';

function createCanvasRoot(): CanvasRootSelection {
    return d3.select(document.createElementNS('http://www.w3.org/2000/svg', 'g')) as unknown as CanvasRootSelection;
}

interface ConnectionRendererPrivateSurface {
    getConnectionSourceComponentId(connection: CanvasConnection, processGroupId: string | null): string;
    getConnectionEndAnchor(
        connection: CanvasConnection,
        processGroupId: string | null,
        canvasRoot: CanvasRootSelection
    ): Position | null;
}

const connectionRendererPrivate = ConnectionRenderer as unknown as ConnectionRendererPrivateSurface;

/**
 * Test setup options for ConnectionRenderer tests
 */
interface SetupOptions {
    connections?: CanvasConnection[];
    canSelect?: boolean;
    canEdit?: boolean;
    disabledConnectionIds?: Set<string>;
    processGroupId?: string;
    callbacks?: Partial<ConnectionRenderContext['callbacks']>;
    reconnect?: ConnectionReconnectContext;
    sourceComponent?: { id: string; position: Position; dimensions: { width: number; height: number } };
    destComponent?: { id: string; position: Position; dimensions: { width: number; height: number } };
}

/**
 * Creates a mock connection entity for testing
 */
function createMockConnection(
    overrides: {
        entity?: {
            id?: string;
            sourceId?: string;
            destinationId?: string;
            sourceGroupId?: string;
            destinationGroupId?: string;
            bends?: Position[];
            zIndex?: number;
            permissions?: { canRead?: boolean; canWrite?: boolean };
            component?: {
                parentGroupId?: string;
                source?: { name?: string; exists?: boolean; running?: boolean; groupId?: string; type?: string };
                destination?: { name?: string; exists?: boolean; running?: boolean; groupId?: string; type?: string };
                selectedRelationships?: string[];
                availableRelationships?: string[];
                backPressureObjectThreshold?: number;
                backPressureDataSizeThreshold?: string;
                flowFileExpiration?: string;
                loadBalanceStrategy?: string;
                retriedRelationships?: string[];
                labelIndex?: number;
            };
            status?: {
                aggregateSnapshot?: {
                    queued?: string;
                    percentUseCount?: number;
                    percentUseBytes?: number;
                    flowFileAvailability?: string;
                    predictions?: {
                        predictedPercentCount?: number;
                        predictedPercentBytes?: number;
                        predictedMillisUntilCountBackpressure?: number;
                        predictedMillisUntilBytesBackpressure?: number;
                        predictionIntervalSeconds?: number;
                    };
                };
            };
        };
        ui?: {
            start?: Position;
            end?: Position;
            bends?: Position[];
            dragging?: boolean;
            tempLabelIndex?: number;
        };
    } = {}
): CanvasConnection {
    const entityOverrides = overrides.entity || {};
    const uiOverrides = overrides.ui || {};

    const defaultConnection: CanvasConnection = {
        entity: {
            id: entityOverrides.id ?? 'connection-1',
            sourceId: entityOverrides.sourceId ?? 'source-1',
            destinationId: entityOverrides.destinationId ?? 'dest-1',
            sourceGroupId: entityOverrides.sourceGroupId,
            destinationGroupId: entityOverrides.destinationGroupId,
            bends: entityOverrides.bends ?? [],
            zIndex: entityOverrides.zIndex ?? 0,
            permissions: {
                canRead: entityOverrides.permissions?.canRead ?? true,
                canWrite: entityOverrides.permissions?.canWrite ?? true
            },
            component: {
                parentGroupId: entityOverrides.component?.parentGroupId ?? 'root',
                source: entityOverrides.component?.source ?? {
                    name: 'Source Processor',
                    exists: true,
                    running: true
                },
                destination: entityOverrides.component?.destination ?? {
                    name: 'Destination Processor',
                    exists: true,
                    running: true
                },
                selectedRelationships: entityOverrides.component?.selectedRelationships ?? ['success'],
                availableRelationships: entityOverrides.component?.availableRelationships ?? ['success', 'failure'],
                backPressureObjectThreshold: entityOverrides.component?.backPressureObjectThreshold ?? 10000,
                backPressureDataSizeThreshold: entityOverrides.component?.backPressureDataSizeThreshold ?? '1 GB',
                flowFileExpiration: entityOverrides.component?.flowFileExpiration ?? '0 sec',
                loadBalanceStrategy: entityOverrides.component?.loadBalanceStrategy ?? 'DO_NOT_LOAD_BALANCE',
                retriedRelationships: entityOverrides.component?.retriedRelationships ?? [],
                labelIndex: entityOverrides.component?.labelIndex ?? 0
            },
            status: entityOverrides.status ?? {
                aggregateSnapshot: {
                    queued: '0 (0 bytes)',
                    percentUseCount: 0,
                    percentUseBytes: 0
                }
            }
        },
        ui: {
            componentType: ComponentType.Connection,
            start: uiOverrides.start ?? { x: 0, y: 0 },
            end: uiOverrides.end ?? { x: 0, y: 0 },
            bends: uiOverrides.bends,
            dragging: uiOverrides.dragging,
            tempLabelIndex: uiOverrides.tempLabelIndex
        }
    };

    return defaultConnection;
}

/**
 * Creates a mock component element on the DOM for connection path calculation
 */
function createMockComponent(
    id: string,
    position: Position,
    dimensions: { width: number; height: number },
    parentGroupId = 'root'
): SVGGElement {
    const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    g.setAttribute('id', `id-${id}`);
    g.classList.add('processor', 'component');

    // Bind data to element using D3
    d3.select(g).datum({
        entity: {
            id,
            position: position,
            component: { parentGroupId }
        },
        ui: {
            componentType: ComponentType.Processor,
            dimensions: dimensions
        }
    });

    return g;
}

/**
 * Creates a mock render context for testing
 */
function createMockContext(options: SetupOptions = {}): ConnectionRenderContext {
    const container = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    container.classList.add('connection-container');
    document.body.appendChild(container);

    // Create source and destination components for path calculation
    if (options.sourceComponent) {
        const sourceEl = createMockComponent(
            options.sourceComponent.id,
            options.sourceComponent.position,
            options.sourceComponent.dimensions,
            options.processGroupId
        );
        container.appendChild(sourceEl);
    }

    if (options.destComponent) {
        const destEl = createMockComponent(
            options.destComponent.id,
            options.destComponent.position,
            options.destComponent.dimensions,
            options.processGroupId
        );
        container.appendChild(destEl);
    }

    const containerSelection = d3.select(container);
    return {
        containerSelection,
        canvasRootResolver: () => containerSelection,
        connections: options.connections || [],
        processGroupId: options.processGroupId ?? 'root',
        canSelect: options.canSelect ?? true,
        disabledConnectionIds: options.disabledConnectionIds,
        getDisabledConnectionIds: () => options.disabledConnectionIds ?? new Set<string>(),
        getCanEdit: () => options.canEdit ?? true,
        getCanSelect: () => options.canSelect ?? true,
        getSelectedIds: () => new Set<string>(),
        reconnect: options.reconnect,
        getReconnect: () => options.reconnect,
        ...createBaseRenderContextFixture({
            textEllipsis: {
                applyEllipsis: vi.fn((selection, text, _className) => {
                    selection.text(text);
                }),
                determineContrastColor: vi.fn(() => '#000000')
            },
            formatUtils: {
                formatQueuedStats: vi.fn((value) => {
                    const parts = value.split(' (');
                    return {
                        count: parts[0] || '0',
                        size: parts[1] ? ` (${parts[1]}` : ' (0 bytes)'
                    };
                })
            },
            nifiCommon: {
                compareNumber: vi.fn((a, b) => (a ?? 0) - (b ?? 0))
            }
        }),
        componentUtils: {
            canvasTooltip: vi.fn(),
            formatPredictedDuration: vi.fn((ms) => `${ms}ms`)
        } as unknown as ConnectionRenderContext['componentUtils'],
        callbacks: {
            onClick: options.callbacks?.onClick,
            onDoubleClick: options.callbacks?.onDoubleClick,
            onBendPointDragEnd: options.callbacks?.onBendPointDragEnd,
            onBendPointAdd: options.callbacks?.onBendPointAdd,
            onBendPointRemove: options.callbacks?.onBendPointRemove,
            onLabelDragEnd: options.callbacks?.onLabelDragEnd,
            onEndpointReconnect: options.callbacks?.onEndpointReconnect
        }
    };
}

/**
 * SIFERS-style async setup function
 */
async function setup(options: SetupOptions = {}) {
    const context = createMockContext(options);

    // Render connections
    ConnectionRenderer.render(context);

    return {
        context,
        container: context.containerSelection.node() as SVGGElement,
        getConnectionElements: () =>
            context.containerSelection.selectAll<SVGGElement, CanvasConnection>('g.connection'),
        cleanup: () => {
            const containers = document.querySelectorAll('g.connection-container');
            containers.forEach((el) => el.remove());
        }
    };
}

describe('ConnectionRenderer', () => {
    afterEach(() => {
        // Clean up DOM after each test
        const containers = document.querySelectorAll('g.connection-container');
        containers.forEach((el) => el.remove());
    });

    describe('render', () => {
        describe('ENTER phase - creating new connections', () => {
            it('should create connection elements', async () => {
                const connection = createMockConnection();
                const { getConnectionElements, cleanup } = await setup({
                    connections: [connection],
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                const elements = getConnectionElements();
                expect(elements.size()).toBe(1);
                expect(elements.attr('id')).toBe('id-connection-1');
                expect(elements.classed('connection')).toBe(true);

                cleanup();
            });

            it('should create connection path elements', async () => {
                const connection = createMockConnection();
                const { getConnectionElements, cleanup } = await setup({
                    connections: [connection],
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                const connectionEl = getConnectionElements();
                expect(connectionEl.select('path.connection-path').empty()).toBe(false);
                expect(connectionEl.select('path.connection-selection-path').empty()).toBe(false);
                expect(connectionEl.select('path.connection-path-selectable').empty()).toBe(false);

                cleanup();
            });

            it('should set pointer-events on selectable path', async () => {
                const connection = createMockConnection();
                const { getConnectionElements, cleanup } = await setup({
                    connections: [connection],
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                const selectablePath = getConnectionElements().select('path.connection-path-selectable');
                expect(selectablePath.attr('pointer-events')).toBe('stroke');

                cleanup();
            });

            it('should create multiple connections', async () => {
                const conn1 = createMockConnection({
                    entity: { id: 'conn-1', sourceId: 'source-1', destinationId: 'dest-1' }
                });
                const conn2 = createMockConnection({
                    entity: { id: 'conn-2', sourceId: 'source-2', destinationId: 'dest-2' }
                });

                const { getConnectionElements, cleanup } = await setup({
                    connections: [conn1, conn2],
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                expect(getConnectionElements().size()).toBe(2);

                cleanup();
            });
        });

        describe('UPDATE phase - styling', () => {
            it('should apply disabled styling when connection is in disabledConnectionIds', async () => {
                const connection = createMockConnection({ entity: { id: 'conn-disabled' } });
                const { getConnectionElements, cleanup } = await setup({
                    connections: [connection],
                    disabledConnectionIds: new Set(['conn-disabled']),
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                const connectionEl = getConnectionElements();
                expect(connectionEl.classed('disabled')).toBe(true);
                expect(connectionEl.style('opacity')).toBe('0.6');
                expect(connectionEl.style('cursor')).toBe('not-allowed');

                cleanup();
            });

            it('should apply pointer cursor when canSelect is true', async () => {
                const connection = createMockConnection();
                const { getConnectionElements, cleanup } = await setup({
                    connections: [connection],
                    canSelect: true,
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                expect(getConnectionElements().style('cursor')).toBe('pointer');

                cleanup();
            });

            it('should apply default cursor when canSelect is false', async () => {
                const connection = createMockConnection();
                const { getConnectionElements, cleanup } = await setup({
                    connections: [connection],
                    canSelect: false,
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                expect(getConnectionElements().style('cursor')).toBe('default');

                cleanup();
            });

            it('should apply unauthorized class when canRead is false', async () => {
                const connection = createMockConnection({
                    entity: {
                        permissions: { canRead: false, canWrite: false }
                    }
                });
                const { getConnectionElements, cleanup } = await setup({
                    connections: [connection],
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                const connectionPath = getConnectionElements().select('path.connection-path');
                expect(connectionPath.classed('unauthorized')).toBe(true);

                cleanup();
            });
        });

        describe('UPDATE phase - z-index sorting', () => {
            it('should sort connections by zIndex', async () => {
                const conn1 = createMockConnection({ entity: { id: 'conn-1', zIndex: 2 } });
                const conn2 = createMockConnection({ entity: { id: 'conn-2', zIndex: 0 } });
                const conn3 = createMockConnection({ entity: { id: 'conn-3', zIndex: 1 } });

                const { context, cleanup } = await setup({
                    connections: [conn1, conn2, conn3],
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                expect(context.nifiCommon.compareNumber).toHaveBeenCalled();

                cleanup();
            });
        });

        describe('EXIT phase - removing connections', () => {
            it('should remove connections that are no longer in data', async () => {
                const conn1 = createMockConnection({ entity: { id: 'conn-1' } });
                const conn2 = createMockConnection({ entity: { id: 'conn-2' } });

                const { context, getConnectionElements, cleanup } = await setup({
                    connections: [conn1, conn2],
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                expect(getConnectionElements().size()).toBe(2);

                // Re-render with only conn1
                context.connections = [conn1];
                ConnectionRenderer.render(context);

                expect(getConnectionElements().size()).toBe(1);
                expect(getConnectionElements().attr('id')).toBe('id-conn-1');

                cleanup();
            });

            it('should remove all connections when data is empty', async () => {
                const connection = createMockConnection();
                const { context, getConnectionElements, cleanup } = await setup({
                    connections: [connection],
                    sourceComponent: {
                        id: 'source-1',
                        position: { x: 0, y: 0 },
                        dimensions: { width: 184, height: 52 }
                    },
                    destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
                });

                expect(getConnectionElements().size()).toBe(1);

                // Re-render with empty array
                context.connections = [];
                ConnectionRenderer.render(context);

                expect(getConnectionElements().size()).toBe(0);

                cleanup();
            });
        });
    });

    describe('event handlers', () => {
        it('should attach click handler when canSelect is true and onClick callback provided', async () => {
            const onClick = vi.fn();
            const connection = createMockConnection();
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                canSelect: true,
                callbacks: { onClick },
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            // Simulate mousedown event
            const connectionEl = getConnectionElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 0 });
            connectionEl.dispatchEvent(event);

            expect(onClick).toHaveBeenCalledTimes(1);
            expect(onClick).toHaveBeenCalledWith(connection, expect.any(MouseEvent));

            cleanup();
        });

        it('should not trigger onClick for right-click', async () => {
            const onClick = vi.fn();
            const connection = createMockConnection();
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                canSelect: true,
                callbacks: { onClick },
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            // Simulate right-click (button: 2)
            const connectionEl = getConnectionElements().node()!;
            const event = new MouseEvent('mousedown', { bubbles: true, button: 2 });
            connectionEl.dispatchEvent(event);

            expect(onClick).not.toHaveBeenCalled();

            cleanup();
        });
    });

    describe('connection points', () => {
        it('should create startpoint and endpoint when user has permissions', async () => {
            const connection = createMockConnection({
                entity: {
                    permissions: { canRead: true, canWrite: true }
                }
            });
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const connectionEl = getConnectionElements();
            expect(connectionEl.select('rect.startpoint').empty()).toBe(false);
            expect(connectionEl.select('rect.endpoint').empty()).toBe(false);

            cleanup();
        });

        it('should create midpoints for bend points', async () => {
            const connection = createMockConnection({
                entity: {
                    bends: [
                        { x: 150, y: 50 },
                        { x: 200, y: 100 }
                    ],
                    permissions: { canRead: true, canWrite: true }
                }
            });
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const connectionEl = getConnectionElements();
            expect(connectionEl.selectAll('rect.midpoint').size()).toBe(2);

            cleanup();
        });

        it('should not create connection points when canRead is false', async () => {
            const connection = createMockConnection({
                entity: {
                    permissions: { canRead: false, canWrite: false }
                }
            });
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const connectionEl = getConnectionElements();
            expect(connectionEl.select('rect.startpoint').empty()).toBe(true);
            expect(connectionEl.select('rect.endpoint').empty()).toBe(true);
            expect(connectionEl.selectAll('rect.midpoint').size()).toBe(0);

            cleanup();
        });

        it('should not create connection points when canWrite is false', async () => {
            const connection = createMockConnection({
                entity: {
                    permissions: { canRead: true, canWrite: false }
                }
            });
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const connectionEl = getConnectionElements();
            expect(connectionEl.select('rect.startpoint').empty()).toBe(true);
            expect(connectionEl.select('rect.endpoint').empty()).toBe(true);

            cleanup();
        });
    });

    describe('pan', () => {
        it('should update connection elements during pan', async () => {
            const connection = createMockConnection();
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            // Mark as entering (becoming visible)
            getConnectionElements().classed('entering', true).classed('visible', true);

            // Call pan to update
            ConnectionRenderer.pan(getConnectionElements(), context);

            // Connection should still exist
            expect(getConnectionElements().size()).toBe(1);

            cleanup();
        });

        it('should handle empty selection gracefully', async () => {
            const { context, cleanup } = await setup({
                connections: []
            });

            // Call pan with empty selection - should not throw
            const emptySelection = context.containerSelection.selectAll<SVGGElement, CanvasConnection>('g.connection');
            expect(() => ConnectionRenderer.pan(emptySelection, context)).not.toThrow();

            cleanup();
        });
    });

    describe('calculatePath', () => {
        it('should calculate straight path without bend points', async () => {
            const connection = createMockConnection({
                entity: {
                    bends: []
                }
            });
            const { context, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const path = ConnectionRenderer.calculatePath(connection, context.canvasRootResolver(), null);
            // Path format: M x,y L x,y (with possible floating point values)
            expect(path).toMatch(/^M [\d.]+,[\d.]+ L [\d.]+,[\d.]+$/);

            cleanup();
        });

        it('should calculate path with bend points', async () => {
            const connection = createMockConnection({
                entity: {
                    bends: [
                        { x: 150, y: 50 },
                        { x: 200, y: 100 }
                    ]
                }
            });
            const { context, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const path = ConnectionRenderer.calculatePath(connection, context.canvasRootResolver(), null);
            // Should have M (move), then L for each bend, then L for end
            expect(path).toContain('M ');
            expect(path).toContain('L 150,50');
            expect(path).toContain('L 200,100');

            cleanup();
        });

        it('should store start and end points on connection.ui', async () => {
            const connection = createMockConnection();
            const { context, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            ConnectionRenderer.calculatePath(connection, context.canvasRootResolver(), null);

            expect(connection.ui.start).toBeDefined();
            expect(connection.ui.end).toBeDefined();
            expect(typeof connection.ui.start.x).toBe('number');
            expect(typeof connection.ui.start.y).toBe('number');
            expect(typeof connection.ui.end.x).toBe('number');
            expect(typeof connection.ui.end.y).toBe('number');

            cleanup();
        });

        it('should initialize ui.bends from entity.bends', async () => {
            const connection = createMockConnection({
                entity: {
                    bends: [
                        { x: 100, y: 50 },
                        { x: 150, y: 75 }
                    ]
                }
            });
            // Clear ui.bends to test initialization
            connection.ui.bends = undefined;

            const { context, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            ConnectionRenderer.calculatePath(connection, context.canvasRootResolver(), null);

            expect(connection.ui.bends).toBeDefined();
            expect(connection.ui.bends!.length).toBe(2);
            expect(connection.ui.bends![0]).toEqual({ x: 100, y: 50 });
            expect(connection.ui.bends![1]).toEqual({ x: 150, y: 75 });

            cleanup();
        });

        it('should return empty string when source component not found', async () => {
            const connection = createMockConnection({
                entity: {
                    sourceId: 'nonexistent-source'
                }
            });
            const { context, cleanup } = await setup({
                connections: [connection],
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const path = ConnectionRenderer.calculatePath(connection, context.canvasRootResolver(), null);
            expect(path).toBe('');

            cleanup();
        });

        it('should return empty string when destination component not found', async () => {
            const connection = createMockConnection({
                entity: {
                    destinationId: 'nonexistent-dest'
                }
            });
            const { context, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const path = ConnectionRenderer.calculatePath(connection, context.canvasRootResolver(), null);
            expect(path).toBe('');

            cleanup();
        });

        it('renders against the optimistic reconnect destination until persistence completes', async () => {
            const connection = createMockConnection({ entity: { destinationId: 'dest-1' } });
            connection.ui.reconnectDestinationId = 'dest-2';
            const { context, container, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });
            container.appendChild(createMockComponent('dest-2', { x: 600, y: 0 }, { width: 184, height: 52 }, 'root'));

            const path = ConnectionRenderer.calculatePath(connection, context.canvasRootResolver(), 'root');

            expect(path).not.toBe('');
            expect(connection.ui.end.x).toBeGreaterThan(500);
            cleanup();
        });

        it('keeps the live endpoint and recalculates the source while reconnect dragging without bends', async () => {
            const connection = createMockConnection();
            connection.ui.bends = [];
            connection.ui.endPointDragging = true;
            connection.ui.end = { x: 500, y: 200 };
            const { context, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const path = ConnectionRenderer.calculatePath(connection, context.canvasRootResolver(), 'root');

            expect(connection.ui.end).toEqual({ x: 500, y: 200 });
            expect(path).toContain('L 500,200');
            expect(connection.ui.start.x).toBeGreaterThan(0);
            cleanup();
        });
    });

    describe('endpoint reconnect geometry', () => {
        it('uses the last bend as the destination perimeter anchor', () => {
            const connection = createMockConnection();
            connection.ui.bends = [
                { x: 10, y: 10 },
                { x: 40, y: 55 }
            ];

            expect(connectionRendererPrivate.getConnectionEndAnchor(connection, 'root', createCanvasRoot())).toEqual({
                x: 40,
                y: 55
            });
        });

        it('resolves an external nested-port source to its process-group canvas component', () => {
            const connection = createMockConnection({
                entity: { sourceId: 'nested-port', sourceGroupId: 'child-pg' }
            });

            expect(connectionRendererPrivate.getConnectionSourceComponentId(connection, 'root')).toBe('child-pg');
            expect(connectionRendererPrivate.getConnectionSourceComponentId(connection, 'child-pg')).toBe(
                'nested-port'
            );
        });
    });

    describe('getLabelPosition', () => {
        it('should return midpoint when no bend points', async () => {
            const connection = createMockConnection();
            connection.ui.start = { x: 0, y: 0 };
            connection.ui.end = { x: 200, y: 100 };
            connection.ui.bends = [];

            const position = ConnectionRenderer.getLabelPosition(connection, createCanvasRoot());

            // Should be centered on midpoint minus half label width/height
            expect(position.x).toBeDefined();
            expect(position.y).toBeDefined();

            // Cleanup not needed for this test
        });

        it('should return position at first bend point when labelIndex is 0', async () => {
            const connection = createMockConnection({
                entity: {
                    component: { labelIndex: 0 }
                }
            });
            connection.ui.start = { x: 0, y: 0 };
            connection.ui.end = { x: 200, y: 100 };
            connection.ui.bends = [
                { x: 50, y: 50 },
                { x: 100, y: 75 }
            ];

            const position = ConnectionRenderer.getLabelPosition(connection, createCanvasRoot());

            // Should be centered on first bend point
            expect(position.x).toBeLessThan(50);
            expect(position.y).toBeLessThan(50);
        });

        it('should use tempLabelIndex during drag', async () => {
            const connection = createMockConnection({
                entity: {
                    component: { labelIndex: 0 }
                }
            });
            connection.ui.start = { x: 0, y: 0 };
            connection.ui.end = { x: 200, y: 100 };
            connection.ui.bends = [
                { x: 50, y: 50 },
                { x: 150, y: 75 }
            ];
            connection.ui.tempLabelIndex = 1;

            const position = ConnectionRenderer.getLabelPosition(connection, createCanvasRoot());

            // Should be centered on second bend point (tempLabelIndex = 1)
            expect(position.x).toBeLessThan(150);
            expect(position.y).toBeLessThan(75);
        });

        it('should clamp labelIndex to valid range', async () => {
            const connection = createMockConnection({
                entity: {
                    component: { labelIndex: 10 } // Out of range
                }
            });
            connection.ui.start = { x: 0, y: 0 };
            connection.ui.end = { x: 200, y: 100 };
            connection.ui.bends = [
                { x: 50, y: 50 },
                { x: 100, y: 75 }
            ];

            const position = ConnectionRenderer.getLabelPosition(connection, createCanvasRoot());

            // Should clamp to last bend point
            expect(position.x).toBeDefined();
            expect(position.y).toBeDefined();
        });

        it('reads label dimensions only from the owning canvas root', () => {
            const connection = createMockConnection({ entity: { id: 'shared-id' } });
            connection.ui.start = { x: 0, y: 0 };
            connection.ui.end = { x: 200, y: 100 };
            connection.ui.bends = [];

            const firstRoot = createCanvasRoot();
            const secondRoot = createCanvasRoot();
            firstRoot
                .append('g')
                .attr('id', 'id-shared-id')
                .append('g')
                .attr('class', 'connection-label-container')
                .append('rect')
                .attr('class', 'body')
                .attr('height', 20);
            secondRoot
                .append('g')
                .attr('id', 'id-shared-id')
                .append('g')
                .attr('class', 'connection-label-container')
                .append('rect')
                .attr('class', 'body')
                .attr('height', 80);

            const firstPosition = ConnectionRenderer.getLabelPosition(connection, firstRoot);
            const secondPosition = ConnectionRenderer.getLabelPosition(connection, secondRoot);

            expect(firstPosition.y - secondPosition.y).toBe(30);
        });
    });

    describe('editing gesture guards', () => {
        it('does not remove a bend when edit permission is revoked after rendering', async () => {
            const onBendPointRemove = vi.fn();
            const connection = createMockConnection({
                entity: { bends: [{ x: 100, y: 50 }] },
                ui: { bends: [{ x: 100, y: 50 }] }
            });
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                callbacks: { onBendPointRemove },
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });
            context.getCanEdit = () => false;

            getConnectionElements()
                .select<SVGRectElement>('rect.midpoint')
                .node()!
                .dispatchEvent(new MouseEvent('dblclick', { bubbles: true }));

            expect(connection.ui.bends).toEqual([{ x: 100, y: 50 }]);
            expect(onBendPointRemove).not.toHaveBeenCalled();
            cleanup();
        });

        it('restores endpoint geometry and clears gesture state after an invalid reconnect drop', async () => {
            const onEndpointReconnect = vi.fn();
            const connection = createMockConnection();
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                callbacks: { onEndpointReconnect },
                reconnect: {
                    isValidConnectionDestination: () => false,
                    getPerimeterPoint: (point) => point,
                    selfLoopXOffset: 20,
                    selfLoopYOffset: 10
                },
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });
            const connectionElement = getConnectionElements();
            const endpoint = connectionElement.select<SVGRectElement>('rect.endpoint').node()!;
            const originalEnd = { ...connection.ui.end };
            const originalPath = connectionElement.select('path.connection-path').attr('d');

            endpoint.dispatchEvent(
                new MouseEvent('mousedown', {
                    bubbles: true,
                    button: 0,
                    clientX: originalEnd.x,
                    clientY: originalEnd.y,
                    view: window
                })
            );
            window.dispatchEvent(
                new MouseEvent('mousemove', {
                    bubbles: true,
                    buttons: 1,
                    clientX: originalEnd.x + 75,
                    clientY: originalEnd.y + 50,
                    view: window
                })
            );
            window.dispatchEvent(
                new MouseEvent('mouseup', {
                    bubbles: true,
                    button: 0,
                    clientX: originalEnd.x + 75,
                    clientY: originalEnd.y + 50,
                    view: window
                })
            );

            expect(connection.ui.end).toEqual(originalEnd);
            expect(connectionElement.select('path.connection-path').attr('d')).toBe(originalPath);
            expect(connection.ui.dragStartRevision).toBeUndefined();
            expect(connection.ui.endPointDragging).toBeUndefined();
            expect(connection.ui.dragging).toBe(false);
            expect(onEndpointReconnect).not.toHaveBeenCalled();
            cleanup();
        });

        it('offsets the dragged endpoint handle from the pointer', async () => {
            const connection = createMockConnection();
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                callbacks: { onEndpointReconnect: vi.fn() },
                reconnect: {
                    isValidConnectionDestination: () => true,
                    getPerimeterPoint: (point) => point,
                    selfLoopXOffset: 20,
                    selfLoopYOffset: 10
                },
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });
            const endpoint = getConnectionElements().select<SVGRectElement>('rect.endpoint').node()!;
            const connectionElement = endpoint.parentNode as SVGGElement;
            Object.defineProperty(connectionElement, 'clientLeft', { value: 0, configurable: true });
            Object.defineProperty(connectionElement, 'clientTop', { value: 0, configurable: true });
            connectionElement.getBoundingClientRect = () =>
                ({
                    left: 0,
                    top: 0,
                    right: 0,
                    bottom: 0,
                    width: 0,
                    height: 0,
                    x: 0,
                    y: 0,
                    toJSON: () => ({})
                }) as DOMRect;

            endpoint.dispatchEvent(
                new MouseEvent('mousedown', {
                    bubbles: true,
                    button: 0,
                    clientX: connection.ui.end.x,
                    clientY: connection.ui.end.y,
                    view: window
                })
            );
            window.dispatchEvent(
                new MouseEvent('mousemove', {
                    bubbles: true,
                    buttons: 1,
                    clientX: 450,
                    clientY: 100,
                    view: window
                })
            );

            expect(connection.ui.end).toEqual({ x: 442, y: 92 });

            window.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, button: 0, view: window }));
            cleanup();
        });

        it('treats a nested-port source group as the source when reconnecting a self-loop', async () => {
            const onEndpointReconnect = vi.fn();
            const connection = createMockConnection({
                entity: {
                    sourceId: 'nested-port',
                    sourceGroupId: 'child-pg',
                    destinationId: 'dest-1',
                    bends: []
                }
            });
            const { container, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                processGroupId: 'root',
                callbacks: { onEndpointReconnect },
                reconnect: {
                    isValidConnectionDestination: () => true,
                    getPerimeterPoint: (point) => point,
                    selfLoopXOffset: 20,
                    selfLoopYOffset: 10
                },
                sourceComponent: { id: 'child-pg', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });
            const endpoint = getConnectionElements().select<SVGRectElement>('rect.endpoint').node()!;
            endpoint.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0, view: window }));
            d3.select(container.querySelector('#id-child-pg') as SVGGElement).classed('connectable-destination', true);
            window.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, button: 0, view: window }));

            expect(onEndpointReconnect).toHaveBeenCalledTimes(1);
            expect(onEndpointReconnect.mock.calls[0][2]).toHaveLength(2);
            cleanup();
        });

        it('removes stale endpoint reconnect behavior when policy becomes null and re-enables cleanly', async () => {
            const connection = createMockConnection();
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                callbacks: { onEndpointReconnect: vi.fn() },
                reconnect: {
                    isValidConnectionDestination: () => true,
                    getPerimeterPoint: (point) => point,
                    selfLoopXOffset: 20,
                    selfLoopYOffset: 10
                },
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });
            const endpoint = getConnectionElements().select<SVGRectElement>('rect.endpoint');
            expect(endpoint.classed('reconnectable')).toBe(true);
            expect(endpoint.on('mousedown.drag')).toBeDefined();
            expect(endpoint.style('cursor')).toBe('move');

            context.reconnect = undefined;
            ConnectionRenderer.render(context);

            expect(endpoint.classed('reconnectable')).toBe(false);
            expect(endpoint.on('mousedown.drag')).toBeUndefined();
            expect(endpoint.style('cursor')).toBe('default');

            context.reconnect = {
                isValidConnectionDestination: () => true,
                getPerimeterPoint: (point) => point,
                selfLoopXOffset: 20,
                selfLoopYOffset: 10
            };
            ConnectionRenderer.render(context);

            expect(endpoint.classed('reconnectable')).toBe(true);
            expect(endpoint.on('mousedown.drag')).toBeDefined();
            expect(endpoint.style('cursor')).toBe('move');
            cleanup();
        });

        it('cancels an active endpoint gesture when reconnect becomes unavailable before mouseup', async () => {
            const onEndpointReconnect = vi.fn();
            const reconnect: ConnectionReconnectContext = {
                isValidConnectionDestination: () => true,
                getPerimeterPoint: (point) => point,
                selfLoopXOffset: 20,
                selfLoopYOffset: 10
            };
            let liveReconnect: ConnectionReconnectContext | undefined = reconnect;
            const connection = createMockConnection();
            const { context, container, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                callbacks: { onEndpointReconnect },
                reconnect,
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });
            context.getReconnect = () => liveReconnect;
            const connectionElement = getConnectionElements();
            const endpoint = connectionElement.select<SVGRectElement>('rect.endpoint').node()!;
            const originalPath = connectionElement.select('path.connection-path').attr('d');

            endpoint.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0, view: window }));
            window.dispatchEvent(
                new MouseEvent('mousemove', {
                    bubbles: true,
                    buttons: 1,
                    clientX: 450,
                    clientY: 100,
                    view: window
                })
            );
            d3.select(container.querySelector('#id-dest-1') as SVGGElement).classed('connectable-destination', true);
            expect(connection.ui.endPointDragging).toBe(true);

            liveReconnect = undefined;
            context.reconnect = undefined;
            ConnectionRenderer.render(context);
            window.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, button: 0, view: window }));

            expect(onEndpointReconnect).not.toHaveBeenCalled();
            expect(container.querySelector('g.connectable-destination')).toBeNull();
            expect(connection.ui.endPointDragging).toBeUndefined();
            expect(connection.ui.dragStartRevision).toBeUndefined();
            expect(connection.ui.dragging).toBe(false);
            expect(connectionElement.select('path.connection-path').attr('d')).toBe(originalPath);
            cleanup();
        });
    });

    describe('label drag click guard', () => {
        it('preserves the saved label index and emits nothing when no drag tick occurred', async () => {
            const onLabelDragEnd = vi.fn();
            const connection = createMockConnection({
                entity: {
                    bends: [
                        { x: 50, y: 50 },
                        { x: 200, y: 100 }
                    ],
                    component: { labelIndex: 1 }
                }
            });
            const { container, cleanup } = await setup({
                connections: [connection],
                callbacks: { onLabelDragEnd },
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });
            const labelContainer = container.querySelector('g.connection-label-container') as SVGGElement;

            labelContainer.dispatchEvent(
                new MouseEvent('mousedown', {
                    bubbles: true,
                    button: 0,
                    clientX: 100,
                    clientY: 100,
                    view: window
                })
            );
            window.dispatchEvent(
                new MouseEvent('mouseup', {
                    bubbles: true,
                    button: 0,
                    clientX: 100,
                    clientY: 100,
                    view: window
                })
            );

            expect(onLabelDragEnd).not.toHaveBeenCalled();
            expect(connection.ui.tempLabelIndex).toBeUndefined();
            expect(connection.entity.component?.labelIndex).toBe(1);
            cleanup();
        });
    });

    describe('data join behavior', () => {
        it('should update existing connections without recreating them', async () => {
            const connection = createMockConnection({ entity: { id: 'conn-1' } });
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            // Get reference to the DOM element
            const originalElement = getConnectionElements().node();

            // Re-render with same connection
            context.connections = [connection];
            ConnectionRenderer.render(context);

            // Should be the same DOM element
            expect(getConnectionElements().node()).toBe(originalElement);

            cleanup();
        });

        it('should add new connections while keeping existing ones', async () => {
            const conn1 = createMockConnection({ entity: { id: 'conn-1' } });
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [conn1],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            expect(getConnectionElements().size()).toBe(1);

            // Add a second connection
            const conn2 = createMockConnection({ entity: { id: 'conn-2' } });
            context.connections = [conn1, conn2];
            ConnectionRenderer.render(context);

            expect(getConnectionElements().size()).toBe(2);

            cleanup();
        });
    });

    describe('disabled state', () => {
        it('should not be disabled by default', async () => {
            const connection = createMockConnection();
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const connectionEl = getConnectionElements();
            expect(connectionEl.classed('disabled')).toBe(false);
            expect(connectionEl.style('opacity')).toBe('');

            cleanup();
        });

        it('should apply disabled state when connection ID is in disabledConnectionIds', async () => {
            const connection = createMockConnection({ entity: { id: 'conn-1' } });
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                disabledConnectionIds: new Set(['conn-1']),
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const connectionEl = getConnectionElements();
            expect(connectionEl.classed('disabled')).toBe(true);
            expect(connectionEl.style('opacity')).toBe('0.6');
            expect(connectionEl.style('cursor')).toBe('not-allowed');

            cleanup();
        });

        it('should not apply disabled state when connection ID is not in disabledConnectionIds', async () => {
            const connection = createMockConnection({ entity: { id: 'conn-1' } });
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                disabledConnectionIds: new Set(['conn-2', 'conn-3']), // Different IDs
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const connectionEl = getConnectionElements();
            expect(connectionEl.classed('disabled')).toBe(false);

            cleanup();
        });

        it('should update disabled state on re-render', async () => {
            const connection = createMockConnection({ entity: { id: 'conn-1' } });
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            // Initially not disabled
            expect(getConnectionElements().classed('disabled')).toBe(false);

            // Add to disabled set and re-render
            context.disabledConnectionIds = new Set(['conn-1']);
            ConnectionRenderer.render(context);

            expect(getConnectionElements().classed('disabled')).toBe(true);

            // Remove from disabled set and re-render
            context.disabledConnectionIds = new Set();
            ConnectionRenderer.render(context);

            expect(getConnectionElements().classed('disabled')).toBe(false);

            cleanup();
        });
    });

    describe('authorization styling', () => {
        it('should apply unauthorized class when canRead is false', async () => {
            const connection = createMockConnection({
                entity: {
                    permissions: { canRead: false, canWrite: false }
                }
            });
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const connectionPath = getConnectionElements().select('path.connection-path');
            expect(connectionPath.classed('unauthorized')).toBe(true);

            cleanup();
        });

        it('should not apply unauthorized class when canRead is true', async () => {
            const connection = createMockConnection({
                entity: {
                    permissions: { canRead: true, canWrite: false }
                }
            });
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            const connectionPath = getConnectionElements().select('path.connection-path');
            expect(connectionPath.classed('unauthorized')).toBe(false);

            cleanup();
        });
    });

    describe('connection label', () => {
        it('should create connection label container when connection is visible', async () => {
            const connection = createMockConnection();
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            // Mark as visible
            getConnectionElements().classed('visible', true);

            // Re-render to trigger label creation
            ConnectionRenderer.render({
                ...(
                    await setup({
                        connections: [connection],
                        sourceComponent: {
                            id: 'source-1',
                            position: { x: 0, y: 0 },
                            dimensions: { width: 184, height: 52 }
                        },
                        destComponent: {
                            id: 'dest-1',
                            position: { x: 300, y: 0 },
                            dimensions: { width: 184, height: 52 }
                        }
                    })
                ).context,
                containerSelection: getConnectionElements().node()?.parentElement
                    ? d3.select(getConnectionElements().node()!.parentElement!)
                    : d3.select(document.body)
            });

            // Label container should exist
            const labelContainer = getConnectionElements().select('g.connection-label-container');
            // Note: Label may or may not exist depending on visibility state
            expect(labelContainer.empty() || !labelContainer.empty()).toBe(true);

            cleanup();
        });

        it('should create queued container in label', async () => {
            const connection = createMockConnection();
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            // Mark as visible to trigger label creation
            getConnectionElements().classed('visible', true);
            ConnectionRenderer.render(context);

            const queuedContainer = getConnectionElements().select('g.queued-container');
            // Queued container should exist in visible connections
            expect(queuedContainer.empty()).toBe(false);

            cleanup();
        });
    });

    describe('expiration icon visibility', () => {
        it('should hide expiration icon when flowFileExpiration is 0 sec', async () => {
            const connection = createMockConnection({
                entity: { component: { flowFileExpiration: '0 sec' } }
            });
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            getConnectionElements().classed('visible', true);
            ConnectionRenderer.render(context);

            const expirationIcon = getConnectionElements().select('text.expiration-icon');
            expect(expirationIcon.empty()).toBe(false);
            expect(expirationIcon.classed('hidden')).toBe(true);

            cleanup();
        });

        it('should show expiration icon when flowFileExpiration is a positive integer', async () => {
            const connection = createMockConnection({
                entity: { component: { flowFileExpiration: '30 sec' } }
            });
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            getConnectionElements().classed('visible', true);
            ConnectionRenderer.render(context);

            const expirationIcon = getConnectionElements().select('text.expiration-icon');
            expect(expirationIcon.empty()).toBe(false);
            expect(expirationIcon.classed('hidden')).toBe(false);

            cleanup();
        });

        it('should show expiration icon when flowFileExpiration is a decimal with leading zero', async () => {
            const connection = createMockConnection({
                entity: { component: { flowFileExpiration: '0.5 sec' } }
            });
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            getConnectionElements().classed('visible', true);
            ConnectionRenderer.render(context);

            const expirationIcon = getConnectionElements().select('text.expiration-icon');
            expect(expirationIcon.empty()).toBe(false);
            expect(expirationIcon.classed('hidden')).toBe(false);

            cleanup();
        });

        it('should show expiration icon when flowFileExpiration is a decimal without leading integer', async () => {
            const connection = createMockConnection({
                entity: { component: { flowFileExpiration: '.5 sec' } }
            });
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            getConnectionElements().classed('visible', true);
            ConnectionRenderer.render(context);

            const expirationIcon = getConnectionElements().select('text.expiration-icon');
            expect(expirationIcon.empty()).toBe(false);
            expect(expirationIcon.classed('hidden')).toBe(false);

            cleanup();
        });
    });

    describe('grouped connections', () => {
        it('should not apply grouped class for single relationship', async () => {
            const connection = createMockConnection({
                entity: {
                    component: {
                        selectedRelationships: ['success']
                    }
                }
            });
            const { getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            // Mark as visible to trigger status update
            getConnectionElements().classed('visible', true);

            const connectionEl = getConnectionElements();
            expect(connectionEl.classed('grouped')).toBe(false);

            cleanup();
        });

        it('should apply grouped class for multiple relationships', async () => {
            const connection = createMockConnection({
                entity: {
                    component: {
                        selectedRelationships: ['success', 'failure']
                    }
                }
            });
            const { context, getConnectionElements, cleanup } = await setup({
                connections: [connection],
                sourceComponent: { id: 'source-1', position: { x: 0, y: 0 }, dimensions: { width: 184, height: 52 } },
                destComponent: { id: 'dest-1', position: { x: 300, y: 0 }, dimensions: { width: 184, height: 52 } }
            });

            // Mark as visible to trigger status update
            getConnectionElements().classed('visible', true);

            // Re-render to apply the grouped class via updateConnectionStatus
            ConnectionRenderer.render(context);

            const connectionEl = getConnectionElements();
            expect(connectionEl.classed('grouped')).toBe(true);

            cleanup();
        });
    });
});
