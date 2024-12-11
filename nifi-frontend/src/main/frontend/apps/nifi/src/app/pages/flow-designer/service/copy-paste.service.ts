/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Dimensions, PasteRequest, PasteRequestContext, PasteRequestEntity } from '../state/flow';
import { Observable } from 'rxjs';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { Position } from '../state/shared';
import { CanvasView } from './canvas-view.service';
import { CopyRequestContext, CopyResponseEntity, PasteRequestStrategy } from '../../../state/copy';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../state';
import { selectCurrentProcessGroupId } from '../state/flow/flow.selectors';
import * as d3 from 'd3';

@Injectable({
    providedIn: 'root'
})
export class CopyPasteService {
    private static readonly API: string = '../nifi-api';
    currentProcessGroupId = this.store.selectSignal(selectCurrentProcessGroupId);

    constructor(
        private httpClient: HttpClient,
        private clusterConnectionService: ClusterConnectionService,
        private canvasView: CanvasView,
        private store: Store<NiFiState>
    ) {}

    copy(copyRequest: CopyRequestContext): Observable<CopyResponseEntity> {
        return this.httpClient.post(
            `${CopyPasteService.API}/process-groups/${copyRequest.processGroupId}/copy`,
            copyRequest.copyRequestEntity
        ) as Observable<CopyResponseEntity>;
    }

    paste(pasteRequest: PasteRequestContext): Observable<any> {
        const payload: PasteRequestEntity = {
            ...pasteRequest.pasteRequest,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
        };
        return this.httpClient.put(
            `${CopyPasteService.API}/process-groups/${pasteRequest.processGroupId}/paste`,
            payload
        );
    }

    public isCopiedContentInView(copyResponse: CopyResponseEntity): boolean {
        const bbox = this.calculateBoundingBoxForCopiedContent(copyResponse);
        return this.canvasView.isBoundingBoxInViewport(bbox, false);
    }

    /**
     * Use when pasting components to the same process group they were copied from and some
     * part of those components are still visible on canvas
     * @param copyResponse
     * @param pasteIncrement how many times the content has been pasted already. used to determine the overall offset.
     * @private
     */
    public toOffsetPasteRequest(copyResponse: CopyResponseEntity, pasteIncrement: number = 0): PasteRequest {
        const offset = 25;
        const paste: PasteRequest = {
            copyResponse: this.cloneCopyResponseEntity(copyResponse),
            strategy: PasteRequestStrategy.OFFSET_FROM_ORIGINAL
        };

        Object.values(paste.copyResponse)
            .filter((values) => !!values && Array.isArray(values))
            .forEach((values: any[]) => {
                values.forEach((value) => {
                    if (value.position) {
                        value.position.x += offset * (pasteIncrement + 1);
                        value.position.y += offset * (pasteIncrement + 1);
                    } else if (value.bends) {
                        value.bends.forEach((bend: Position) => {
                            bend.x += offset * (pasteIncrement + 1);
                            bend.y += offset * (pasteIncrement + 1);
                        });
                    }
                });
            });
        return paste;
    }

    /**
     * Use when it isn't known if the copied content is still visible on the screen (possibly a different pg or browser tab),
     * or it is known to be off-screen.
     * @param copyResponse
     * @private
     */
    public toCenteredPasteRequest(copyResponse: CopyResponseEntity): PasteRequest {
        const paste: PasteRequest = {
            copyResponse: this.cloneCopyResponseEntity(copyResponse),
            strategy: PasteRequestStrategy.CENTER_ON_CANVAS
        };

        // get center of canvas
        const canvasBBox = this.canvasView.getCanvasBoundingClientRect();
        if (canvasBBox) {
            // Get the normalized center of the canvas to later compare with the center of the items being pasted
            const canvasCenterNormalized = this.canvasView.getCanvasPosition({
                x: canvasBBox.width / 2 + canvasBBox.left,
                y: canvasBBox.height / 2 + canvasBBox.top
            });
            if (canvasCenterNormalized) {
                // get the bounding box of the items being pasted (including the bends of connections)
                const copiedBBox = this.calculateBoundingBoxForCopiedContent(paste.copyResponse);

                // get it's center
                const centerOfCopiedContent: Position = {
                    x: copiedBBox.width / 2 + copiedBBox.x,
                    y: copiedBBox.height / 2 + copiedBBox.y
                };

                // find the difference between the centers
                const centerOffset: Position = {
                    x: canvasCenterNormalized.x - centerOfCopiedContent.x,
                    y: canvasCenterNormalized.y - centerOfCopiedContent.y
                };

                // try to detect if the proposed paste content has already been pasted and might overlap
                const offset = this.calculateOffsetForCenterPaste(paste.copyResponse, centerOffset);

                // offset all items (and bends) by the diff of the centers
                Object.values(paste.copyResponse)
                    .filter((values) => !!values && Array.isArray(values))
                    .forEach((componentArray: any[]) => {
                        componentArray.forEach((component) => {
                            if (component.position) {
                                component.position.x += centerOffset.x + offset;
                                component.position.y += centerOffset.y + offset;
                            } else if (component.bends) {
                                component.bends.forEach((bend: Position) => {
                                    bend.x += centerOffset.x + offset;
                                    bend.y += centerOffset.y + offset;
                                });
                            }
                        });
                    });

                // set the new bounding box on the request with a scale that would fit the contents
                paste.bbox = {
                    height: copiedBBox.height,
                    width: copiedBBox.width,
                    x: copiedBBox.x + centerOffset.x,
                    y: copiedBBox.y + centerOffset.y
                };

                const willItFit = this.canvasView.isBoundingBoxInViewport(paste.bbox, true);
                if (!willItFit) {
                    paste.fitToScreen = true;
                    const scale = Math.min(canvasBBox.width / copiedBBox.width, canvasBBox.height / copiedBBox.height);
                    paste.bbox.scale = scale * 0.95; // leave a bit of padding around the newly centered selection
                }
            }
        }
        return paste;
    }

    private calculateOffsetForCenterPaste(proposedPaste: CopyResponseEntity, centerOffset: Position): number {
        // get the positions of things already on the screen
        const existingPositions = this.getAllComponentPositions();
        const offsetIncrement = 25;
        const buffer = 4;
        let offset = 0;

        // get a sample component to probe the canvas with to detect a duplicate paste
        const positioned = Object.values(proposedPaste)
            .filter((values) => !!values && Array.isArray(values))
            .flat()
            .filter((component) => {
                return !!component.position;
            })
            .map((component) => component.position);

        if (positioned.length > 0) {
            const sample: Position = {
                x: positioned[0].x + centerOffset.x,
                y: positioned[0].y + centerOffset.y
            };

            let foundCollision = existingPositions.some(
                (position) =>
                    position.x >= sample.x - buffer &&
                    position.x <= sample.x + buffer &&
                    position.y >= sample.y - buffer &&
                    position.y <= sample.y + buffer
            );

            while (foundCollision) {
                offset += offsetIncrement;
                foundCollision = existingPositions.some(
                    (position) =>
                        position.x >= sample.x + offset - buffer &&
                        position.x <= sample.x + offset + buffer &&
                        position.y >= sample.y + offset - buffer &&
                        position.y <= sample.y + offset + buffer
                );
            }
        }
        return offset;
    }

    private getAllComponentPositions() {
        const positions: Position[] = [];
        const selectionBoundingBox = this.canvasView.getCanvasBoundingClientRect();
        if (selectionBoundingBox) {
            d3.selectAll('g.component').each((d: any) => {
                positions.push(d.position);
            });
        }
        return positions;
    }

    private cloneCopyResponseEntity(copyResponse: CopyResponseEntity): CopyResponseEntity {
        const arrayOrUndefined = (arr: any[] | undefined) => {
            if (arr && Array.isArray(arr) && arr.length > 0) {
                if (arr[0].position) {
                    return arr.map((component: any) => {
                        if (component.position) {
                            return {
                                ...component,
                                position: {
                                    ...component.position
                                }
                            };
                        }
                    });
                } else {
                    // this is an array of connections, handle them differently to account for bends
                    return arr.map((connection: any) => {
                        if (connection.bends && connection.bends.length > 0) {
                            const clonedBends = connection.bends.map((bend: Position) => {
                                return {
                                    ...bend
                                };
                            });
                            return {
                                ...connection,
                                bends: clonedBends
                            };
                        }
                        return {
                            ...connection
                        };
                    });
                }
            }
            return undefined;
        };
        return {
            id: copyResponse.id,
            connections: arrayOrUndefined(copyResponse.connections),
            funnels: arrayOrUndefined(copyResponse.funnels),
            inputPorts: arrayOrUndefined(copyResponse.inputPorts),
            labels: arrayOrUndefined(copyResponse.labels),
            outputPorts: arrayOrUndefined(copyResponse.outputPorts),
            processGroups: arrayOrUndefined(copyResponse.processGroups),
            processors: arrayOrUndefined(copyResponse.processors),
            remoteProcessGroups: arrayOrUndefined(copyResponse.remoteProcessGroups),
            externalControllerServiceReferences: copyResponse.externalControllerServiceReferences,
            parameterContexts: copyResponse.parameterContexts,
            parameterProviders: copyResponse.parameterProviders
        } as CopyResponseEntity;
    }

    private calculateBoundingBoxForCopiedContent(copyResponse: CopyResponseEntity): any {
        const bbox = {
            left: Number.MAX_SAFE_INTEGER,
            top: Number.MAX_SAFE_INTEGER,
            right: Number.MIN_SAFE_INTEGER,
            bottom: Number.MIN_SAFE_INTEGER
        };
        Object.values(copyResponse)
            .flat()
            .filter((value: any[]) => !!value)
            .reduce((acc, current) => {
                if (current.componentType) {
                    const dimensions: Dimensions = this.getComponentWidth(current);
                    if (current.componentType === 'CONNECTION') {
                        current.bends.forEach((bend: Position) => {
                            acc.left = Math.min(acc.left, bend.x);
                            acc.top = Math.min(acc.top, bend.y);
                            acc.right = Math.max(acc.right, bend.x);
                            acc.bottom = Math.max(acc.bottom, bend.y);
                        });
                    } else {
                        acc.left = Math.min(acc.left, current.position.x);
                        acc.top = Math.min(acc.top, current.position.y);
                        acc.right = Math.max(acc.right, current.position.x + dimensions.width);
                        acc.bottom = Math.max(acc.bottom, current.position.y + dimensions.height);
                    }
                }
                return acc;
            }, bbox);

        return {
            x: bbox.left,
            y: bbox.top,
            width: bbox.right - bbox.left,
            height: bbox.bottom - bbox.top
        };
    }

    private getComponentWidth(component: any): Dimensions {
        if (!component) {
            return { height: 0, width: 0 };
        }
        switch (component.componentType) {
            case 'PROCESSOR':
                return {
                    width: 352,
                    height: 128
                };
            case 'PROCESS_GROUP':
            case 'REMOTE_PROCESS_GROUP':
                return {
                    width: 384,
                    height: 176
                };
            case 'INPUT_PORT':
            case 'OUTPUT_PORT':
            case 'REMOTE_INPUT_PORT':
            case 'REMOTE_OUTPUT_PORT':
                return {
                    width: 240,
                    height: 48
                };
            case 'FUNNEL':
                return { height: 48, width: 48 };
            case 'LABEL':
                return { height: component.height, width: component.width };
            default:
                return { height: 0, width: 0 };
        }
    }
}
