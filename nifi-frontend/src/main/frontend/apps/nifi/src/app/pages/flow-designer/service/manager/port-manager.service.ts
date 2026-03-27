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

import { Injectable, OnDestroy, inject } from '@angular/core';
import { CanvasState } from '../../state';
import { Store } from '@ngrx/store';
import { CanvasUtils } from '../canvas-utils.service';
import { PositionBehavior } from '../behavior/position-behavior.service';
import { SelectableBehavior } from '../behavior/selectable-behavior.service';
import { EditableBehavior } from '../behavior/editable-behavior.service';
import * as d3 from 'd3';
import {
    selectAnySelectedComponentIds,
    selectFlowLoadingStatus,
    selectPorts,
    selectTransitionRequired
} from '../../state/flow/flow.selectors';
import { QuickSelectBehavior } from '../behavior/quick-select-behavior.service';
import { ComponentType, TextTip, NiFiCommon } from '@nifi/shared';
import { ValidationErrorsTip } from '../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { Dimension } from '../../state/shared';
import { filter, Subject, switchMap, takeUntil } from 'rxjs';
import { renderConnectionsForComponent } from '../../state/flow/flow.actions';

@Injectable({
    providedIn: 'root'
})
export class PortManager implements OnDestroy {
    private store = inject<Store<CanvasState>>(Store);
    private canvasUtils = inject(CanvasUtils);
    private nifiCommon = inject(NiFiCommon);
    private positionBehavior = inject(PositionBehavior);
    private selectableBehavior = inject(SelectableBehavior);
    private quickSelectBehavior = inject(QuickSelectBehavior);
    private editableBehavior = inject(EditableBehavior);

    private destroyed$: Subject<boolean> = new Subject();

    private portDimensions: Dimension = {
        width: 240,
        height: 48
    };
    private remotePortDimensions: Dimension = {
        width: 240,
        height: 80
    };

    private static readonly PREVIEW_NAME_LENGTH: number = 15;
    private static readonly OFFSET_VALUE: number = 25;

    private ports: [] = [];
    private portContainer: any = null;
    private transitionRequired = false;

    private dimensions(d: any): Dimension {
        return d.allowRemoteAccess === true ? this.remotePortDimensions : this.portDimensions;
    }

    private portType(d: any): ComponentType {
        return d.portType === 'INPUT_PORT' ? ComponentType.InputPort : ComponentType.OutputPort;
    }

    /**
     * Utility method to check if the target port is a local port.
     */
    private isLocalPort(d: any) {
        return d.allowRemoteAccess !== true;
    }

    /**
     * Utility method to calculate offset y position based on whether this port is remotely accessible.
     */
    private offsetY(y: any) {
        return (d: any) => y + (this.isLocalPort(d) ? 0 : PortManager.OFFSET_VALUE);
    }

    private select() {
        return this.portContainer.selectAll('g.input-port, g.output-port').data(this.ports, function (d: any) {
            return d.id;
        });
    }

    private renderPorts(entered: any) {
        if (entered.empty()) {
            return entered;
        }

        const port = entered
            .append('g')
            .attr('id', function (d: any) {
                return 'id-' + d.id;
            })
            .attr('class', function (d: any) {
                if (d.portType === 'INPUT_PORT') {
                    return 'input-port component';
                } else {
                    return 'output-port component';
                }
            });

        // port border
        port.append('rect')
            .attr('class', 'border')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .attr('fill', 'transparent')
            .attr('stroke', 'transparent');

        // port body
        port.append('rect')
            .attr('class', 'body')
            .attr('width', function (d: any) {
                return d.dimensions.width;
            })
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .attr('filter', 'url(#component-drop-shadow)')
            .attr('stroke-width', 0);

        // port remote banner
        port.append('rect')
            .attr('class', 'remote-banner banner')
            .attr('width', this.remotePortDimensions.width)
            .attr('height', PortManager.OFFSET_VALUE)
            .classed('hidden', this.isLocalPort);

        // port icon
        port.append('text')
            .attr('class', 'port-icon')
            .attr('x', 10)
            .attr('y', this.offsetY(38))
            .text(function (d: any) {
                if (d.portType === 'INPUT_PORT') {
                    return '\ue832';
                } else {
                    return '\ue833';
                }
            });

        // port name
        port.append('text')
            .attr('x', 70)
            .attr('y', this.offsetY(25))
            .attr('width', 95)
            .attr('height', 30)
            .attr('class', 'port-name');

        this.selectableBehavior.activate(port);
        this.quickSelectBehavior.activate(port);

        return port;
    }

    private updatePorts(updated: any) {
        if (updated.empty()) {
            return;
        }

        // port border authorization
        updated
            .select('rect.border')
            .attr('height', (d: any) => d.dimensions.height)
            .classed('unauthorized', (d: any) => d.permissions.canRead === false);

        // port body authorization
        updated
            .select('rect.body')
            .attr('height', (d: any) => d.dimensions.height)
            .classed('unauthorized', (d: any) => d.permissions.canRead === false);

        updated.each((portData: any, i: number, nodes: Element[]) => {
            const port: any = d3.select(nodes[i]);
            let details: any = port.select('g.port-details');

            // update the component behavior as appropriate
            this.editableBehavior.editable(port);

            // if this port is visible, render everything
            if (port.classed('visible')) {
                if (details.empty()) {
                    // Adding details when the port is rendered for the 1st time, or it becomes visible due to permission updates.
                    details = port.append('g').attr('class', 'port-details');

                    // port transmitting icon
                    details
                        .append('text')
                        .attr('class', 'port-transmission-icon')
                        .attr('x', 10)
                        .attr('y', 18)
                        .classed('hidden', this.isLocalPort);

                    // bulletin background
                    details
                        .append('rect')
                        .attr('class', 'bulletin-background')
                        .attr('x', this.remotePortDimensions.width - PortManager.OFFSET_VALUE)
                        .attr('width', PortManager.OFFSET_VALUE)
                        .attr('height', PortManager.OFFSET_VALUE)
                        .classed('hidden', this.isLocalPort);

                    // bulletin icon
                    details
                        .append('text')
                        .attr('class', 'bulletin-icon')
                        .attr('x', this.remotePortDimensions.width - 18)
                        .attr('y', 18)
                        .text('\uf24a')
                        .classed('hidden', this.isLocalPort);

                    // run status icon
                    details.append('text').attr('class', 'run-status-icon').attr('x', 50).attr('y', this.offsetY(25));

                    // --------
                    // comments
                    // --------

                    details
                        .append('text')
                        .attr('class', 'component-comments')
                        .attr(
                            'transform',
                            'translate(' +
                                (portData.dimensions.width - 11) +
                                ', ' +
                                (portData.dimensions.height - 3) +
                                ')'
                        )
                        .text('\uf075');

                    // -------------------
                    // active thread count
                    // -------------------

                    // active thread count
                    details
                        .append('text')
                        .attr('class', 'active-thread-count-icon')
                        .attr('y', this.offsetY(43))
                        .text('\ue83f');

                    // active thread icon
                    details.append('text').attr('class', 'active-thread-count').attr('y', this.offsetY(43));
                }

                if (portData.permissions.canRead) {
                    // Update the remote port banner, these are needed when remote access is changed.
                    port.select('rect.remote-banner').classed('hidden', this.isLocalPort);

                    port.select('text.port-icon').attr('y', this.offsetY(38));

                    details.select('text.port-transmission-icon').classed('hidden', this.isLocalPort);

                    details.select('rect.bulletin-background').classed('hidden', this.isLocalPort);

                    details.select('rect.bulletin-icon').classed('hidden', this.isLocalPort);

                    // update the port name
                    port.select('text.port-name')
                        .attr('y', this.offsetY(25))
                        .each((_d: any, i: number, nodes: Element[]) => {
                            const portName = d3.select(nodes[i]);
                            const name = portData.component.name;
                            const words = name.split(/\s+/);

                            // reset the port name to handle any previous state
                            portName.text(null).selectAll('tspan, title').remove();

                            // handle based on the number of tokens in the port name
                            if (words.length === 1) {
                                // apply ellipsis to the port name as necessary
                                this.canvasUtils.ellipsis(portName, name, 'port-name');
                            } else {
                                this.canvasUtils.multilineEllipsis(portName, 2, name, 'port-name');
                            }
                        })
                        .append('title')
                        .text(function (d: any) {
                            return d.component.name;
                        });

                    // update the port comments
                    port.select('text.component-comments')
                        .style(
                            'visibility',
                            this.nifiCommon.isBlank(portData.component.comments) ? 'hidden' : 'visible'
                        )
                        .each((_d: any, i: number, nodes: Element[]) => {
                            if (!this.nifiCommon.isBlank(portData.component.comments)) {
                                this.canvasUtils.canvasTooltip(
                                    TextTip,
                                    d3.select(nodes[i]),
                                    portData.component.comments
                                );
                            } else {
                                this.canvasUtils.resetCanvasTooltip(d3.select(nodes[i]));
                            }
                        });
                } else {
                    // clear the port name
                    port.select('text.port-name').text(null);

                    // clear the port comments
                    port.select('text.component-comments').style('visibility', 'hidden');
                }

                // populate the stats
                this.updatePortStatus(port);

                // Update connections to update anchor point positions those may have been updated by changing ports remote accessibility.
                this.store.dispatch(
                    renderConnectionsForComponent({ id: portData.id, updatePath: true, updateLabel: true })
                );
            } else {
                if (portData.permissions.canRead) {
                    // update the port name
                    port.select('text.port-name').text(function (d: any) {
                        const name = d.component.name;
                        if (name.length > PortManager.PREVIEW_NAME_LENGTH) {
                            return name.substring(0, PortManager.PREVIEW_NAME_LENGTH) + String.fromCharCode(8230);
                        } else {
                            return name;
                        }
                    });
                } else {
                    // clear the port name
                    port.select('text.port-name').text(null);
                }

                // remove the details if necessary
                if (!details.empty()) {
                    details.remove();
                }
            }
        });
    }

    private updatePortStatus(updated: any) {
        if (updated.empty()) {
            return;
        }

        // update the run status
        updated
            .select('text.run-status-icon')
            .attr('class', (d: any) => {
                let clazz = 'primary-color';

                if (d.status.aggregateSnapshot.runStatus === 'Invalid') {
                    clazz = 'invalid caution-color';
                } else if (d.status.aggregateSnapshot.runStatus === 'Running') {
                    clazz = 'running success-color-default';
                } else if (d.status.aggregateSnapshot.runStatus === 'Stopped') {
                    clazz = 'stopped error-color-variant';
                }

                return `run-status-icon ${clazz}`;
            })
            .attr('font-family', (d: any) => {
                let family = 'FontAwesome';
                if (d.status.aggregateSnapshot.runStatus === 'Disabled') {
                    family = 'flowfont';
                }
                return family;
            })
            .attr('y', this.offsetY(25))
            .text((d: any) => {
                let img = '';
                if (d.status.aggregateSnapshot.runStatus === 'Disabled') {
                    img = '\ue802';
                } else if (d.status.aggregateSnapshot.runStatus === 'Invalid') {
                    img = '\uf071';
                } else if (d.status.aggregateSnapshot.runStatus === 'Running') {
                    img = '\uf04b';
                } else if (d.status.aggregateSnapshot.runStatus === 'Stopped') {
                    img = '\uf04d';
                }
                return img;
            })
            .each((d: any, i: number, nodes: Element[]) => {
                // if there are validation errors generate a tooltip
                if (d.permissions.canRead && !this.nifiCommon.isEmpty(d.component.validationErrors)) {
                    this.canvasUtils.canvasTooltip(ValidationErrorsTip, d3.select(nodes[i]), {
                        isValidating: false,
                        validationErrors: d.component.validationErrors
                    });
                } else {
                    this.canvasUtils.resetCanvasTooltip(d3.select(nodes[i]));
                }
            });

        updated
            .select('text.port-transmission-icon')
            .attr('font-family', (d: any) => {
                if (d.status.transmitting === true) {
                    return 'FontAwesome';
                } else {
                    return 'flowfont';
                }
            })
            .text((d: any) => {
                if (d.status.transmitting === true) {
                    return '\uf140';
                } else {
                    return '\ue80a';
                }
            })
            .classed('transmitting success-color-variant', (d: any) => d.status.transmitting === true)
            .classed('not-transmitting neutral-color', (d: any) => d.status.transmitting !== true);

        updated.each((d: any, i: number, nodes: Element[]) => {
            const port: any = d3.select(nodes[i]);

            // -------------------
            // active thread count
            // -------------------

            this.canvasUtils.activeThreadCount(port, d);

            port.select('text.active-thread-count-icon').attr('y', this.offsetY(43));
            port.select('text.active-thread-count').attr('y', this.offsetY(43));

            // ---------
            // bulletins
            // ---------

            this.canvasUtils.bulletins(port, d.bulletins);
        });
    }

    private removePorts(removed: any) {
        removed.remove();
    }

    public init(): void {
        this.portContainer = d3.select('#canvas').append('g').attr('pointer-events', 'all').attr('class', 'ports');

        this.store
            .select(selectPorts)
            .pipe(
                filter(() => this.portContainer !== null),
                takeUntil(this.destroyed$)
            )
            .subscribe((ports) => {
                this.set(ports);
            });

        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'success'),
                filter(() => this.portContainer !== null),
                switchMap(() => this.store.select(selectAnySelectedComponentIds)),
                takeUntil(this.destroyed$)
            )
            .subscribe((selected) => {
                this.portContainer.selectAll('g.input-port, g.output-port').classed('selected', function (d: any) {
                    return selected.includes(d.id);
                });
            });

        this.store
            .select(selectTransitionRequired)
            .pipe(takeUntil(this.destroyed$))
            .subscribe((transitionRequired) => {
                this.transitionRequired = transitionRequired;
            });
    }

    public destroy(): void {
        this.portContainer = null;
        this.destroyed$.next(true);
    }

    ngOnDestroy(): void {
        this.destroyed$.complete();
    }

    private set(ports: any): void {
        // update the ports
        this.ports = ports.map((port: any) => {
            return {
                ...port,
                type: this.portType(port),
                dimensions: this.dimensions(port)
            };
        });

        // select
        const selection = this.select();

        // enter
        const entered = this.renderPorts(selection.enter());

        // update
        const updated = selection.merge(entered);
        this.updatePorts(updated);

        // position
        this.positionBehavior.position(updated, this.transitionRequired);

        // exit
        this.removePorts(selection.exit());
    }

    public selectAll(): any {
        return this.portContainer.selectAll('g.input-port, g.output-port');
    }

    public render(): void {
        this.updatePorts(this.selectAll());
    }

    public pan(): void {
        this.updatePorts(
            this.portContainer.selectAll(
                'g.input-port.entering, g.output-port.entering, g.input-port.leaving, g.output-port.leaving'
            )
        );
    }
}
