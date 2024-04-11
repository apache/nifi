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

import { DestroyRef, inject, Injectable, ViewContainerRef } from '@angular/core';
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
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { QuickSelectBehavior } from '../behavior/quick-select-behavior.service';
import { TextTip } from '../../../../ui/common/tooltips/text-tip/text-tip.component';
import { ValidationErrorsTip } from '../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { Dimension } from '../../state/shared';
import { ComponentType } from '../../../../state/shared';
import { filter, switchMap } from 'rxjs';
import { NiFiCommon } from '../../../../service/nifi-common.service';
import { renderConnectionsForComponent } from '../../state/flow/flow.actions';

@Injectable({
    providedIn: 'root'
})
export class PortManager {
    private destroyRef = inject(DestroyRef);

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
    private portContainer: any;
    private transitionRequired = false;

    private viewContainerRef: ViewContainerRef | undefined;

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils,
        private nifiCommon: NiFiCommon,
        private positionBehavior: PositionBehavior,
        private selectableBehavior: SelectableBehavior,
        private quickSelectBehavior: QuickSelectBehavior,
        private editableBehavior: EditableBehavior
    ) {}

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
        const self: PortManager = this;
        return function (d: any) {
            return y + (self.isLocalPort(d) ? 0 : PortManager.OFFSET_VALUE);
        };
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
            .attr('class', 'remote-banner')
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
        const self: PortManager = this;

        // port border authorization
        updated
            .select('rect.border')
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .classed('unauthorized', function (d: any) {
                return d.permissions.canRead === false;
            });

        // port body authorization
        updated
            .select('rect.body')
            .attr('height', function (d: any) {
                return d.dimensions.height;
            })
            .classed('unauthorized', function (d: any) {
                return d.permissions.canRead === false;
            });

        updated.each(function (this: any, portData: any) {
            const port: any = d3.select(this);
            let details: any = port.select('g.port-details');

            // update the component behavior as appropriate
            self.editableBehavior.editable(port);

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
                        .classed('hidden', self.isLocalPort);

                    // bulletin background
                    details
                        .append('rect')
                        .attr('class', 'bulletin-background')
                        .attr('x', self.remotePortDimensions.width - PortManager.OFFSET_VALUE)
                        .attr('width', PortManager.OFFSET_VALUE)
                        .attr('height', PortManager.OFFSET_VALUE)
                        .classed('hidden', self.isLocalPort);

                    // bulletin icon
                    details
                        .append('text')
                        .attr('class', 'bulletin-icon')
                        .attr('x', self.remotePortDimensions.width - 18)
                        .attr('y', 18)
                        .text('\uf24a')
                        .classed('hidden', self.isLocalPort);

                    // run status icon
                    details.append('text').attr('class', 'run-status-icon').attr('x', 50).attr('y', self.offsetY(25));

                    // --------
                    // comments
                    // --------

                    details
                        .append('path')
                        .attr('class', 'component-comments')
                        .attr(
                            'transform',
                            'translate(' +
                                (portData.dimensions.width - 2) +
                                ', ' +
                                (portData.dimensions.height - 10) +
                                ')'
                        )
                        .attr('d', 'm0,0 l0,8 l-8,0 z');

                    // -------------------
                    // active thread count
                    // -------------------

                    // active thread count
                    details
                        .append('text')
                        .attr('class', 'active-thread-count-icon')
                        .attr('y', self.offsetY(43))
                        .text('\ue83f');

                    // active thread icon
                    details.append('text').attr('class', 'active-thread-count').attr('y', self.offsetY(43));
                }

                if (portData.permissions.canRead) {
                    // Update the remote port banner, these are needed when remote access is changed.
                    port.select('rect.remote-banner').classed('hidden', self.isLocalPort);

                    port.select('text.port-icon').attr('y', self.offsetY(38));

                    details.select('text.port-transmission-icon').classed('hidden', self.isLocalPort);

                    details.select('rect.bulletin-background').classed('hidden', self.isLocalPort);

                    details.select('rect.bulletin-icon').classed('hidden', self.isLocalPort);

                    // update the port name
                    port.select('text.port-name')
                        .attr('y', self.offsetY(25))
                        .each(function (this: any, d: any) {
                            const portName = d3.select(this);
                            const name = d.component.name;
                            const words = name.split(/\s+/);

                            // reset the port name to handle any previous state
                            portName.text(null).selectAll('tspan, title').remove();

                            // handle based on the number of tokens in the port name
                            if (words.length === 1) {
                                // apply ellipsis to the port name as necessary
                                self.canvasUtils.ellipsis(portName, name, 'port-name');
                            } else {
                                self.canvasUtils.multilineEllipsis(portName, 2, name, 'port-name');
                            }
                        })
                        .append('title')
                        .text(function (d: any) {
                            return d.component.name;
                        });

                    // update the port comments
                    port.select('path.component-comments')
                        .style(
                            'visibility',
                            self.nifiCommon.isBlank(portData.component.comments) ? 'hidden' : 'visible'
                        )
                        .attr(
                            'transform',
                            'translate(' +
                                (portData.dimensions.width - 2) +
                                ', ' +
                                (portData.dimensions.height - 10) +
                                ')'
                        )
                        .each(function (this: any) {
                            if (!self.nifiCommon.isBlank(portData.component.comments) && self.viewContainerRef) {
                                self.canvasUtils.canvasTooltip(self.viewContainerRef, TextTip, d3.select(this), {
                                    text: portData.component.comments
                                });
                            }
                        });
                } else {
                    // clear the port name
                    port.select('text.port-name').text(null);

                    // clear the port comments
                    port.select('path.component-comments').style('visibility', 'hidden');
                }

                // populate the stats
                self.updatePortStatus(port);

                // Update connections to update anchor point positions those may have been updated by changing ports remote accessibility.
                self.store.dispatch(
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
        const self: PortManager = this;

        // update the run status
        updated
            .select('text.run-status-icon')
            .attr('class', function (d: any) {
                let clazz = 'primary-color';

                if (d.status.aggregateSnapshot.runStatus === 'Invalid') {
                    clazz = 'invalid';
                } else if (d.status.aggregateSnapshot.runStatus === 'Running') {
                    clazz = 'running nifi-success-lighter';
                } else if (d.status.aggregateSnapshot.runStatus === 'Stopped') {
                    clazz = 'stopped nifi-warn-lighter';
                }

                return `run-status-icon ${clazz}`;
            })
            .attr('font-family', function (d: any) {
                let family = 'FontAwesome';
                if (d.status.aggregateSnapshot.runStatus === 'Disabled') {
                    family = 'flowfont';
                }
                return family;
            })
            .attr('y', this.offsetY(25))
            .text(function (d: any) {
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
            .each(function (this: any, d: any) {
                // if there are validation errors generate a tooltip
                if (
                    d.permissions.canRead &&
                    !self.nifiCommon.isEmpty(d.component.validationErrors) &&
                    self.viewContainerRef
                ) {
                    self.canvasUtils.canvasTooltip(self.viewContainerRef, ValidationErrorsTip, d3.select(this), {
                        isValidating: false,
                        validationErrors: d.component.validationErrors
                    });
                }
            });

        updated
            .select('text.port-transmission-icon')
            .attr('font-family', function (d: any) {
                if (d.status.transmitting === true) {
                    return 'FontAwesome';
                } else {
                    return 'flowfont';
                }
            })
            .text(function (d: any) {
                if (d.status.transmitting === true) {
                    return '\uf140';
                } else {
                    return '\ue80a';
                }
            })
            .classed('transmitting nifi-success-default', function (d: any) {
                return d.status.transmitting === true;
            })
            .classed('not-transmitting primary-color', function (d: any) {
                return d.status.transmitting !== true;
            });

        updated.each(function (this: any, d: any) {
            const port: any = d3.select(this);

            // -------------------
            // active thread count
            // -------------------

            self.canvasUtils.activeThreadCount(port, d);

            port.select('text.active-thread-count-icon').attr('y', self.offsetY(43));
            port.select('text.active-thread-count').attr('y', self.offsetY(43));

            // ---------
            // bulletins
            // ---------

            if (self.viewContainerRef) {
                self.canvasUtils.bulletins(self.viewContainerRef, port, d.bulletins);
            }
        });
    }

    private removePorts(removed: any) {
        removed.remove();
    }

    public init(viewContainerRef: ViewContainerRef): void {
        this.viewContainerRef = viewContainerRef;

        this.portContainer = d3.select('#canvas').append('g').attr('pointer-events', 'all').attr('class', 'ports');

        this.store
            .select(selectPorts)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((ports) => {
                this.set(ports);
            });

        this.store
            .select(selectFlowLoadingStatus)
            .pipe(
                filter((status) => status === 'success'),
                switchMap(() => this.store.select(selectAnySelectedComponentIds)),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe((selected) => {
                this.portContainer.selectAll('g.input-port, g.output-port').classed('selected', function (d: any) {
                    return selected.includes(d.id);
                });
            });

        this.store
            .select(selectTransitionRequired)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((transitionRequired) => {
                this.transitionRequired = transitionRequired;
            });
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
