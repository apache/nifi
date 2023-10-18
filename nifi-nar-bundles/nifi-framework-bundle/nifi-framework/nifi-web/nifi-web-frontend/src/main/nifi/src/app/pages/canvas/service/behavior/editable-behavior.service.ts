import { Injectable } from '@angular/core';
import { DraggableBehavior } from './draggable-behavior.service';
import { CanvasUtils } from '../canvas-utils.service';
import { ConnectableBehavior } from './connectable-behavior.service';

@Injectable({
    providedIn: 'root'
})
export class EditableBehavior {
    constructor(
        private draggableBehavior: DraggableBehavior,
        private connectableBehavior: ConnectableBehavior,
        private canvasUtils: CanvasUtils
    ) {}

    public editable(selection: any): void {
        if (this.canvasUtils.canModify(selection)) {
            if (!selection.classed('connectable')) {
                this.connectableBehavior.activate(selection);
            }
            if (!selection.classed('moveable')) {
                this.draggableBehavior.activate(selection);
            }
        } else {
            if (selection.classed('connectable')) {
                this.connectableBehavior.deactivate(selection);
            }
            if (selection.classed('moveable')) {
                this.draggableBehavior.deactivate(selection);
            }
        }
    }
}
