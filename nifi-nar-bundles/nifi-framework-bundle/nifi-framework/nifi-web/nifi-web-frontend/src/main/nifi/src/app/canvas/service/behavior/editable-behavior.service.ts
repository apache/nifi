import { Injectable } from '@angular/core';
import { DraggableBehavior } from './draggable-behavior.service';
import { CanvasUtils } from '../canvas-utils.service';

@Injectable({
    providedIn: 'root'
})
export class EditableBehavior {
    constructor(
        private draggableBehavior: DraggableBehavior,
        private canvasUtils: CanvasUtils
    ) {}

    public editable(selection: any): void {
        if (this.canvasUtils.canModify(selection)) {
            // if (!selection.classed('connectable')) {
            //   selection.call(nfConnectableRef.activate);
            // }
            if (!selection.classed('moveable')) {
                this.draggableBehavior.activate(selection);
            }
        } else {
            // if (selection.classed('connectable')) {
            //   selection.call(nfConnectableRef.deactivate);
            // }
            if (selection.classed('moveable')) {
                this.draggableBehavior.deactivate(selection);
            }
        }
    }
}
