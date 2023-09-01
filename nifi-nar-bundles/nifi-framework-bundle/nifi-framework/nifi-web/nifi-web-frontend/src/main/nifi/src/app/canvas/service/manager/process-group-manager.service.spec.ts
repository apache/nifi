import { TestBed } from '@angular/core/testing';

import { ProcessGroupManager } from './process-group-manager.service';

describe('ProcessGroupManager', () => {
    let service: ProcessGroupManager;

    beforeEach(() => {
        TestBed.configureTestingModule({});
        service = TestBed.inject(ProcessGroupManager);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });
});
