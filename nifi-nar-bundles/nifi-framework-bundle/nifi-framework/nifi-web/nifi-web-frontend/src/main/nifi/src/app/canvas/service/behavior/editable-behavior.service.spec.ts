import { TestBed } from '@angular/core/testing';

import { EditableBehavior } from './editable-behavior.service';

describe('EditableBehaviorService', () => {
  let service: EditableBehavior;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(EditableBehavior);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
