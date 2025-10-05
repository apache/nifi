// LoadingService migrated from Angular with RxJS
import { BehaviorSubject, Observable } from 'rxjs';
import { delay } from 'rxjs/operators';

export class LoadingService {
  private loading: BehaviorSubject<boolean> = new BehaviorSubject<boolean>(false);
  private requests: Map<string, boolean> = new Map<string, boolean>();
  public status$: Observable<boolean>;

  constructor() {
    this.status$ = this.loading.asObservable().pipe(delay(0));
  }

  set(loading: boolean, url: string): void {
    if (loading) {
      this.requests.set(url, loading);
      this.loading.next(true);
      return;
    }
    this.requests.delete(url);
    this.loading.next(this.requests.size > 0);
  }
}
