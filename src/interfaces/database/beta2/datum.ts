import { Value } from './common';
import { Query } from './query';

export class Datum<T> extends Query<T> {
  // do

  // TODO: Better name?
  public key<K extends keyof T, U = undefined>(key: K, def?: U) {
    return this.stage(
      Datum<U extends undefined ? T[K] : Exclude<T[K], undefined | null> | U>,
      'key',
      undefined,
      key,
      def,
    );
  }

  public default<U>(val: Value<U>) {
    return this.stage(Datum<Exclude<T, undefined | null> | U>, 'default', undefined, val);
  }
  // lookup
  // append
  // prepend
  // pluck
  // value
}
