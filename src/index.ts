import { MongoClientOptions } from 'mongodb';
import { ImplementInterface } from '@ajs/core/beta';
import { Connect, Disconnect } from './connection';
import { setIdProvider } from './implementations/database/beta';

export type IdProvider = 'objectid' | 'uuid';

export interface Options {
  url: string;
  id_provider?: IdProvider;
  options?: MongoClientOptions;
}

export async function construct(options: Options) {
  if (!options?.id_provider) {
    options.id_provider = 'uuid';
  }
  setIdProvider(options.id_provider);
  await Connect(options?.url, options?.options);

  ImplementInterface(await import('@ajs.local/database/beta/runtime'), await import('./implementations/database/beta'));
}

export async function destroy() {
  await Disconnect();
}
