import { CreateDatabase, Database, DeleteDatabase, ListDatabases } from '@ajs/database/beta';
import { Logging } from '@ajs/logging/beta';

export function construct(config: unknown): void {
  // Set things up when module is loaded
  Logging.Debug('MongoDB module playground initialized with config:' + JSON.stringify(config));
}

export function destroy(): void {}

type Car = {
  brand: string;
  model: string;
  year: number;
};

const DB_NAME = 'antelope-playground';
const COLLECTION_NAME = 'cars';

export async function start(): Promise<void> {
  // Create database if it doesn't exist
  const databases = await ListDatabases();
  if (databases.includes(DB_NAME)) {
    await DeleteDatabase(DB_NAME);
  }
  await CreateDatabase(DB_NAME);

  // Create table if it doesn't exist
  const tables = await Database(DB_NAME).tableList();
  if (tables.includes(COLLECTION_NAME)) {
    await Database(DB_NAME).tableDrop(COLLECTION_NAME);
  }
  await Database(DB_NAME).tableCreate(COLLECTION_NAME);

  // Insert index
  const indexes = await Database(DB_NAME).table(COLLECTION_NAME).indexList();
  if (!indexes.includes('brand')) {
    await Database(DB_NAME).table(COLLECTION_NAME).indexCreate('brand');
  }

  // Insert data into table
  const cars_collection = Database(DB_NAME).table<Car>(COLLECTION_NAME);

  await cars_collection.insert({ brand: 'Bugatti', model: 'Chiron', year: 2020 });
  await cars_collection.insert({ brand: 'Porsche', model: '911', year: 2021 });
  await cars_collection.insert({ brand: 'Porsche', model: '718', year: 2022 });
  await cars_collection.insert({ brand: 'Porsche', model: 'Taycan', year: 2023 });
  await cars_collection.insert({ brand: 'McLaren', model: '720S', year: 2024 });

  await cars_collection.getAll('brand', 'McLaren').nth(0).update({ year: 2025 });

  const fastestCar = await cars_collection.getAll('brand', 'Bugatti').nth(0);

  const newestCar = await cars_collection.getAll('brand', 'Bugatti').orderBy('year', 'desc', true).nth(0);

  const averageYear = await cars_collection.avg('year');

  const totalCars = await cars_collection.count();

  const totalCarsByBrand = await cars_collection.group('brand', (stream, group) => {
    return {
      brand: group,
      count: stream.count(),
    };
  });

  Logging.Debug('[MONGODB] Fastest car model', fastestCar?.model);
  Logging.Debug('[MONGODB] Newest car model', newestCar?.model);
  Logging.Debug('[MONGODB] Average year', averageYear);
  Logging.Debug('[MONGODB] Total cars of all brands', totalCars);
  Logging.Debug('[MONGODB] Total cars by brand', JSON.stringify(totalCarsByBrand));
}

export function stop(): void {
  Database(DB_NAME).tableDrop(COLLECTION_NAME);
}
