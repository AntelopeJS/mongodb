import { CROSS_INSTANCE } from "@antelopejs/interface-database/schema";
import { GetCollection } from "../../connection";
import { GetTableNames } from "./schema";
import {
  BOOKKEEPING_COLLECTION,
  collectionName,
  INSTANCE_FIELD,
  normalizeInstanceId,
} from "./utils";

function rejectCrossInstance(action: string, value: unknown) {
  if (value === CROSS_INSTANCE) {
    throw new Error(
      `${action} does not accept CROSS_INSTANCE: pass a string id or omit for the default instance`,
    );
  }
}

export async function CreateInstance(
  schemaId: string,
  id: unknown,
): Promise<string> {
  rejectCrossInstance("createInstance", id);
  const instanceId = normalizeInstanceId(id);
  const collection = await GetCollection(BOOKKEEPING_COLLECTION);
  await collection.updateOne(
    { schemaId, instanceId },
    { $setOnInsert: { schemaId, instanceId, createdAt: new Date() } },
    { upsert: true },
  );
  return instanceId ?? "";
}

export async function DestroyInstance(
  schemaId: string,
  id: unknown,
): Promise<void> {
  rejectCrossInstance("destroyInstance", id);
  const instanceId = normalizeInstanceId(id);
  for (const tableName of GetTableNames(schemaId)) {
    const collection = await GetCollection(collectionName(schemaId, tableName));
    await collection.deleteMany({ [INSTANCE_FIELD]: instanceId });
  }
  const bookkeeping = await GetCollection(BOOKKEEPING_COLLECTION);
  await bookkeeping.deleteOne({ schemaId, instanceId });
}

export async function ListInstances(schemaId: string): Promise<string[]> {
  const collection = await GetCollection(BOOKKEEPING_COLLECTION);
  const rows = await collection
    .find({ schemaId, instanceId: { $ne: null } })
    .project({ instanceId: 1 })
    .toArray();
  return rows.map((row) => row.instanceId as string);
}
