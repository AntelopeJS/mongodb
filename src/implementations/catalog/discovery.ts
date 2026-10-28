import type {
  CatalogInfo,
  RelationDescription,
  RelationKey,
  RelationSummary,
  RelationWrites,
} from "@antelopejs/interface-database-catalog";
import type { Db, Document } from "mongodb";

import { GetDatabase } from "../../connection";
import {
  type KeyFinding as Finding,
  KeyOf,
} from "../database/external-descriptor";
import {
  KindOf,
  ReadEnds,
  type ValidatorSchema,
} from "../database/external-probe";
import { DescribeColumns } from "./columns";
import { EstimatedRows, MeasuredCollection } from "./measures";

const SYSTEM_PREFIX = "system.";
const WRITE_ACTIONS: Record<keyof RelationWrites, string> = {
  insert: "insert",
  update: "update",
  delete: "remove",
};

interface Privilege {
  resource: { db?: string; collection?: string; anyResource?: boolean };
  actions: string[];
}

/** A collection or view of the configured database, as `listCollections` answers it. */
export interface ListedRelation {
  db: Db;
  info: Document;
  privileges: Privilege[];
}

export async function ReadInfo(): Promise<CatalogInfo> {
  const db = await GetDatabase();
  const build = await db.admin().command({ buildInfo: 1 });
  const status = await db.command({ connectionStatus: 1 });
  const [user] = status.authInfo?.authenticatedUsers ?? [];
  return {
    engine: "mongodb",
    version: String(build.version),
    database: db.databaseName,
    role: user?.user ?? "",
  };
}

async function Privileges(db: Db): Promise<Privilege[]> {
  const status = await db.command({
    connectionStatus: 1,
    showPrivileges: true,
  });
  return status.authInfo?.authenticatedUserPrivileges ?? [];
}

/** Every collection and view the connection lists, `system.*` left out, in name order. */
export async function ReadRelations(
  names?: string[],
): Promise<ListedRelation[]> {
  const db = await GetDatabase();
  const filter = names ? { name: { $in: names } } : {};
  const infos = await db.listCollections(filter, { nameOnly: false }).toArray();
  const privileges = await Privileges(db);
  return infos
    .filter((info) => !info.name.startsWith(SYSTEM_PREFIX))
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((info) => ({ db, info, privileges }));
}

export function RelationName(relation: ListedRelation): string {
  return `${relation.db.databaseName}.${relation.info.name}`;
}

function Allows(relation: ListedRelation, action: string): boolean {
  const { databaseName } = relation.db;
  return relation.privileges.some(
    (privilege) =>
      privilege.actions.includes(action) &&
      (privilege.resource.anyResource === true ||
        ((privilege.resource.db === databaseName ||
          privilege.resource.db === "") &&
          (privilege.resource.collection === relation.info.name ||
            privilege.resource.collection === ""))),
  );
}

function Writes(relation: ListedRelation): RelationWrites {
  const isView = KindOf(relation.info) === "view";
  return {
    insert: !isView && Allows(relation, WRITE_ACTIONS.insert),
    update: !isView && Allows(relation, WRITE_ACTIONS.update),
    delete: !isView && Allows(relation, WRITE_ACTIONS.delete),
  };
}

function ValidatorOf(relation: ListedRelation): ValidatorSchema | undefined {
  return relation.info.options?.validator?.$jsonSchema;
}

/** The key type class of a listed relation, or why it has none. */
export async function KeyFinding(relation: ListedRelation): Promise<Finding> {
  const kind = KindOf(relation.info);
  const ends =
    kind === "collection"
      ? await ReadEnds(relation.db.collection(relation.info.name))
      : undefined;
  return KeyOf({ kind, ends });
}

const PRIMARY_KEY: RelationKey = { columns: ["_id"], source: "primary" };

/**
 * One relation's summary: a view is a `view`, and a collection, time-series included, a
 * `table`; its key is `_id` when both ends of the `_id` index share one type class.
 */
export async function Summarize(
  relation: ListedRelation,
): Promise<RelationSummary> {
  const kind = KindOf(relation.info);
  const finding = await KeyFinding(relation);
  const estimatedRows =
    kind === "view"
      ? null
      : await EstimatedRows(await MeasuredCollection(relation.info.name));
  return {
    schema: relation.db.databaseName,
    name: relation.info.name,
    kind: kind === "view" ? "view" : "table",
    estimatedRows,
    estimate: estimatedRows === null ? null : "statistics",
    key: finding.key ? PRIMARY_KEY : null,
    writes: Writes(relation),
    comment: ValidatorOf(relation)?.description ?? null,
  };
}

async function Indexes(relation: ListedRelation) {
  if (KindOf(relation.info) === "view") {
    return [];
  }
  const indexes = await relation.db.collection(relation.info.name).indexes();
  return indexes.map((index) => ({
    name: index.name ?? "",
    columns: Object.keys(index.key),
    unique: index.unique === true,
  }));
}

/** One relation in full: its summary, its fields from the validator and a sample, its indexes. */
export async function DescribeRelation(
  relation: ListedRelation,
): Promise<RelationDescription> {
  const summary = await Summarize(relation);
  const finding = await KeyFinding(relation);
  return {
    ...summary,
    columns: await DescribeColumns(
      relation.info.name,
      ValidatorOf(relation),
      finding,
    ),
    indexes: await Indexes(relation),
    references: [],
    referencedBy: [],
  };
}
