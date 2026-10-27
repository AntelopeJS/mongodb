import { GetRuntimeInfo } from "@antelopejs/interface-core/runtime";
import type {
  CatalogInfo,
  Measure,
  ProfileRequest,
  ReferenceCheck,
  ReferenceMatch,
  RelationDescription,
  RelationProfile,
  RelationSummary,
  RowSample,
  SampleRequest,
} from "@antelopejs/interface-database-catalog";

import { GetOptions, IsExternal } from "../../config";
import { GetDatabase } from "../../connection";
import {
  DescribeRelation,
  ReadInfo,
  ReadRelations,
  RelationName,
  Summarize,
} from "./discovery";
import { Sequentially, Serialized } from "./measures";
import { ProfileRelation } from "./profile";
import { CheckColumnReference } from "./reference";
import { SampleRelationRows } from "./rows";

const OWNED_REFUSAL =
  "the catalog describes a database an external instance was pointed at; this instance owns its collections";
const DEVELOPMENT_REFUSAL =
  "the catalog is a development tool: it answers under ajs project dev or run, or with catalog.enabled";

async function IsEnabled(): Promise<boolean> {
  const configured = GetOptions().catalog.enabled;
  return configured ?? (await GetRuntimeInfo()).dev;
}

/** Refuses every catalog call on an owned instance, and outside development unless enabled. */
async function Guarded<T>(work: () => Promise<T>): Promise<T> {
  if (!IsExternal()) {
    throw new Error(OWNED_REFUSAL);
  }
  if (!(await IsEnabled())) {
    throw new Error(DEVELOPMENT_REFUSAL);
  }
  return Serialized(work);
}

export function Info(): Promise<CatalogInfo> {
  return Guarded(ReadInfo);
}

export function ListRelations(schemas?: string[]): Promise<RelationSummary[]> {
  return Guarded(async () => {
    const db = await GetDatabase();
    if (schemas && !schemas.includes(db.databaseName)) {
      return [];
    }
    return Sequentially(await ReadRelations(), Summarize);
  });
}

export function DescribeRelations(
  relations: string[],
): Promise<RelationDescription[]> {
  return Guarded(async () => {
    const db = await GetDatabase();
    const names = relations.map((relation) =>
      relation.startsWith(`${db.databaseName}.`)
        ? relation.slice(db.databaseName.length + 1)
        : "",
    );
    const listed = await ReadRelations(names);
    const found = new Set(listed.map(RelationName));
    const missing = relations.filter((relation) => !found.has(relation));
    if (missing.length > 0) {
      throw new Error(
        `No relation ${missing.join(", ")} that the connected account can list`,
      );
    }
    const ordered = relations.flatMap((relation) =>
      listed.filter((entry) => RelationName(entry) === relation),
    );
    return Sequentially(ordered, DescribeRelation);
  });
}

export function Profile(request: ProfileRequest): Promise<RelationProfile> {
  return Guarded(() => ProfileRelation(request));
}

export function CheckReference(
  check: ReferenceCheck,
): Promise<Measure<ReferenceMatch>> {
  return Guarded(() => CheckColumnReference(check));
}

export function SampleRows(request: SampleRequest): Promise<RowSample> {
  return Guarded(() => SampleRelationRows(request));
}
