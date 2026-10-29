import { expect } from "chai";
import type { ValueProxy } from "@antelopejs/interface-database";
import {
  Schema,
  type SchemaDefinition,
} from "@antelopejs/interface-database/schema";

import * as connection from "../connection";
import { collectionName } from "../implementations/database/utils";

interface Category {
  _id: string;
  label: string;
  parent: string;
}

interface Product {
  _id: string;
  name: string;
  category: string;
  stock: number;
}

/** A product row once the computed fields and the relation are materialized. */
interface ProductView extends Omit<Product, "category"> {
  category: string | Category | null;
  family?: string | null;
  isLowStock?: boolean;
}

interface CatalogTables {
  categories: Category;
  products: ProductView;
}

type ProductRow = ValueProxy<ProductView>;

const SCHEMA_ID = "computed-relations";
const CATEGORIES = "categories";
const PRODUCTS = "products";
const TENANT = "tenant-one";
const LOW_STOCK_THRESHOLD = 5;

const CATEGORY_ROWS: Category[] = [
  { _id: "cat-a", label: "Category A", parent: "family-1" },
  { _id: "cat-b", label: "Category B", parent: "family-2" },
];

const PRODUCT_ROWS: Product[] = [
  { _id: "prod-1", name: "First", category: "cat-a", stock: 2 },
  { _id: "prod-2", name: "Second", category: "cat-b", stock: 10 },
  { _id: "prod-3", name: "Third", category: "cat-a", stock: 20 },
];

const definition: SchemaDefinition = {
  [CATEGORIES]: { fields: {}, indexes: {} },
  [PRODUCTS]: { fields: {}, indexes: { stock: {} } },
};

let schema: Schema<CatalogTables>;

async function createSchema(): Promise<Schema<CatalogTables>> {
  for (const table of [CATEGORIES, PRODUCTS]) {
    const collection = await connection.GetCollection(
      collectionName(SCHEMA_ID, table),
    );
    await collection.drop().catch(() => undefined);
  }
  await connection.InitializeSchema(SCHEMA_ID, definition);
  return new Schema<CatalogTables>(SCHEMA_ID, definition);
}

function categories() {
  return schema.instance(TENANT).table(CATEGORIES);
}

function products() {
  return schema.instance(TENANT).table(PRODUCTS);
}

/**
 * Mirrors how a data layer materializes computed fields before resolving
 * relations: a first map adds values read from another table (wrapping the
 * row), then a later map replaces the foreign key with the related row.
 */
function withComputed(row: ProductRow) {
  return row
    .merge({
      family: categories()
        .get(row.key("category").cast<string>())
        .key("parent"),
      isLowStock: row.key("stock").lt(LOW_STOCK_THRESHOLD),
    })
    .cast<ProductView>();
}

function withFamilyDefault(row: ProductRow) {
  return row
    .merge({ family: row.key("family").default(null) })
    .cast<ProductView>();
}

function withCategory(row: ProductRow) {
  return row
    .merge({
      category: categories()
        .get(row.key("category").cast<string>())
        .default(null),
    })
    .cast<ProductView>();
}

/** Rows read back carry the instance they belong to. */
function joined(category: Category) {
  return { ...category, _instance: TENANT };
}

function expectedRow(product: Product) {
  const category = joined(
    CATEGORY_ROWS.find((row) => row._id === product.category)!,
  );
  return {
    ...product,
    category,
    family: category?.parent,
    isLowStock: product.stock < LOW_STOCK_THRESHOLD,
  };
}

describe("Computed fields followed by a relation lookup", () => {
  before(async () => {
    schema = await createSchema();
    await categories().insert(CATEGORY_ROWS).run();
    await products().insert(PRODUCT_ROWS).run();
  });

  it("joins the relation on a single-row read", async () => {
    const row = await products()
      .get("prod-1")
      .do(withComputed)
      .do(withFamilyDefault)
      .do(withCategory)
      .run();

    expect(row).to.deep.include(expectedRow(PRODUCT_ROWS[0]));
  });

  it("joins the relation on a list read", async () => {
    const rows = await products()
      .map(withComputed)
      .map(withFamilyDefault)
      .map(withCategory)
      .orderBy("_id")
      .run();

    expect(rows).to.have.length(PRODUCT_ROWS.length);
    for (const [index, product] of PRODUCT_ROWS.entries()) {
      expect(rows[index]).to.deep.include(expectedRow(product));
    }
  });

  it("joins the relation with a lookup stage after the computed fields", async () => {
    const rows = await products()
      .map(withComputed)
      .lookup(categories(), "category", "_id")
      .orderBy("_id")
      .run();

    for (const [index, product] of PRODUCT_ROWS.entries()) {
      expect(rows[index]).to.deep.include(expectedRow(product));
    }
  });

  it("filters and sorts on a computed field before joining", async () => {
    const rows = await products()
      .map(withComputed)
      .filter((row) => row.key("family").eq("family-1"))
      .orderBy("stock", "desc")
      .map(withCategory)
      .run();

    expect(rows.map((row) => row._id)).to.deep.equal(["prod-3", "prod-1"]);
    expect(rows.map((row) => row.category)).to.deep.equal([
      joined(CATEGORY_ROWS[0]),
      joined(CATEGORY_ROWS[0]),
    ]);
  });

  it("counts and plucks rows carrying a computed field", async () => {
    const lowStock = products()
      .map(withComputed)
      .filter((row) => row.key("isLowStock"));

    expect(await lowStock.count().run()).to.equal(1);
    expect(
      await lowStock.map(withCategory).pluck("category").run(),
    ).to.deep.equal([{ category: joined(CATEGORY_ROWS[0]) }]);
  });
});
