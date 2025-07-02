import { CreateDatabase, Database, DeleteDatabase, ListDatabases } from "@ajs/database/beta"
import { expect } from "chai";

describe("Main", () => {
	it("tableCreate", async () => {
		await Database("mydb").tableCreate("mytable");
	});
	it("insert", async () => {
		await Database("mydb").table("mytable").insert({ Hello: 1 } as any);
	});
	it("gettable", async () => {
		console.log(await Database("mydb").table("mytable"));
	});
})

const db = Database("test-main");

describe("Schema", () => {
	it("Create database", async () => {
		await CreateDatabase("test-main").run();
		await CreateDatabase("test-to-delete").run();
	});

	it("List Databases", async () => {
		expect(await ListDatabases().run()).to.have.members(["test-main", "test-to-delete"]);
	})

	it("Delete Database", async () => {
		await DeleteDatabase("test-to-delete").run();
		expect(await ListDatabases().run()).to.not.have.members(["test-to-delete"]);
	});

	const tables = ["items", "categories", "users", "orders", "to-delete"];
	it("Create Table", async () => {
		for (const name of tables) {
			await db.tableCreate(name).run();
		}
	});

	it("List Tables", async () => {
		expect(await db.tableList().run()).to.have.members(tables);
	});

	it("Delete Table", async () => {
		await db.tableDrop("to-delete").run();
		expect(await db.tableList().run()).to.not.have.members(["to-delete"]);
	});

	const indexesMap = {
		"items": ["serial", "category", "to-delete"],
		"orders": ["user"],
		"users": ["email"]
	}
	it("Create Index", async () => {
		for (const [table, indexes] of Object.entries(indexesMap)) {
			for (const index of indexes) {
				await db.table(table).indexCreate(index, index).run();
			}
		}
	});

	it("List Indexes", async () => {
		for (const [table, indexes] of Object.entries(indexesMap)) {
			expect(await db.table(table).indexList().run()).to.include.members(indexes);
		}
	});

	it("Delete Index", async () => {
		await db.table("items").indexDrop("to-delete").run();
		expect(await db.table("items").indexList()).to.not.have.members(["to-delete"]);
	});
});

describe("Basic Operations", () => {
	it("Insert", async() => {

	});

	it("Get", async() => {

	});

	it("Get All", async() => {

	});

	it("Get By Index", async() => {

	});

	it("Update", async() => {

	});

	it("Replace", async() => {

	});

	it("Delete", async() => {

	});

});

/*

structure:
	create database
	list databases
	delete database

	create table
	list tables
	delete table

	create index
	list indexes
	delete index

basic:
	insert
	get
	get all
	get by index
	update
	replace
	delete



*/
