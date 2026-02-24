/**
 * MongoDB Interface for Antelope
 *
 * This file defines the core MongoDB client interface used throughout the Antelope application.
 * It provides a centralized way to manage MongoDB connections using a singleton pattern
 * to ensure only one client connection is active at a time.
 */
import { MongoClient } from 'mongodb';
/**
 * Returns a promise that resolves to the MongoDB client
 *
 * This is the primary way for application code to access the MongoDB connection.
 * The returned promise will resolve once a connection has been established.
 *
 * @returns A promise that resolves to the MongoDB client instance
 */
export declare function GetClient(): Promise<MongoClient>;
