/**
 * MongoDB Interface for Antelope
 *
 * This file defines the core MongoDB client interface used throughout the Antelope application.
 * It provides a centralized way to manage MongoDB connections using a singleton pattern
 * to ensure only one client connection is active at a time.
 */

import { MongoClient } from 'mongodb';

/**
 * Internal namespace for MongoDB client management
 *
 * Contains implementation details that should not be directly accessed by application code.
 * This namespace handles the MongoDB client connection lifecycle and state.
 */
export namespace internal {
  /**
   * Promise that resolves to the MongoDB client instance once connected
   * Acts as a singleton access point to the database connection
   */
  export let client: Promise<MongoClient>;

  /**
   * Connection state flag
   * Indicates whether the MongoDB client is currently connected
   */
  export let connected = false;

  /**
   * Client setter function
   * Used internally to resolve the client promise when a connection is established
   *
   * @param client The MongoDB client instance to set
   */
  export let SetClient: (client: MongoClient) => void;

  /**
   * Resets the client promise and prepares for a new connection
   * Used when disconnecting or when needing to establish a fresh connection
   *
   * @returns A new unresolved client promise
   */
  export const UnsetClient = () => (client = new Promise((resolve) => (SetClient = resolve)));

  // Initialize the client promise on module load
  UnsetClient();
}

/**
 * Returns a promise that resolves to the MongoDB client
 *
 * This is the primary way for application code to access the MongoDB connection.
 * The returned promise will resolve once a connection has been established.
 *
 * @returns A promise that resolves to the MongoDB client instance
 */
export function GetClient(): Promise<MongoClient> {
  return internal.client;
}
