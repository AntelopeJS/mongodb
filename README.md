![MongoDB](.github/social-card.png)

# @antelopejs/mongodb

[![npm version](https://img.shields.io/npm/v/@antelopejs/mongodb.svg)](https://www.npmjs.com/package/@antelopejs/mongodb)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0-blue)](https://www.typescriptlang.org/)

A full-featured MongoDB client module that implements both the MongoDB interface and the Database interface for AntelopeJS.

For detailed documentation on the MongoDB interfaces, please refer to:

- [MongoDB Interface](https://github.com/AntelopeJS/interface-mongodb)
- [Database Interface](https://github.com/AntelopeJS/interface-database)

## Installation

```bash
ajs project modules add @antelopejs/mongodb
```

## Overview

The AntelopeJS MongoDB module provides functionality for interacting with MongoDB:

- MongoDB client connection management through the MongoDB interface
- Common database operations through the Database interface

## Configuration

The MongoDB module supports connection using the native MongoDB driver with the following options:

```typescript
// MongoDB connection options
{
    url: "mongodb://localhost:27017",     // The MongoDB connection string
    id_provider: "uuid",                  // ID generation strategy: "uuid" (default) or "objectid"
    options: {                            // Optional MongoDB client options
        useNewUrlParser: true,
        useUnifiedTopology: true,
        maxPoolSize: 10,                  // Maximum number of connections in the pool
        connectTimeoutMS: 30000,          // Connection timeout in milliseconds
        socketTimeoutMS: 30000            // Socket timeout in milliseconds
    }
}
```

### Configuration Details

The module uses the official MongoDB Node.js driver to establish connections to your MongoDB servers:

- Connection using `MongoClient.connect()` from the mongodb package
- Support for standard MongoDB connection options
- Built-in connection pooling through the MongoDB driver
- ID generation strategies:
  - `uuid` (default): Uses UUID v4 for generating unique identifiers
  - `objectid`: Uses MongoDB's native ObjectId for document identifiers

## License

Apache-2.0
