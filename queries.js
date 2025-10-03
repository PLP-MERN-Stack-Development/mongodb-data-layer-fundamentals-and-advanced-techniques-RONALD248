// queries.js - MongoDB Queries and Operations

const { MongoClient } = require("mongodb");
const uri = "mongodb://localhost:27017"; // change if using Atlas
const dbName = "plp_bookstore";
const collectionName = "books";

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // ------------------------------
    // Task 2: Basic CRUD Operations
    // ------------------------------

    // 1. Find all books in a specific genre (e.g., Fiction)
    console.log("\nBooks in Fiction genre:");
    console.log(await books.find({ genre: "Fiction" }).toArray());

    // 2. Find books published after a certain year (e.g., after 1950)
    console.log("\nBooks published after 1950:");
    console.log(await books.find({ published_year: { $gt: 1950 } }).toArray());

    // 3. Find books by a specific author (George Orwell)
    console.log("\nBooks by George Orwell:");
    console.log(await books.find({ author: "George Orwell" }).toArray());

    // 4. Update the price of a specific book (The Hobbit → 17.99)
    await books.updateOne({ title: "The Hobbit" }, { $set: { price: 17.99 } });
    console.log("\nUpdated The Hobbit’s price:");

    // 5. Delete a book by its title (Moby Dick)
    await books.deleteOne({ title: "Moby Dick" });
    console.log("\nDeleted 'Moby Dick'");

    // ------------------------------
    // Task 3: Advanced Queries
    // ------------------------------

    // 1. Books in stock and published after 2010
    console.log("\nBooks in stock and published after 2010:");
    console.log(
      await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray()
    );

    // 2. Projection (title, author, price only)
    console.log("\nBooks with selected fields:");
    console.log(
      await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray()
    );

    // 3. Sorting by price ascending
    console.log("\nBooks sorted by price ascending:");
    console.log(await books.find().sort({ price: 1 }).toArray());

    // 4. Sorting by price descending
    console.log("\nBooks sorted by price descending:");
    console.log(await books.find().sort({ price: -1 }).toArray());

    // 5. Pagination (5 books per page, page 1)
    console.log("\nPage 1 (first 5 books):");
    console.log(await books.find().limit(5).toArray());

    console.log("\nPage 2 (next 5 books):");
    console.log(await books.find().skip(5).limit(5).toArray());

    // ------------------------------
    // Task 4: Aggregation Pipelines
    // ------------------------------

    // 1. Average price of books by genre
    console.log("\nAverage price by genre:");
    console.log(
      await books.aggregate([
        { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
      ]).toArray()
    );

    // 2. Author with the most books
    console.log("\nAuthor with the most books:");
    console.log(
      await books.aggregate([
        { $group: { _id: "$author", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 1 }
      ]).toArray()
    );

    // 3. Books grouped by decade
    console.log("\nBooks grouped by decade:");
    console.log(
      await books.aggregate([
        {
          $group: {
            _id: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] },
            count: { $sum: 1 }
          }
        },
        { $sort: { _id: 1 } }
      ]).toArray()
    );

    // ------------------------------
    // Task 5: Indexing
    // ------------------------------

    // 1. Index on title
    await books.createIndex({ title: 1 });
    console.log("\nCreated index on 'title'");

    // 2. Compound index on author + published_year
    await books.createIndex({ author: 1, published_year: -1 });
    console.log("\nCreated compound index on 'author + published_year'");

    // 3. Use explain() to show performance
    console.log("\nExplain plan for title search:");
    console.log(await books.find({ title: "1984" }).explain("executionStats"));

  } finally {
    await client.close();
    console.log("\nConnection closed.");
  }
}

runQueries().catch(console.error);
