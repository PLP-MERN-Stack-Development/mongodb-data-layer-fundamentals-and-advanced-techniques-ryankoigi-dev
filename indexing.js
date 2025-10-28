// indexing.js - MongoDB Indexing for plp_bookstore

const { MongoClient } = require('mongodb');

// MongoDB connection
const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runIndexing() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('✅ Connected to MongoDB for Indexing Demo');

    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // -------------------------------
    // 🧩 1. CREATE BASIC INDEXES
    // -------------------------------

    console.log('\n📘 Creating basic indexes...');

    // Index on title for faster text-based searches
    await books.createIndex({ title: 1 });
    console.log('✅ Index created on title field');

    // Index on author to speed up author-based queries
    await books.createIndex({ author: 1 });
    console.log('✅ Index created on author field');

    // Compound index on genre and price for multi-field filters
    await books.createIndex({ genre: 1, price: -1 });
    console.log('✅ Compound index created on genre and price');

    // Text index for full-text search (title, author, publisher)
    await books.createIndex({ title: "text", author: "text", publisher: "text" });
    console.log('✅ Text index created on title, author, and publisher');

    // -------------------------------
    // ⚙️ 2. VIEW EXISTING INDEXES
    // -------------------------------
    const indexes = await books.indexes();
    console.log('\n📋 Current indexes in collection:');
    console.table(indexes);

    // -------------------------------
    // 🚀 3. TEST INDEX PERFORMANCE
    // -------------------------------
    console.log('\n🔍 Testing indexed queries...');

    console.time('Query without index');
    await books.find({ genre: 'Fiction' }).toArray();
    console.timeEnd('Query without index');

    console.time('Query using index');
    await books.find({ genre: 'Fiction', price: { $lt: 20 } }).hint({ genre: 1, price: -1 }).toArray();
    console.timeEnd('Query using index');

    // -------------------------------
    // 🔎 4. TEXT SEARCH USING INDEX
    // -------------------------------
    console.log('\n🔎 Performing full-text search for "Python"');
    const searchResults = await books.find({ $text: { $search: "Python" } }).toArray();
    console.table(searchResults);

    // -------------------------------
    // 🧹 5. REMOVE AN INDEX (OPTIONAL)
    // -------------------------------
    // Example: await books.dropIndex("author_1");
    // console.log('❌ Dropped index on author');

  } catch (err) {
    console.error('❌ Error during indexing demo:', err);
  } finally {
    await client.close();
    console.log('\n🔒 MongoDB connection closed');
  }
}

runIndexing();
