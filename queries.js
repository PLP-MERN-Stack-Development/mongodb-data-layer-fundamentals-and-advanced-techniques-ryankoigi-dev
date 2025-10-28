// queries.js - MongoDB CRUD, Advanced Queries, Aggregation & Indexing

const { MongoClient } = require('mongodb');

// Connection URI (update if using MongoDB Atlas)
const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('✅ Connected to MongoDB');
    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // -------------------------------
    // 📘 TASK 2: BASIC CRUD OPERATIONS
    // -------------------------------

    console.log('\n--- BASIC CRUD OPERATIONS ---');

    // 1️⃣ Find all books in a specific genre
    const fictionBooks = await books.find({ genre: 'Fiction' }).toArray();
    console.log('\nBooks in Fiction genre:', fictionBooks);

    // 2️⃣ Find books published after a certain year
    const recentBooks = await books.find({ published_year: { $gt: 1950 } }).toArray();
    console.log('\nBooks published after 1950:', recentBooks);

    // 3️⃣ Find books by a specific author
    const orwellBooks = await books.find({ author: 'George Orwell' }).toArray();
    console.log('\nBooks by George Orwell:', orwellBooks);

    // 4️⃣ Update the price of a specific book
    const updated = await books.updateOne(
      { title: '1984' },
      { $set: { price: 13.99 } }
    );
    console.log(`\nUpdated ${updated.modifiedCount} book price for "1984"`);

    // 5️⃣ Delete a book by its title
    const deleted = await books.deleteOne({ title: 'Moby Dick' });
    console.log(`\nDeleted ${deleted.deletedCount} book(s) with title "Moby Dick"`);

    // -------------------------------
    // 📗 TASK 3: ADVANCED QUERIES
    // -------------------------------

    console.log('\n--- ADVANCED QUERIES ---');

    // 1️⃣ Find books that are in stock and published after 2010
    const inStockRecent = await books
      .find({ in_stock: true, published_year: { $gt: 2010 } })
      .project({ title: 1, author: 1, price: 1, _id: 0 })
      .toArray();
    console.log('\nIn-stock books published after 2010:', inStockRecent);

    // 2️⃣ Projection example (return only title, author, and price)
    const projectedBooks = await books
      .find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } })
      .toArray();
    console.log('\nProjected books (title, author, price):', projectedBooks);

    // 3️⃣ Sorting (price ascending)
    const sortedAsc = await books.find().sort({ price: 1 }).toArray();
    console.log('\nBooks sorted by price (ascending):', sortedAsc);

    // 4️⃣ Sorting (price descending)
    const sortedDesc = await books.find().sort({ price: -1 }).toArray();
    console.log('\nBooks sorted by price (descending):', sortedDesc);

    // 5️⃣ Pagination (5 books per page)
    const page = 1; // Change this for other pages (1-based)
    const booksPerPage = 5;
    const paginatedBooks = await books
      .find()
      .skip((page - 1) * booksPerPage)
      .limit(booksPerPage)
      .toArray();
    console.log(`\nBooks - Page ${page}:`, paginatedBooks);

    // -------------------------------
    // 📙 TASK 4: AGGREGATION PIPELINE
    // -------------------------------

    console.log('\n--- AGGREGATION PIPELINES ---');

    // 1️⃣ Average price by genre
    const avgPriceByGenre = await books.aggregate([
      { $group: { _id: '$genre', averagePrice: { $avg: '$price' } } },
      { $sort: { averagePrice: -1 } }
    ]).toArray();
    console.log('\nAverage price by genre:', avgPriceByGenre);

    // 2️⃣ Author with the most books
    const topAuthor = await books.aggregate([
      { $group: { _id: '$author', bookCount: { $sum: 1 } } },
      { $sort: { bookCount: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log('\nAuthor with the most books:', topAuthor);

    // 3️⃣ Group books by publication decade and count them
    const booksByDecade = await books.aggregate([
      {
        $project: {
          decade: {
            $concat: [
              { $toString: { $multiply: [{ $floor: { $divide: ['$published_year', 10] } }, 10] } },
              's'
            ]
          }
        }
      },
      { $group: { _id: '$decade', count: { $sum: 1 } } },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log('\nBooks grouped by decade:', booksByDecade);

    // -------------------------------
    // 📒 TASK 5: INDEXING
    // -------------------------------

    console.log('\n--- INDEXING ---');

    // 1️⃣ Create index on title field
    const titleIndex = await books.createIndex({ title: 1 });
    console.log('\nCreated index on title:', titleIndex);

    // 2️⃣ Create compound index on author and published_year
    const compoundIndex = await books.createIndex({ author: 1, published_year: -1 });
    console.log('Created compound index on author and published_year:', compoundIndex);

    // 3️⃣ Use explain() to show performance improvement
    const explainBefore = await books.find({ title: '1984' }).explain('executionStats');
    console.log('\nExplain (query plan) for title search:', explainBefore.executionStats);

  } catch (err) {
    console.error('❌ Error:', err);
  } finally {
    await client.close();
    console.log('\n🔒 Connection closed');
  }
}

runQueries();
