// aggregations.js - MongoDB Aggregation Pipelines for plp_bookstore

const { MongoClient } = require('mongodb');

// Connection URI (update if using Atlas)
const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runAggregations() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('✅ Connected to MongoDB for Aggregation Pipelines');

    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // -------------------------------
    // 📊 TASK 4: AGGREGATION PIPELINES
    // -------------------------------

    console.log('\n--- AGGREGATION PIPELINES ---');

    // 1️⃣ Calculate average price of books by genre
    const avgPriceByGenre = await books.aggregate([
      { $group: { _id: '$genre', averagePrice: { $avg: '$price' } } },
      { $sort: { averagePrice: -1 } }
    ]).toArray();
    console.log('\n📘 Average price by genre:');
    console.table(avgPriceByGenre);

    // 2️⃣ Find the author with the most books
    const mostBooksByAuthor = await books.aggregate([
      { $group: { _id: '$author', totalBooks: { $sum: 1 } } },
      { $sort: { totalBooks: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log('\n📗 Author with the most books:');
    console.table(mostBooksByAuthor);

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
    console.log('\n📙 Number of books grouped by decade:');
    console.table(booksByDecade);

    // -------------------------------
    // 🔍 BONUS AGGREGATIONS
    // -------------------------------

    // 4️⃣ Calculate total pages by genre
    const totalPagesByGenre = await books.aggregate([
      { $group: { _id: '$genre', totalPages: { $sum: '$pages' } } },
      { $sort: { totalPages: -1 } }
    ]).toArray();
    console.log('\n📘 Total pages by genre:');
    console.table(totalPagesByGenre);

    // 5️⃣ Find top 3 publishers with the most books
    const topPublishers = await books.aggregate([
      { $group: { _id: '$publisher', totalBooks: { $sum: 1 } } },
      { $sort: { totalBooks: -1 } },
      { $limit: 3 }
    ]).toArray();
    console.log('\n📕 Top 3 publishers with most books:');
    console.table(topPublishers);

    // 6️⃣ Categorize books by price range (budget, mid, premium)
    const booksByPriceRange = await books.aggregate([
      {
        $bucket: {
          groupBy: '$price',
          boundaries: [0, 10, 15, 25],
          default: 'Above $25',
          output: {
            totalBooks: { $sum: 1 },
            avgPrice: { $avg: '$price' },
            books: { $push: '$title' }
          }
        }
      }
    ]).toArray();
    console.log('\n💰 Books categorized by price range:');
    console.table(booksByPriceRange);

    // 7️⃣ Average pages per author
    const avgPagesByAuthor = await books.aggregate([
      { $group: { _id: '$author', averagePages: { $avg: '$pages' } } },
      { $sort: { averagePages: -1 } }
    ]).toArray();
    console.log('\n📘 Average pages per author:');
    console.table(avgPagesByAuthor);

    // 8️⃣ Count how many books are in stock vs out of stock
    const stockStatus = await books.aggregate([
      {
        $group: {
          _id: '$in_stock',
          total: { $sum: 1 }
        }
      }
    ]).toArray();
    console.log('\n📦 Stock status (true = in stock, false = out of stock):');
    console.table(stockStatus);

  } catch (err) {
    console.error('❌ Error during aggregation:', err);
  } finally {
    await client.close();
    console.log('\n🔒 MongoDB connection closed');
  }
}

runAggregations();
