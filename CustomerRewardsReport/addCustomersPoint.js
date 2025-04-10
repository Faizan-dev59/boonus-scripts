const fs = require("fs");
const { MongoClient, ObjectId } = require("mongodb");
const csvParser = require("csv-parser");
require("dotenv").config();

// MongoDB configuration
const MONGO_URI = process.env.DATABASE_URI; // Replace with your MongoDB URI
const DB_NAME = "boonus";
const COLLECTION_NAME = "customerloyalties";

const BATCH_SIZE = 100; // Batch size for updates
const CSV_FILE_PATH = "./Point-Prod.csv"; // Path to your CSV file

const main = async () => {
  const client = new MongoClient(MONGO_URI);
  let updates = [];
  let batchCount = 0;

  try {
    console.log("Connecting to MongoDB...");
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    console.log("Reading CSV file...");
    const stream = fs.createReadStream(CSV_FILE_PATH).pipe(csvParser());

    for await (const row of stream) {
      const { countryCode, number, "Points to be added ": pointsToAdd } = row;

      // Default to 0 if Points to be added is empty or invalid
      const points = parseInt(pointsToAdd) || 0;

      // Prepare update query
      updates.push({
        updateOne: {
          filter: {
            businessId: new ObjectId("65a3ee2199959a001ccd9bd2"),
            loyaltyId: new ObjectId("65a3ee2199959a001ccd9bd3"),
            "phone.countryCode": countryCode.trim(),
            "phone.number": number.trim(),
          },
          update: {
            $inc: { points: points, totalPoints: points },
          },
        },
      });

      // Execute batch when it reaches the defined size
      if (updates.length >= BATCH_SIZE) {
        await processBatch(collection, updates, ++batchCount);
        updates = [];
      }
    }

    // Process remaining updates
    if (updates.length > 0) {
      await processBatch(collection, updates, ++batchCount);
    }

    console.log("Batch update completed.");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    // Ensure client is closed only after all operations
    await client.close();
    console.log("MongoDB connection closed.");
  }
};

// Function to process and execute batch updates
const processBatch = async (collection, updates, batchNumber) => {
  if (updates.length === 0) return;

  try {
    const result = await collection.bulkWrite(updates, { ordered: false });
    console.log(
      `Batch ${batchNumber} processed: ${result.matchedCount} documents matched, ${result.modifiedCount} updated.`
    );
  } catch (err) {
    console.error(`Error processing batch ${batchNumber}:`, err);
  }
};

main();
