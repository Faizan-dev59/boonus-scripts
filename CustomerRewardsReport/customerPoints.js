const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const collectionName = "customerloyalties"; // Replace with your collection name

const customerIds = [
  "60533c06c0348c002cacb3fe",
  "605390b52bfc6c002c5baed3",
  "5f586691054269002c9f97c1",
  "66f03720cbfbeb001d2f75e0",
];

async function extractCustomerData(customerIds) {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Aggregation pipeline to fetch data for multiple customerIds
    const pipeline = [
      {
        $match: {
          customerId: {
            $in: customerIds.map((id) => new ObjectId(id)),
          },
        },
      },
      {
        $project: {
          _id: 0,
          customerId: 1,
          totalPoints: 1,
          points: 1,
          businessId: 1, // Add businessId to the output
        },
      },
    ];

    // Execute aggregation
    const cursor = collection.aggregate(pipeline);
    const data = await cursor.toArray();

    if (data.length === 0) {
      console.log("No data found for the provided customer IDs.");
      return;
    }

    // Prepare data for CSV
    const fields = ["customerId", "businessId", "totalPoints", "points"];
    const json2csvParser = new Parser({ fields });
    const csv = json2csvParser.parse(data);

    // Write CSV to file
    const outputFile = `Customer-Points-Data.csv`;
    await fs.writeFile(outputFile, csv);
    console.log(`Customer data successfully written to ${outputFile}`);
  } catch (error) {
    console.error("Error while extracting customer data:", error);
  } finally {
    await client.close();
  }
}

// Execute the function
extractCustomerData(customerIds);
