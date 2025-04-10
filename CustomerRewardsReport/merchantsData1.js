const { createWriteStream } = require("fs");
const { parse } = require("json2csv");
const { MongoClient } = require("mongodb");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);

async function main() {
  try {
    console.log("Connected to MongoDB.");

    // Access the collection directly
    const db = client.db("boonus");
    const businessesCollection = db.collection("businesses"); // Replace 'businesses' with your actual collection name

    // Define the fields to be included in the CSV
    const fields = ["Merchant Name", "Integration Type"];
    const csvOptions = { fields };

    // Query for businesses with the required conditions using aggregate
    const businesses = await businessesCollection
      .aggregate([
        {
          $match: {
            "integrations.integrationName": "foodics",
            "integrations.active": true,
            metaData: {
              $exists: true,
              $type: "object",
            },
          },
        },
        {
          $limit: 10,
        },
        {
          $project: {
            merchantName: "$name",
            integrationType: {
              $cond: {
                if: {
                  $and: [
                    { $eq: ["$integrations.integrationName", "foodics"] },
                    { $eq: ["$integrations.active", true] },
                  ],
                },
                then: "foodics",
                else: "cashier",
              },
            },
          },
        },
      ])
      .toArray(); // Convert the cursor to an array

    // Convert data to CSV format
    const csv = parse(businesses, csvOptions);

    // Write CSV to a file
    const outputFilePath = "./output.csv";
    const writeStream = createWriteStream(outputFilePath);
    writeStream.write(csv);
    writeStream.end();

    console.log(`CSV file has been saved to ${outputFilePath}.`);
  } catch (error) {
    console.error("Error:", error.message);
  } finally {
    // Close the database connection
    await client.close();
    console.log("Database connection closed.");
  }
}

main()
