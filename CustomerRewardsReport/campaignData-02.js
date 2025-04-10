const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const campaignCollectionName = "campaigns";

async function extractReachList(campaignId) {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const campaignCollection = db.collection(campaignCollectionName);

    // Query to find the specific campaign by _id
    const campaign = await campaignCollection.findOne({ _id: new ObjectId(campaignId) });

    if (!campaign) {
      console.log(`Campaign with ID ${campaignId} not found.`);
      return;
    }

    const reachList = campaign.reachList || [];

    if (reachList.length === 0) {
      console.log(`No reachList IDs found for campaign ID ${campaignId}`);
      return;
    }

    // Track duplicates using a Set
    const seen = new Set();
    const data = reachList.map((id, index) => {
      const isDuplicate = seen.has(id.toString());
      seen.add(id.toString()); // Add to the Set after checking
      return {
        index: index + 1,
        reachListId: id,
        duplicate: isDuplicate ? 'Yes' : 'No', // Add "duplicate" column
      };
    });

    // Define CSV fields
    const fields = ["index", "reachListId", "duplicate"];

    // Convert data to CSV
    const json2csvParser = new Parser({ fields });
    const csv = json2csvParser.parse(data);

    // Write CSV to file using async fs
    const outputFile = `reachlist${campaignId}.csv`;
    await fs.writeFile(outputFile, csv);
    console.log(`ReachList data successfully written to ${outputFile}`);
  } catch (error) {
    console.error("Error while extracting reachList data:", error);
  } finally {
    await client.close();
  }
}

// Replace with the specific campaign ID
const campaignId = "67a1bc501d4372001d1b16b8"; 
extractReachList(campaignId);
