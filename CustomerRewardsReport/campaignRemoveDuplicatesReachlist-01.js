const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const campaignCollectionName = "campaigns";

async function extractAndDeduplicateReachLists(campaignIds) {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const campaignCollection = db.collection(campaignCollectionName);

    for (const campaignId of campaignIds) {
      console.log(`Processing campaign ID: ${campaignId}`);

      const campaign = await campaignCollection.findOne({
        _id: new ObjectId(campaignId),
      });

      if (!campaign) {
        console.log(`Campaign with ID ${campaignId} not found.`);
        continue;
      }

      let reachList = campaign.reachList || [];

      // Remove duplicates
      const uniqueReachList = [
        ...new Set(reachList.map((id) => id.toString())),
      ];
      const removedDuplicates = reachList.length - uniqueReachList.length;
      console.log(
        `Removed ${removedDuplicates} duplicate(s) from reachList of campaign ID ${campaignId}.`
      );

      // Update campaign document with deduplicated reachList
      await campaignCollection.updateOne(
        { _id: new ObjectId(campaignId) },
        { $set: { reachList: uniqueReachList } }
      );

      //   // Prepare data for CSV
      //   const data = uniqueReachList.map((id, index) => ({
      //     index: index + 1,
      //     reachListId: id,
      //   }));

      //   // Define CSV fields
      //   const fields = ["index", "reachListId"];
      //   const json2csvParser = new Parser({ fields });
      //   const csv = json2csvParser.parse(data);

      //   // Write CSV to file
      //   const outputFile = `reachlist_${campaignId}.csv`;
      //   await fs.writeFile(outputFile, csv);
      //   console.log(`ReachList data successfully written to ${outputFile}`);
    }
  } catch (error) {
    console.error(
      "Error while extracting and deduplicating reachList data:",
      error
    );
  } finally {
    await client.close();
  }
}

// Replace with the specific campaign IDs
const campaignIds = [
  "64be6b8553090f001ddff664",
];

extractAndDeduplicateReachLists(campaignIds);
