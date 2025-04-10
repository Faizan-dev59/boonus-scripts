const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const campaignCollectionName = "campaigns";
const businessCollectionName = "businesses";

const start = new Date("2025-02-07T00:00:00.000Z");
const end = new Date("2025-02-07T23:59:59.000Z");

async function extractData() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const campaignCollection = db.collection(campaignCollectionName);
    const businessCollection = db.collection(businessCollectionName);

    // Query to find campaigns in the specified date range
    const query = {
      updated_at: {
        $gte: start,
        $lt: end,
      },
      trigger: "LAST_VISIT",
      // active: true,
    };

    // Fetch campaigns
    const campaigns = await campaignCollection.find(query).toArray();

    if (campaigns.length === 0) {
      console.log("No campaigns found for the specified date range");
      return;
    }

    // Fetch business names for corresponding businessIds
    const businessIds = campaigns
      .map((campaign) => campaign.businessId)
      .filter(Boolean);
    const businesses = await businessCollection
      .find({ _id: { $in: businessIds } })
      .toArray();

    const businessNameMap = businesses.reduce((acc, business) => {
      acc[business._id] = business.name;
      return acc;
    }, {});

    const trigger = {
        "LAST_VISIT": "ABSENCE",
        "PASS_ID": "INSTANT",
        "BIRTHDAY": "BIRTHDAY"
    }

    // Prepare data for CSV
    const data = campaigns.map((campaign) => ({
      campaignId: campaign._id,
      launchTime: campaign.launchDate || "N/A",
      numberOfNotifications: campaign.reachList?.length || 0,
      merchantId: campaign.businessId || "N/A",
      merchantName: businessNameMap[campaign.businessId] || "N/A",
      campaignType: trigger[campaign.trigger] || "N/A",
      updatedAt: campaign.updated_at || "N/A",
    }));

    // Define CSV fields
    const fields = [
      "campaignId",
      "launchTime",
      "numberOfNotifications",
      "merchantId",
      "merchantName",
      "campaignType",
      "updatedAt",
    ];

    // Convert data to CSV
    const json2csvParser = new Parser({ fields });
    const csv = json2csvParser.parse(data);

    // Write CSV to file using async fs
    const outputFile = 'Absence-Campaigns-7thFeb.csv';
    await fs.writeFile(outputFile, csv);
    console.log(`Data successfully written to ${outputFile}`);
  } catch (error) {
    console.error("Error while extracting campaign data:", error);
  } finally {
    await client.close();
  }
}

extractData();
