const { MongoClient, ObjectId } = require("mongodb");
require("dotenv").config();

const uri = process.env.DATABASE_URI_LOCAL;
const dbName = "test";
const campaignCollectionName = "campaigns";

const handleCustomerVisitsInBatch = async (customerId, campaigns, campaignCollection) => {
  const bulkOperations = campaigns.map(campaign => ({
    updateOne: {
      filter: { _id: campaign._id },
      update: { $pull: { reachList: customerId } }
    }
  }));

  if (bulkOperations.length > 0) {
    await campaignCollection.bulkWrite(bulkOperations);
  }
};

async function extractReachList() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const campaignCollection = db.collection(campaignCollectionName);

    // Query to find all campaigns for the specified businessId and trigger
    const campaigns = await campaignCollection
      .find({
        businessId: new ObjectId("675007716330190a182fd76b"),
        trigger: "LAST_VISIT"
      })
      .toArray();

    const customerId = new ObjectId("6788ee793d88a0031b21c7b3");

    // Filter campaigns where reachList is not empty
    const campaignsToUpdate = campaigns.filter(campaign => campaign.reachList.length > 0);

    // Perform batch updates
    await handleCustomerVisitsInBatch(customerId, campaignsToUpdate, campaignCollection);

    console.log(`${campaignsToUpdate.length} campaigns updated.`);
  } catch (error) {
    console.error("Error while extracting reachList data:", error);
  } finally {
    await client.close();
  }
}

extractReachList();
