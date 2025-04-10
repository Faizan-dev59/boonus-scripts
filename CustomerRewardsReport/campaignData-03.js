const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
const moment = require("moment");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const customerLoyaltyCollectionName = "customerloyalties";
const campaignCollectionName = "campaigns";
const walletCollectionName = "wallets";
const plan = "plans";
const business = "businesses";

// Define checkNotificationLimit function
const checkNotificationLimit = async (
  customerLoyaltyList,
  business,
  planCollection
) => {
  console.log(business?.planId);
  let availablePushNotification;
  if (business?.planId) {
    const businessPlan = await planCollection.findOne({ _id: business.planId });
    availablePushNotification =
      businessPlan.pushNotificationLimit - business.pushNotificationUsage;
  } else {
    availablePushNotification =
      business.pushNotificationLimit - business.pushNotificationUsage;
  }
  return {
    availablePushNotification,
    customerLoyaltyList: customerLoyaltyList.slice(
      0,
      availablePushNotification
    ),
  };
};

async function extractCustomerLoyaltyData(campaignId, triggerValue) {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const customerLoyaltyCollection = db.collection(
      customerLoyaltyCollectionName
    );
    const campaignCollection = db.collection(campaignCollectionName);
    const walletCollection = db.collection(walletCollectionName);
    const planCollection = db.collection(plan);
    const businessCollection = db.collection(business);

    // Step 1: Fetch the campaign by its ID
    const campaign = await campaignCollection.findOne({
      _id: new ObjectId(campaignId),
    });

    if (!campaign) {
      console.log(`Campaign with ID ${campaignId} not found.`);
      return;
    }

    // Extract businessId and perform checks for the business
    const businessId = campaign.businessId;

    const businessData = await businessCollection.findOne({
      _id: campaign.businessId,
    });

    // Step 2: Fetch the wallet for this business
    const wallet = await walletCollection.findOne({
      default: true,
      businessId,
    });

    if (!wallet) {
      console.log(`Wallet not found for business ID ${businessId}`);
      return;
    }

    console.log(`Found wallet with ID: ${wallet._id}`);

    // Step 3: Calculate the date `triggerValue` days ago using moment.js
    const daysAgo = moment()
      .startOf("day")
      .subtract(triggerValue, "days")
      .toDate();

    // Step 4: Query the customer loyalty data using reachList and lastVisit <= daysAgo
    const customers = await customerLoyaltyCollection
      .find({
        "serialNumbers.walletId": wallet._id, // Match walletId from the found wallet
        customerId: { $nin: campaign.reachList }, // Exclude customer IDs in reachList
        lastVisit: { $lte: daysAgo }, // Match lastVisit <= calculated daysAgo
        businessId: businessId, // Match businessId from the campaign's businessId
      })
      .toArray();


    if (customers.length === 0) {
      console.log(`No customer data found for campaign ID ${campaignId}`);
      return;
    }

    // Step 5: Check notification limit and get available push notifications
    const limitResult = await checkNotificationLimit(
      customers,
      {
        pushNotificationLimit: businessData.pushNotificationLimit,
        pushNotificationUsage: businessData.pushNotificationUsage,
        planId: businessData.planId,
      },
      planCollection
    );

    //console.log(limitResult.customerLoyaltyList);

    // // //Prepare data for CSV with limited customers
    // const data = limitResult.customerLoyaltyList.map((customer, index) => ({
    //   index: index + 1,
    //   customerId: customer.customerId,
    //   lastVisit: customer.lastVisit,
    //   businessId: customer.businessId,
    // }));

    // // Define CSV fields
    // const fields = ["index", "customerId", "lastVisit", "businessId"];

    // // Convert data to CSV
    // const json2csvParser = new Parser({ fields });
    // const csv = json2csvParser.parse(data);

    // // Write CSV to file using async fs
    // const outputFile = `test-1.csv`;
    // await fs.writeFile(outputFile, csv);
    // console.log(`Customer loyalty data successfully written to ${outputFile}`);
  } catch (error) {
    console.error("Error while extracting customer loyalty data:", error);
  } finally {
    await client.close();
  }
}

// Replace with the specific campaign ID and trigger value (days)
const campaignId = "6741c80c28d5f6001d4736d2"; // Provided campaign ID
const triggerValue = "10"; // Number of days ago
extractCustomerLoyaltyData(campaignId, triggerValue);
