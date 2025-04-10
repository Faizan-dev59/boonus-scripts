const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const campaignCollectionName = "campaigns";
const customerVoucherCollectionName = "customervouchers";
const customerCollectionName = "customers";

const start = new Date("2022-01-01T00:00:00.000Z");
const end = new Date("2022-12-31T23:59:59.000Z");

async function extractData() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const campaignCollection = db.collection(campaignCollectionName);
    const customerVoucherCollection = db.collection(
      customerVoucherCollectionName
    );
    const customerCollection = db.collection(customerCollectionName);

    // Fetch campaigns within the specified date range
    const query = {
      businessId: new ObjectId("63110db6c7f269001ff563c2"),
      reward: { $ne: null },
    //   created_at: {
    //     $gte: start,
    //     $lt: end,
    //   },
    };

    const campaigns = await campaignCollection.find(query).toArray();
    const trigger = {
      LAST_VISIT: "ABSENCE",
      PASS_ID: "INSTANT",
      BIRTHDAY: "BIRTHDAY",
    };

    if (campaigns.length === 0) {
      console.log("No campaigns found for the specified date range");
      return;
    }

    // Extract required campaign data and collect all customerIds (owners) from reachLists
    const campaignData = campaigns.map((campaign) => ({
      campaignId: campaign._id,
      campaignName: campaign.name || "N/A",
      trigger: trigger[campaign.trigger] || "N/A",
      reward: campaign.reward || "N/A",
      reachListLength: campaign.reachList?.length || 0,
      reachListOwners: campaign.reachList?.map((item) => item) || [],
    }));

    // Flatten and get unique owners
    const allOwners = [
      ...new Set(campaignData.flatMap((campaign) => campaign.reachListOwners)),
    ].map((id) => new ObjectId(id));

    if (allOwners.length === 0) {
      console.log("No customers found in reachLists.");
      return;
    }

    // Query to find matching customer vouchers
    const voucherQuery = { owner: { $in: allOwners } };
    const customerVouchers = await customerVoucherCollection
      .find(voucherQuery)
      .toArray();

    // Group vouchers by customerId (owner)
    const customerVoucherMap = customerVouchers.reduce((acc, voucher) => {
      acc[voucher.owner.toString()] = acc[voucher.owner.toString()] || [];
      acc[voucher.owner.toString()].push({
        customerId: voucher.owner,
        code: voucher.code || "N/A",
        redeemed: voucher.redeemed || false,
      });
      return acc;
    }, {});

    // Fetch customer names
    const customerQuery = { _id: { $in: allOwners } };
    console.log(customerQuery);
    const customers = await customerCollection.find(customerQuery).toArray();

    console.log(customers);

    // Create a map of customerId -> full name
    const customerNameMap = customers.reduce((acc, customer) => {
      acc[
        customer._id.toString()
      ] = `${customer.firstName} ${customer.lastName}`;
      return acc;
    }, {});

    console.log(customerNameMap);

    // Prepare CSV data campaign-wise
    const csvData = [];
    campaignData.forEach((campaign) => {
      campaign.reachListOwners.forEach((owner) => {
        const vouchers = customerVoucherMap[owner.toString()] || [
          { customerId: owner, code: "N/A", redeemed: "N/A" },
        ];
        vouchers.forEach((voucher) => {
          csvData.push({
            campaignId: campaign.campaignId,
            campaignName: campaign.campaignName,
            trigger: campaign.trigger,
            reward: campaign.reward,
            reachListLength: campaign.reachListLength,
            customerId: voucher.customerId,
            customerName: customerNameMap[owner.toString()] || "N/A",
            code: voucher.code,
            redeemed: voucher.redeemed,
          });
        });
      });
    });

    // Define CSV fields
    const fields = [
      "campaignId",
      "campaignName",
      "trigger",
      "reward",
      "reachListLength",
      "customerId",
      "customerName",
      "code",
      "redeemed",
    ];

    // Convert data to CSV
    const json2csvParser = new Parser({ fields });
    const csv = json2csvParser.parse(csvData);

    // Write CSV to file
    const outputFile = "Campaigns-Data-HappyHospitalityRestaurantsCompany-2022.csv";
    await fs.writeFile(outputFile, csv);
    console.log(`Data successfully written to ${outputFile}`);
  } catch (error) {
    console.error("Error while extracting campaign data:", error);
  } finally {
    await client.close();
  }
}

extractData();
