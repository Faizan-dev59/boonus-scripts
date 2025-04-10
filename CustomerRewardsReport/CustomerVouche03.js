const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const collectionName = "customervouchers";

const start = new Date("2024-01-01T00:00:00.000Z");
const end = new Date("2025-01-15T23:59:59.000Z");

async function extractData() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Define the query to match the vouchers
    const query = {
        //   businessId: new ObjectId("63110db6c7f269001ff563c2"),
          updated_at: {
            $gte: start,
            $lte: end,
          },
          redeemed: false,
        //   campaignId: new ObjectId("644f9665f8425c001d5090af"),
    };

    // Perform aggregation with $lookup to join campaigns and customer collection
    const aggregationPipeline = [
      {
        $match: query,
      },
      {
        $lookup: {
          from: "campaigns", // Join with campaigns collection
          localField: "campaignId", // Match campaignId in customervouchers
          foreignField: "_id", // Match _id in campaigns collection
          as: "campaignDetails",
        },
      },
      {
        $unwind: {
          path: "$campaignDetails",
          preserveNullAndEmptyArrays: true, // Keep vouchers without matching campaign data
        },
      },
      {
        $match: {
          "campaignDetails.trigger": "LAST_VISIT", 
        },
      },
      {
        $lookup: {
          from: "customers", // Join with customers collection
          localField: "owner", // Match this field in customervouchers
          foreignField: "_id", // Match _id in customers collection
          as: "customerDetails",
        },
      },
      {
        $unwind: {
          path: "$customerDetails",
          preserveNullAndEmptyArrays: true, // Keep vouchers without matching customer data
        },
      },
      {
        $project: {
          _id: 1,
          stampCardId: 1,
          campaignId: 1,
          walletId: 1,
          redeemed: 1,
          businessId: 1,
          owner: 1,
          loyaltyId: 1,
          code: 1,
          discountType: { $ifNull: ["$discount.type", "N/A"] },
          discountUnitOff: { $ifNull: ["$discount.unitOff", "N/A"] },
          arabicUnitType: { $ifNull: ["$discount.arabicUnitType", "N/A"] },
          unitType: { $ifNull: ["$discount.unitType", "N/A"] },
          voucherSource: { $ifNull: ["$voucherSource", "N/A"] },
          name: { $ifNull: ["$name", "N/A"] },
          arabicName: { $ifNull: ["$arabicName", "N/A"] },
          rewardCost: { $ifNull: ["$rewardCost", "N/A"] },
          createdAt: { $ifNull: ["$created_at", "N/A"] },
          updatedAt: { $ifNull: ["$updated_at", "N/A"] },
          customerFirstName: { $ifNull: ["$customerDetails.firstName", "N/A"] },
          customerLastName: { $ifNull: ["$customerDetails.lastName", "N/A"] },
          customerPhone: {
            $ifNull: [
              { $concat: ["+$", "$customerDetails.phone.countryCode", "$customerDetails.phone.number"] },
              "N/A",
            ],
          },
          customerBirthDate: { $ifNull: ["$customerDetails.birthDate", "N/A"] },
        },
      },
    ];

    // Execute the aggregation pipeline
    const vouchers = await collection.aggregate(aggregationPipeline).toArray();

    if (vouchers.length === 0) {
      console.log("No vouchers found for the specified query");
      return;
    }

    // Prepare data for CSV
    const data = vouchers.map((voucher) => ({
      _id: voucher._id ? voucher._id.toString() : "N/A",
      stampCardId: voucher.stampCardId || "N/A",
      campaignId: voucher.campaignId ? voucher.campaignId.toString() : "N/A",
      walletId: voucher.walletId ? voucher.walletId.toString() : "N/A",
      redeemed: voucher.redeemed || false,
      businessId: voucher.businessId ? voucher.businessId.toString() : "N/A",
      owner: voucher.owner ? voucher.owner.toString() : "N/A",
      loyaltyId: voucher.loyaltyId ? voucher.loyaltyId.toString() : "N/A",
      code: voucher.code || "N/A",
      discountType: voucher.discountType || "N/A",
      discountUnitOff: voucher.discountUnitOff || "N/A",
      arabicUnitType: voucher.arabicUnitType || "N/A",
      unitType: voucher.unitType || "N/A",
      voucherSource: voucher.voucherSource || "N/A",
      name: voucher.name || "N/A",
      arabicName: voucher.arabicName || "N/A",
      rewardCost: voucher.rewardCost || "N/A",
      createdAt: voucher.createdAt || "N/A",
      updatedAt: voucher.updatedAt || "N/A",
      customerFirstName: voucher.customerFirstName,
      customerLastName: voucher.customerLastName,
      customerPhone: voucher.customerPhone,
      customerBirthDate: voucher.customerBirthDate,
    }));

    // Define CSV fields
    const fields = [
      "_id",
      "stampCardId",
      "campaignId",
      "walletId",
      "redeemed",
      "businessId",
      "owner",
      "loyaltyId",
      "code",
      "discountType",
      "discountUnitOff",
      "arabicUnitType",
      "unitType",
      "voucherSource",
      "name",
      "arabicName",
      "rewardCost",
      "createdAt",
      "updatedAt",
      "customerFirstName",
      "customerLastName",
      "customerPhone",
      "customerBirthDate",
    ];

    // Convert data to CSV
    const json2csvParser = new Parser({ fields });
    const csv = json2csvParser.parse(data);

    // Write CSV to file using async fs
    const outputFile = 'All-Absence-Rewards-(01-Jan-24-to-15-Jan-25).csv';
    await fs.writeFile(outputFile, csv);
    console.log(`Data successfully written to ${outputFile}`);
  } catch (error) {
    console.error("Error while extracting voucher data:", error);
  } finally {
    await client.close();
  }
}

extractData();
