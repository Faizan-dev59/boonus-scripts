const Json2csvParser = require("json2csv").Parser;
const fs = require("fs");
const { ObjectId, MongoClient } = require("mongodb");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);
const BUSINESS_ID = "5fd8c929767c175c640b92e7"; // Business ID

// Build the aggregation pipeline
function buildAggregationPipeline(businessId) {
  return [
    {
      $lookup: {
        from: "customers",
        localField: "customerId",
        foreignField: "_id",
        as: "information",
      },
    },
    {
      $match: {
        businessId: new ObjectId(businessId),
        created_at: {
          $gte: new Date("2024-01-01T00:00:00.000Z"),
          $lte: new Date("2024-06-01T23:59:59.000Z")
    }
      },
    },
    {
      $unwind: { path: "$information", preserveNullAndEmptyArrays: true },
    },
    {
      $addFields: {
        number: "$phone.number",
        countryCode: "$phone.countryCode",
        firstName: "$information.firstName",
        lastName: "$information.lastName",
        birthday: "$information.birthDate",
        gender: "$information.gender",
        joinedAt: "$created_at",
        lastVisit: "$lastVisit",
        stampsProgress: "$stampsProgress",
        serialNumbers: "$serialNumbers",
      },
    },
    {
      $lookup: {
        from: "transactions",
        as: "transaction",
        let: {
          type: "$type",
          customerId: "$customerId",
          businessId: "$businessId",
        },
        pipeline: [
          {
            $match: {
              // $expr: {
              //   $in: ["$type", ["POINTS", "STAMP_CARD_POINTS", "STAMP"]],
              // },
              $expr: {
                $and: [
                  { $eq: ["$$customerId", "$customerId"] },
                  { $eq: ["$$businessId", "$businessId"] },
                ],
              },
            },
          },
          // {
          //   $limit: 1,
          // },
        ],
      },
    },
    {
      $unwind: {
        path: "$transaction",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $lookup: {
        from: "transactions",
        let: { customerId: "$customerId", businessId: "$businessId" },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ["$$customerId", "$customerId"] },
                  { $eq: ["$$businessId", "$businessId"] },
                ],
              },
            },
          },
          {
            $group: {
              _id: null,
              totalSpending: { $sum: "$amount" },
              visits: { $sum: 1 },
            },
          },
          { $project: { _id: 0, totalSpending: 1, visits: 1 } },
        ],
        as: "transactions",
      },
    },
    {
      $unwind: { path: "$transactions", preserveNullAndEmptyArrays: true },
    },
    {
      $lookup: {
        from: "customervouchers",
        let: { customerId: "$customerId" },
        pipeline: [
          {
            $match: {
              $expr: { $eq: ["$$customerId", "$owner"] },
              businessId: new ObjectId(businessId),
            },
          },
          {
            $group: {
              _id: null,
              rewards: { $sum: 1 },
              redeemedRewards: {
                $sum: { $cond: [{ $eq: ["$redeemed", true] }, 1, 0] },
              },
            },
          },
          { $project: { _id: 0, rewards: 1, redeemedRewards: 1 } },
        ],
        as: "customervouchers",
      },
    },
    {
      $unwind: { path: "$customervouchers", preserveNullAndEmptyArrays: true },
    },
    {
      $addFields: {
        totalRewards: { $ifNull: ["$customervouchers.rewards", 0] },
        totalRedeemedRewards: {
          $ifNull: ["$customervouchers.redeemedRewards", 0],
        },
      },
    },
    // {
    //   $limit: 100,
    // },
    {
      $project: {
        _id: 0,
        firstName: "$firstName",
        lastName: "$lastName",
        birthday: "$birthday",
        gender: "$gender",
        countryCode: "$countryCode",
        number: "$number",
        currentPoints: "$points",
        joinedAt: "$joinedAt",
        lastVisit: "$lastVisit",
        // currentStamps: "$stampsProgress",
        // completedStamps: "$stampsProgress",
        downloadedThePass: "$serialNumbers",
        totalPoints: "$totalPoints",
        totalVisits: "$transactions.visits",
        totalSpending: "$transactions.totalSpending",
        totalRewards: "$totalRewards",
        redeemedRewards: "$totalRedeemedRewards",
        transactionCreatedAt: "$transaction.created_at",
        transactionUserId: "$transaction.user",
      },
    },
  ];
}

// Process the aggregation results
function processResults(data) {
  data.forEach((customer) => {
    // const sumCurrent = customer.currentStamps.reduce(
    //   (acc, stampsProgress) => acc + stampsProgress.numberOfStamps,
    //   0
    // );
    // const sumFinished = customer.currentStamps.reduce(
    //   (acc, stampsProgress) => acc + stampsProgress.numberOfFinishedCards,
    //   0
    // );
    // customer.currentStamps = sumCurrent;
    // customer.completedStamps = sumFinished;
    customer.downloadedThePass =
      customer.downloadedThePass?.length > 0 ? "Yes" : "No";
  });
  return data;
}

// Export data to CSV
function exportToCSV(data, filename) {
  const json2csvParser = new Json2csvParser({ header: true });
  const csvData = json2csvParser.parse(data);
  fs.writeFile(filename, csvData, (error) => {
    if (error) throw error;
    console.log(`Data exported to ${filename} successfully!`);
  });
}

// Main execution flow
async function main() {
  try {
    const db = client.db("boonus");
    const customerLoyalty = db.collection("customerloyalties");

    const pipeline = buildAggregationPipeline(BUSINESS_ID);
    const data = await customerLoyalty
      .aggregate(pipeline, {
        allowDiskUse: true,
        batchSize: 10000,
      })
      .toArray();

      console.log(data);

    const processedData = processResults(data);
    exportToCSV(processedData, "TransactionReport-Faris+(Jan-Jun-2024).csv");

    client.close();
  } catch (error) {
    console.error("Error:", error);
  }
}

main();
