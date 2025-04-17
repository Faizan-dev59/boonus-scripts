const fs = require("fs");
const { ObjectId, MongoClient } = require("mongodb");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);
const BUSINESS_ID = "63110db6c7f269001ff563c2";
const BATCH_SIZE = 500;

function buildAggregationPipeline(businessId) {
  return [
    {
      $match: {
        businessId: new ObjectId(businessId),
        created_at: {
          $gte: new Date("2024-01-01T00:00:00Z"),
          $lte: new Date("2025-12-31T23:59:59.999Z"),
        },
      },
    },
    {
      $lookup: {
        from: "customers",
        let: { customerId: "$customerId" },
        pipeline: [
          {
            $match: { $expr: { $eq: ["$_id", "$$customerId"] } },
          },
          {
            $project: {
              firstName: 1,
              lastName: 1,
              birthDate: 1,
              gender: 1,
            },
          },
        ],
        as: "information",
      },
    },
    {
      $addFields: {
        customerInfo: { $arrayElemAt: ["$information", 0] },
      },
    },
    // {
    //   $lookup: {
    //     from: "transactions",
    //     as: "transaction",
    //     let: {
    //       type: "$type",
    //       customerId: "$customerId",
    //       businessId: "$businessId",
    //     },
    //     pipeline: [
    //       {
    //         $match: {
    //           // $expr: {
    //           //   $in: ["$type", ["POINTS", "STAMP_CARD_POINTS", "STAMP"]],
    //           // },
    //           $expr: {
    //             $and: [
    //               { $eq: ["$$customerId", "$customerId"] },
    //               { $eq: ["$$businessId", "$businessId"] },
    //             ],
    //           },
    //         },
    //       },
    //       // {
    //       //   $limit: 1,
    //       // },
    //     ],
    //   },
    // },
    // {
    //   $unwind: {
    //     path: "$transaction",
    //     preserveNullAndEmptyArrays: true,
    //   },
    // },
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
      $addFields: {
        transactionData: { $arrayElemAt: ["$transactions", 0] },
      },
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
              totalRewardCost: {
                $sum: "$rewardCost",
              },
            },
          },
          {
            $project: {
              _id: 0,
              rewards: 1,
              redeemedRewards: 1,
              totalRewardCost: 1,
            },
          },
        ],
        as: "customervouchers",
      },
    },
    {
      $addFields: {
        voucherData: { $arrayElemAt: ["$customervouchers", 0] },
      },
    },
    {
      $project: {
        firstName: "$customerInfo.firstName",
        lastName: "$customerInfo.lastName",
        birthday: "$customerInfo.birthDate",
        gender: "$customerInfo.gender",
        countryCode: "$phone.countryCode",
        number: "$phone.number",
        joinedAt: "$created_at",
        lastVisit: "$lastVisit",
        // currentStamps: "$stampsProgress",
        // completedStamps: "$stampsProgress",
        downloadedThePass: {
          $size: { $ifNull: ["$serialNumbers", []] }, // Ensures serialNumbers is always an array
        },
        currentPoints: "$points",
        totalPoints: "$totalPoints",
        totalVisits: "$transactionData.visits",
        totalSpending: "$transactionData.totalSpending",
        totalRewards: { $ifNull: ["$voucherData.rewards", 0] },
        redeemedRewards: { $ifNull: ["$voucherData.redeemedRewards", 0] },
        totalRewardCost: { $ifNull: ["$voucherData.totalRewardCost", 0] },
        // transactionCreatedAt: "$transaction.created_at",
        // transactionUserId: "$transaction.user",
      },
    },
  ];
}

function formatDate(date) {
  if (!date) return "";
  const options = {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  };
  return new Intl.DateTimeFormat("en-GB", options).format(new Date(date));
}

function flattenCustomerData(customer) {
  const sumCurrent =
    customer.currentStamps?.reduce(
      (acc, stamp) => acc + (stamp.numberOfStamps || 0),
      0
    ) || 0;

  const sumFinished =
    customer.completedStamps?.reduce(
      (acc, stamp) => acc + (stamp.numberOfFinishedCards || 0),
      0
    ) || 0;

  return {
    firstName: customer.firstName || "",
    lastName: customer.lastName || "",
    birthday: formatDate(customer.birthday), // Format date
    gender: customer.gender || "",
    countryCode: customer.countryCode || "",
    number: customer.number || "",
    // currentStamps: sumCurrent,
    // completedStamps: sumFinished,
    downloadedThePass: customer?.downloadedThePass > 0 ? "Yes" : "No",
    joinedAt: formatDate(customer.joinedAt), // Format date
    lastVisit: formatDate(customer.lastVisit), // Format date
    currentPoints: customer?.currentPoints || 0,
    totalPoints: customer?.totalPoints || 0,
    totalVisits: customer?.totalVisits || 0,
    totalSpending: customer?.totalSpending || 0,
    totalRewards: customer?.totalRewards || 0,
    redeemedRewards: customer?.redeemedRewards || 0,
    totalRewardCost: customer?.totalRewardCost || 0,
    // transactionCreatedAt: formatDate(customer?.transactionCreatedAt) || "",
    // transactionUserId: customer?.transactionUserId,
  };
}

function writeHeaders(fileStream, headers) {
  fileStream.write(headers.join(",") + "\n");
}

function writeRow(fileStream, row) {
  const csvRow = Object.values(row)
    .map((value) => `"${String(value).replace(/"/g, '""')}"`)
    .join(",");
  fileStream.write(csvRow + "\n");
}

async function main() {
  try {
    console.log("Connecting to the database...");
    const db = client.db("boonus");
    const customerLoyalty = db.collection("customerloyalties");

    const pipeline = buildAggregationPipeline(BUSINESS_ID);
    const cursor = customerLoyalty.aggregate(pipeline, {
      allowDiskUse: true,
      batchSize: BATCH_SIZE,
    });

    const fileStream = fs.createWriteStream("Happy Hospitality Restaurants Company-(2024-2025).csv");
    const headers = [
      "firstName",
      "lastName",
      "birthday",
      "gender",
      "countryCode",
      "number",
      // "currentStamps",
      // "completedStamps",
      "downloadedThePass",
      "joinedAt",
      "lastVisit",
      "currentPoints",
      "totalPoints",
      "totalVisits",
      "totalSpending",
      "totalRewards",
      "redeemedRewards",
      "totalRewardCost",
      // "transactionCreatedAt",
      // "transactionUserId",
    ];

    writeHeaders(fileStream, headers);

    let batch = [];
    let batchCount = 0;

    for await (const document of cursor) {
      batch.push(document);

      if (batch.length === BATCH_SIZE) {
        batchCount += 1;
        console.log(`Processing batch ${batchCount}...`);
        batch.forEach((customer) => {
          const flatCustomer = flattenCustomerData(customer);
          // console.log(flatCustomer);
          writeRow(fileStream, flatCustomer);
        });
        batch = [];
      }
    }

    if (batch.length > 0) {
      batchCount += 1;
      console.log(`Processing final batch ${batchCount}...`);
      batch.forEach((customer) => {
        const flatCustomer = flattenCustomerData(customer);
        // console.log(flatCustomer);
        writeRow(fileStream, flatCustomer);
      });
    }

    fileStream.end();
    console.log("Data export completed successfully!");
  } catch (error) {
    console.error("Error:", error);
  } finally {
    await client.close();
    console.log("Database connection closed.");
  }
}

main();
