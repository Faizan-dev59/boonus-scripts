const mongodb = require("mongodb").MongoClient;
const Json2csvParser = require("json2csv").Parser;
const { lookup } = require("dns");
const fs = require("fs");
const { ObjectId, MongoClient } = require("mongodb");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);

/**
 * Builds the aggregation pipeline for customer loyalty data.
 */
const buildPipeline = (businessId) => {
  return [
    {
      $match: {
        businessId: new ObjectId(businessId),
      },
    },
    {
      $lookup: {
        from: "customers",
        localField: "customerId",
        foreignField: "_id",
        as: "information",
      },
    },
    {
      $unwind: {
        path: "$information",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $addFields: {
        number: "$phone.number",
        countryCode: "$phone.countryCode",
        firstName: "$information.firstName",
        lastName: "$information.lastName",
        birthday: {
          $dateToString: {
            format: "%d-%m-%Y",
            date: "$information.birthDate",
          },
        },
        gender: "$information.gender",
        joinDate: {
          $dateToString: {
            format: "%d-%m-%Y",
            date: "$created_at",
          },
        },
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
    // {
    //   $lookup: {
    //     from: "transactions",
    //     let: { customerId: "$customerId", businessId: "$businessId" },
    //     pipeline: [
    //       {
    //         $match: {
    //           $expr: {
    //             $and: [
    //               { $eq: ["$$customerId", "$customerId"] },
    //               { $eq: ["$$businessId", "$businessId"] },
    //             ],
    //           },
    //         },
    //       },
    //       {
    //         $group: {
    //           _id: null,
    //           totalSpending: { $sum: "$amount" },
    //           visits: { $sum: 1 },
    //         },
    //       },
    //       {
    //         $project: {
    //           _id: 0,
    //           totalSpending: 1,
    //           visits: 1,
    //         },
    //       },
    //     ],
    //     as: "transactionsData",
    //   },
    // },
    // {
    //   $unwind: {
    //     path: "$transactionsData",
    //     preserveNullAndEmptyArrays: true,
    //   },
    // },
    // {
    //   $lookup: {
    //     from: "customervouchers",
    //     let: { customerId: "$customerId" },
    //     pipeline: [
    //       {
    //         $match: {
    //           $expr: { $eq: ["$$customerId", "$owner"] },
    //           businessId: new ObjectId(businessId),
    //         },
    //       },
    //       {
    //         $group: {
    //           _id: null,
    //           rewards: {
    //             $sum: 1,
    //           },
    //           redeemedRewards: {
    //             $sum: {
    //               $cond: [{ $eq: ["$redeemed", true] }, 1, 0],
    //             },
    //           },
    //         },
    //       },
    //       {
    //         $project: {
    //           _id: 0,
    //           rewards: 1,
    //           redeemedRewards: 1,
    //         },
    //       },
    //     ],
    //     as: "customervouchers",
    //   },
    // },
    // {
    //   $lookup: {
    //     from: "customervouchers",
    //     let: { customerId: "$customerId" },
    //     pipeline: [
    //       {
    //         $match: {
    //           $expr: { $eq: ["$$customerId", "$owner"] },
    //           businessId: new ObjectId(businessId),
    //         },
    //       },
    //       {
    //         $project: {
    //           _id: 0,
    //           redeemed: 1,
    //           rewardCost: 1,
    //           // Other fields...
    //         },
    //       },
    //     ],
    //     as: "customervouchers",
    //   },
    // },
    // {
    //   $unwind: {
    //     path: "$customervouchers",
    //     preserveNullAndEmptyArrays: true,
    //   },
    // },
    // {
    //   $addFields: {
    //     redeem: { $ifNull: ["$customervouchers.redeem", false] },
    //     rewardCost: { $ifNull: ["$customervouchers.rewardCost", 0] },
    //   },
    // },
    {
      $lookup: {
        from: "businesses",
        localField: "businessId",
        foreignField: "_id",
        as: "businessInfo",
      },
    },
    // {
    //   $addFields: {
    //     totalRewards: { $ifNull: ["$customervouchers.rewards", 0] },
    //     totalRedeemedRewards: {
    //       $ifNull: ["$customervouchers.redeemedRewards", 0],
    //     },
    //   },
    // },
    // {
    //   $limit: 10,
    // },
    {
      $project: {
        _id: 0,
        billNo: "$transaction.orderNumber",
        transactionDate: "$transaction.created_at",
        businessName: {
          $arrayElemAt: ["$businessInfo.name", 0],
        },
        businessType: {
          $arrayElemAt: ["$businessInfo.category", 0],
        },
        countryCode: "$phone.countryCode",
        number: "$phone.number",
        firstName: "$information.firstName",
        lastName: "$information.lastName",
        gender: "$information.gender",
        birthday: "$birthday",
        joinDate: "$joinDate",
        amount: { $ifNull: ["$transaction.amount", 0] },
        points: { $ifNull: ["$transaction.points", 0] },
        numberOfStamps: { $ifNull: ["$transaction.numberOfStamps", 0] },
        // currentLevel: "$currentLevel",
        // currentPoints: "$points",
        // totalPoints: "$totalPoints",
        // totalVisits: "$transactionsData.visits",
        // totalSpending: "$transactionsData.totalSpending",
        // totalRewards: "$totalRewards",
        // redeemedRewards: "$totalRedeemedRewards",
        // redeemed: "$customervouchers.redeemed",
        // rewardCost: "$customervouchers.rewardCost",
        transactionType: "$transaction.type",
        discountAmount: "$transaction.discountAmount",
        transactionUserId: "$transaction.user",
        transactionDate: "$transaction.created_at",
      },
    },
  ];
};

/**
 * Exports data to a CSV file.
 */
const exportToCSV = (data, fileName) => {
  const json2csvParser = new Json2csvParser({ header: true });
  const csvData = json2csvParser.parse(data);
  fs.writeFileSync(fileName, csvData);
  console.log(`Exported data to ${fileName} successfully!`);
};

/**
 * Main function to execute the aggregation and export.
 */
const exportCustomerData = async () => {
  try {
    await client.connect();
    const db = client.db("boonus");

    const customerLoyalty = db.collection("customerloyalties");
    const pipeline = buildPipeline("66533871667e19001dca04fd");
    const data = await customerLoyalty
      .aggregate(pipeline, {
        allowDiskUse: true,
        batchSize: 10000,
      })
      .toArray();

    exportToCSV(data, "Super Spot Car Wash-TransactionReport.csv");
    client.close();
  } catch (err) {
    console.error("Error occurred:", err);
  }
};

// Run the script
exportCustomerData();
