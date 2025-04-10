const mongodb = require("mongodb").MongoClient;
const Json2csvParser = require("json2csv").Parser;
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
        created_at: {
          $gte: new Date("2023-01-01T00:00:00Z"),
          $lt: new Date("2025-01-01T00:00:00Z"),
        },
      },
    },
    {
      $lookup: {
        from: "customers",
        localField: "customerId",
        foreignField: "_id",
        as: "customerInfo",
      },
    },
    {
      $unwind: {
        path: "$customerInfo",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $lookup: {
        from: "transactions",
        localField: "customerId",
        foreignField: "customerId",
        as: "transactions",
      },
    },
    {
      $unwind: {
        path: "$transactions",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $unwind: {
        path: "$transactions.customerVoucherIds",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $lookup: {
        from: "customervouchers",
        localField: "transactions.customerVoucherIds",
        foreignField: "_id",
        as: "voucherInfo",
      },
    },
    {
      $unwind: {
        path: "$voucherInfo",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $addFields: {
        customerId: "$customerInfo._id",
        firstName: "$customerInfo.firstName",
        lastName: "$customerInfo.lastName",
        gender: "$customerInfo.gender",
        phoneNumber: "$customerInfo.phone.number",
        transactionAmount: {
          $ifNull: ["$transactions.amount", 0]
        },
        transactionRedeemed: {
          $ifNull: ["$voucherInfo.redeemed", false]
        },
        joinDate: {
          $concat: [
            {
              $dateToString: {
                format: "%m/%d/%Y",
                date: "$customerInfo.created_at",
              },
            },
            " ",
            {
              $toString: {
                $mod: [{ $hour: "$customerInfo.created_at" }, 12],
              },
            },
            ":",
            {
              $dateToString: {
                format: "%M:%S",
                date: "$customerInfo.created_at",
              },
            },
            " ",
            {
              $cond: [
                { $gte: [{ $hour: "$customerInfo.created_at" }, 12] },
                "PM",
                "AM",
              ],
            },
          ],
        },
        transactionCreatedAt: {
          $concat: [
            {
              $dateToString: {
                format: "%m/%d/%Y",
                date: "$transactions.created_at",
              },
            },
            " ",
            {
              $toString: {
                $mod: [{ $hour: "$transactions.created_at" }, 12],
              },
            },
            ":",
            {
              $dateToString: {
                format: "%M:%S",
                date: "$transactions.created_at",
              },
            },
            " ",
            {
              $cond: [
                { $gte: [{ $hour: "$transactions.created_at" }, 12] },
                "PM",
                "AM",
              ],
            },
          ],
        },
        transactionUpdatedAt: {
          $concat: [
            {
              $dateToString: {
                format: "%m/%d/%Y",
                date: "$transactions.updated_at",
              },
            },
            " ",
            {
              $toString: {
                $mod: [{ $hour: "$transactions.updated_at" }, 12],
              },
            },
            ":",
            {
              $dateToString: {
                format: "%M:%S",
                date: "$transactions.updated_at",
              },
            },
            " ",
            {
              $cond: [
                { $gte: [{ $hour: "$transactions.updated_at" }, 12] },
                "PM",
                "AM",
              ],
            },
          ],
        },
        rewardName: "$voucherInfo.name",
        rewardCode: "$voucherInfo.code",
      },
    },

    {
      $project: {
        _id: 0,
        customerId: 1,
        joinDate: 1,
        firstName: 1,
        lastName: 1,
        gender: 1,
        phoneNumber: 1,
        transactionAmount: 1,
        transactionRedeemed: 1,
        transactionCreatedAt: 1,
        transactionUpdatedAt: 1,
        rewardName: 1,
        rewardCode: 1,
      },
    },
    // {
    //   $limit: 10,
    // },
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
    const pipeline = buildPipeline("63513c0716d2f7001f211c37");
    const data = await customerLoyalty
      .aggregate(pipeline, {
        allowDiskUse: true,
        batchSize: 10000,
      })
      .toArray();


    exportToCSV(data, "CustomerRewardsReport-RATE-CAFE.csv");
    client.close();
  } catch (err) {
    console.error("Error occurred:", err);
  }
};

// Run the script
exportCustomerData();
