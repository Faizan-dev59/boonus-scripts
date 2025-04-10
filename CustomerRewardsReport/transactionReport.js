const Json2csvParser = require("json2csv").Parser;
const fs = require("fs");
const { ObjectId, MongoClient } = require("mongodb");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);
const BUSINESS_ID = "5fd8c929767c175c640b92e7"; // Business ID
const BATCH_SIZE = 1000; // Process 1000 records at a time
const OUTPUT_FILE = "TransactionReport-Faris+(1-Feb-24-31-June-24).csv";
const START_DATE = "2024-02-01T00:00:00.000Z";
const END_DATE = "2024-06-31T23:59:59.000Z";

// Build the aggregation pipeline with pagination support
function buildAggregationPipeline(businessId, skip = 0, limit = BATCH_SIZE) {
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
          $gte: new Date(START_DATE),
          $lte: new Date(END_DATE)
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
      $skip: skip
    },
    {
      $limit: limit
    },
    {
      $project: {
        _id: 0,
        firstName: { $ifNull: ["$firstName", ""] },
        lastName: { $ifNull: ["$lastName", ""] },
        birthday: { $ifNull: ["$birthday", ""] },
        gender: { $ifNull: ["$gender", ""] },
        countryCode: { $ifNull: ["$countryCode", ""] },
        number: { $ifNull: ["$number", ""] },
        currentPoints: { $ifNull: ["$points", 0] },
        joinedAt: { $ifNull: ["$joinedAt", ""] },
        lastVisit: { $ifNull: ["$lastVisit", ""] },
        // currentStamps: "$stampsProgress",
        // completedStamps: "$stampsProgress",
        downloadedThePass: { $ifNull: ["$serialNumbers", []] },
        totalPoints: { $ifNull: ["$totalPoints", 0] },
        totalVisits: { $ifNull: ["$transactions.visits", 0] },
        totalSpending: { $ifNull: ["$transactions.totalSpending", 0] },
        totalRewards: { $ifNull: ["$totalRewards", 0] },
        redeemedRewards: { $ifNull: ["$totalRedeemedRewards", 0] },
        transactionCreatedAt: { $ifNull: ["$transaction.created_at", ""] },
        transactionUserId: { $ifNull: ["$transaction.user", ""] },
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

// Export data to CSV (using promises)
function exportToCSV(data, filename, isFirstBatch) {
  return new Promise((resolve, reject) => {
    const json2csvParser = new Json2csvParser({ 
      header: isFirstBatch // Only include header for first batch
    });
    
    const csvData = json2csvParser.parse(data);
    
    if (isFirstBatch) {
      // Write new file for first batch
      fs.writeFile(filename, csvData, (error) => {
        if (error) return reject(error);
        console.log(`First batch exported to ${filename} successfully!`);
        resolve();
      });
    } else {
      // Append for subsequent batches
      fs.appendFile(filename, "\n" + csvData, (error) => {
        if (error) return reject(error);
        console.log(`Batch appended to ${filename} successfully!`);
        resolve();
      });
    }
  });
}

// Get total count for pagination
async function getTotalCount(collection, businessId) {
  return await collection.countDocuments({
    businessId: new ObjectId(businessId),
    created_at: {
      $gte: new Date(START_DATE),
      $lte: new Date(END_DATE)
    }
  });
}

// Main execution flow with batching
async function main() {
  try {
    await client.connect();
    console.log("Connected to MongoDB");
    
    const db = client.db("boonus");
    const customerLoyalty = db.collection("customerloyalties");
    
    // Get total count to determine number of batches
    const totalCount = await getTotalCount(customerLoyalty, BUSINESS_ID);
    console.log(`Total records to process: ${totalCount}`);
    
    let processedCount = 0;
    let batchNumber = 0;
    
    // Process in batches
    while (processedCount < totalCount) {
      console.log(`Processing batch ${batchNumber + 1}: records ${processedCount} to ${Math.min(processedCount + BATCH_SIZE, totalCount)}`);
      
      const pipeline = buildAggregationPipeline(BUSINESS_ID, processedCount, BATCH_SIZE);
      const batchData = await customerLoyalty
        .aggregate(pipeline, {
          allowDiskUse: true,
          batchSize: BATCH_SIZE,
        })
        .toArray();
      
      if (batchData.length === 0) {
        console.log("No more data to process");
        break;
      }
      
      const processedData = processResults(batchData);
      await exportToCSV(processedData, OUTPUT_FILE, batchNumber === 0);
      
      processedCount += batchData.length;
      batchNumber++;
      
      console.log(`Progress: ${Math.round((processedCount / totalCount) * 100)}% complete`);
    }
    
    console.log(`Processing complete. Total records processed: ${processedCount}`);
  } catch (error) {
    console.error("Error:", error);
  } finally {
    await client.close();
    console.log("MongoDB connection closed");
  }
}

main();