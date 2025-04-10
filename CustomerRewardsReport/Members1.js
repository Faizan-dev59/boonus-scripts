const mongodb = require("mongodb").MongoClient;
const Json2csvParser = require("json2csv").Parser;
const fs = require("fs").promises; // Use the fs promises API for async file operations
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
        gender: "$information.gender",
      },
    },
    {
      $limit: 10,
    },
    {
      $project: {
        _id: 0,
        firstName: "$information.firstName",
        lastName: "$information.lastName",
        gender: "$information.gender",
        countryCode: "$phone.countryCode",
        number: "$phone.number",
        currentPoints: "$points",
        totalPoints: "$totalPoints",
      },
    },
  ];
};

/**
 * Exports data to a CSV file asynchronously.
 */
const exportToCSV = async (data, fileName) => {
  const json2csvParser = new Json2csvParser({ header: true });
  const csvData = json2csvParser.parse(data);

  try {
    await fs.writeFile(fileName, csvData);
    console.log(`Exported data to ${fileName} successfully!`);
  } catch (err) {
    console.error("Error writing CSV file:", err);
  }
};

/**
 * Main function to execute the aggregation and export.
 */
const exportCustomerData = async () => {
  try {
    await client.connect();
    const db = client.db("boonus");

    const customerLoyalty = db.collection("customerloyalties");
    const pipeline = buildPipeline("671a4821dd4b3c001eca896a");
    const data = await customerLoyalty
      .aggregate(pipeline, {
        allowDiskUse: true,
        batchSize: 1000,
      })
      .toArray();

    data.forEach((item, index) => {
      console.log("Item " + index + 1);
      console.log(item);
    });

    // await exportToCSV(data, "Customer-Report-FROOJ-ABO-ALABED.csv");
  } catch (err) {
    console.error("Error occurred:", err);
  } finally {
    await client.close();
  }
};

// Run the script
exportCustomerData();
