const { MongoClient, ObjectId } = require("mongodb");
const fs = require("fs");
const csvParser = require("csv-parser");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const collectionName = "customerloyalties";
const csvFilePath = "customers_data_kabisat.csv";

async function updateCustomerPoints() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const customers = await parseCSV(csvFilePath);

    // console.log(customers);

    let count = 0;
    for (const customer of customers) {
      const { phoneNumber, pointsToAdd } = customer;
      console.log(pointsToAdd);

      const filter = {
        businessId: new ObjectId("67b720b3506a0c001d8c9914"),
        loyaltyId: new ObjectId("67b720b3506a0c001d8c9915"),
        "phone.number": phoneNumber,
      };
      console.log(filter);

        const update = {
          $inc: { points: Number(pointsToAdd), totalPoints: Number(pointsToAdd) },
          $set: { updated_at: new Date() },
        };

        const result = await collection.updateOne(filter, update);
        if (result.matchedCount > 0) {
          console.log(`Updated points for phone number: ${phoneNumber}`);
        } else {
          console.log(`No matching customer found for phone: ${phoneNumber}`);
        }
      count++;
      console.log("count", count);
    }
    console.log(customers.length);
  } catch (error) {
    console.error("Error updating customer points:", error);
  } finally {
    await client.close();
  }
}

function parseCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (data) => {
        results.push({
          phoneNumber: data?.phone?.startsWith("+966")
            ? data?.phone?.split("")?.slice(4)?.join("")
            : data?.phone,
          pointsToAdd: data.points,
        });
      })
      .on("end", () => resolve(results))
      .on("error", (error) => reject(error));
  });
}

updateCustomerPoints();
