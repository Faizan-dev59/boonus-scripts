const { MongoClient, ObjectId } = require("mongodb");
const fs = require("fs");
require("dotenv").config();
const Papa = require("papaparse");

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const businessCollectionName = "businesses";
const transactionCollectionName = "transactions";
const customerCollectionName = "customerloyalties";
const customerVoucherCollection = "customervouchers";
const walletCollectionName = "wallets";
const loyaltyCollectionName = "loyalties";

const removeBusiness = async (filePath) => {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const businessCollection = db.collection(businessCollectionName);
    const transactionCollection = db.collection(transactionCollectionName);
    const customerCollection = db.collection(customerCollectionName);
    const customerVoucherCollectionRef = db.collection(
      customerVoucherCollection
    );
    const walletCollection = db.collection(walletCollectionName);
    const loyaltyCollection = db.collection(loyaltyCollectionName);

    // Read and parse CSV file
    const file = fs.readFileSync(filePath, "utf8");
    const results = Papa.parse(file, { header: true });

    if (!results.data || results.data.length === 0) {
      console.log("No data found in the CSV file.");
      return;
    }

    // Extract businessIds
    const businessIds = results.data
      .map((row) => new ObjectId(row?.businessId))
      .filter(Boolean);

    if (businessIds.length === 0) {
      console.log("No valid businessIds found.");
      return;
    }

    console.log(
      `Deleting ${businessIds.length} businesses and related data...`
    );

   

    // Delete from all collections
    await businessCollection.deleteMany({ _id: { $in: businessIds } });
    await transactionCollection.deleteMany({
      businessId: { $in: businessIds },
    });
    await customerCollection.deleteMany({ businessId: { $in: businessIds } });
    await customerVoucherCollectionRef.deleteMany({
      businessId: { $in: businessIds },
    });
    await walletCollection.deleteMany({ businessId: { $in: businessIds } });
    await loyaltyCollection.deleteMany({ businessId: { in: businessIds } });

    console.log("Deletion complete.");
  } catch (err) {
    console.error("Error occurred:", err);
  } finally {
    await client.close();
    console.log("Disconnected from MongoDB");
  }
};

removeBusiness("businessToRemove.csv");
