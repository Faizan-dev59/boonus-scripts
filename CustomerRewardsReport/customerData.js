const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const transactionCollectionName = "transactions";

async function extractTransactionData(businessId, customerLoyaltyId) {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const transactionCollection = db.collection(transactionCollectionName);

    // Query to match the documents
    const query = {
      businessId: new ObjectId(businessId),
      customerLoyaltyId: new ObjectId(customerLoyaltyId),
    };

    const transactions = await transactionCollection.find(query).toArray();

    if (transactions.length === 0) {
      console.log("No matching transactions found.");
      return;
    }

    // Prepare data for CSV
    const data = transactions.map((transaction) => ({
      type: transaction.type,
      orderNumber: transaction.orderNumber,
      amount: transaction.amount,
      points: transaction.points,
      customerVoucherId: transaction.customerVoucherIds.join(", "), // Join array if multiple voucher IDs
      created_at: transaction.created_at,
      user: transaction.user.toString(), // Convert ObjectId to string
    }));

    // Define CSV fields
    const fields = ["type", "orderNumber", "amount", "points", "customerVoucherId","created_at" ,"user"];

    // Convert data to CSV
    const json2csvParser = new Parser({ fields });
    const csv = json2csvParser.parse(data);

    // Write CSV to file using async fs
    const outputFile = `Transaction_data_for_0570004966.csv`;
    await fs.writeFile(outputFile, csv);
    console.log(`Transaction data successfully written to ${outputFile}`);
  } catch (error) {
    console.error("Error while extracting transaction data:", error);
  } finally {
    await client.close();
  }
}

// Replace with the actual businessId and customerLoyaltyId
const businessId = "619bd7a663fb467995beb75b";
const customerLoyaltyId = "670c15ffb4185e001db73a11";
extractTransactionData(businessId, customerLoyaltyId);
