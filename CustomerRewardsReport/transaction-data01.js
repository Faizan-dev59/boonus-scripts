const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const transactionsCollectionName = "transactions";

const businessId = "66f93496da7116001d491555"; 
const customerLoyaltyId = "674b53db196cea001d02b681"; 

async function extractTransactionDetails(businessId, customerLoyaltyId) {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const transactionsCollection = db.collection(transactionsCollectionName);

    // Query to find transactions for the given businessId and customerLoyaltyId
    const transactions = await transactionsCollection.find({
      businessId: new ObjectId(businessId),
      customerLoyaltyId: new ObjectId(customerLoyaltyId),
    }).toArray();

    if (!transactions || transactions.length === 0) {
      console.log(`No transactions found for business ID ${businessId} and customer loyalty ID ${customerLoyaltyId}.`);
      return;
    }

    // Prepare data for CSV
    const data = transactions.map((transaction, index) => ({
      index: index + 1,
      transactionId: transaction._id.toString(),
      customerId: transaction.customerId.toString(),
      customerLoyaltyId: transaction.customerLoyaltyId.toString(),
      voucherId: transaction.voucherId,
      stampCardId: transaction.stampCardId,
      discountAmount: transaction.discountAmount,
      claimed: transaction.claimed,
      status: transaction.status,
      state: transaction.state,
      points: transaction.points,
      amount: transaction.amount,
      orderNumber: transaction.orderNumber,
      transactionUserId: transaction.user.toString(),
      type: transaction.type,
      sourceId: transaction.sourceId,
      createdAt: transaction.created_at,
      updatedAt: transaction.updated_at,
    }));

    // Define CSV fields
    const fields = [
      "index",
      "transactionId",
      "customerId",
      "customerLoyaltyId",
      "voucherId",
      "stampCardId",
      "discountAmount",
      "claimed",
      "status",
      "state",
      "points",
      "amount",
      "orderNumber",
      "transactionUserId",
      "type",
      "sourceId",
      "createdAt",
      "updatedAt",
    ];

    // Convert data to CSV
    const json2csvParser = new Parser({ fields });
    const csv = json2csvParser.parse(data);

    // Write CSV to file using async fs
    const outputFile = `Transaction-Details-559622751.csv`;
    await fs.writeFile(outputFile, csv);
    console.log(`Transaction details successfully written to ${outputFile}`);
  } catch (error) {
    console.error("Error while extracting transaction details:", error);
  } finally {
    await client.close();
  }
}

// Call function with specific business and customer loyalty ID
extractTransactionDetails(businessId, customerLoyaltyId);
