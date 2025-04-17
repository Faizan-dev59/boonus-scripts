const { MongoClient, ObjectId } = require("mongodb");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";

const removeBusiness = async ({ businessId, email }) => {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");

    const db = client.db(dbName);
    const businessCollection = db.collection("businesses");
    const transactionCollection = db.collection("transactions");
    const customerCollection = db.collection("customerloyalties");
    const customerVoucherCollection = db.collection("customervouchers");
    const walletCollection = db.collection("wallets");
    const loyaltyCollection = db.collection("loyalties");
    const usersCollection = db.collection("users");

    let query = {};

    if (businessId && ObjectId.isValid(businessId)) {
      query._id = new ObjectId(businessId);
    } else if (email) {
      query.email = email;
    } else {
      console.log("❌ Please provide a valid businessId or email.");
      return;
    }

    const business = await businessCollection.findOne(query);

    // if (!business) {
    //   console.log("❌ No business found for given businessId or email.");
    //   return;
    // }

    const id = business?._id;
    console.log(`🔍 Found business with _id: ${id}`);

    // Delete from all collections with logs
    const bRes = await businessCollection.deleteOne({ _id: id });
    console.log(bRes.deletedCount > 0 ? "✅ Business deleted." : "⚠️ Business not found for deletion.");

    const tRes = await transactionCollection.deleteMany({ businessId: id });
    console.log(`🧾 Transactions deleted: ${tRes.deletedCount}`);

    const cRes = await customerCollection.deleteMany({ businessId: id });
    console.log(`👥 Customer loyalties deleted: ${cRes.deletedCount}`);

    const vRes = await customerVoucherCollection.deleteMany({ businessId: id });
    console.log(`🎟️ Customer vouchers deleted: ${vRes.deletedCount}`);

    const wRes = await walletCollection.deleteMany({ businessId: id });
    console.log(`💳 Wallets deleted: ${wRes.deletedCount}`);

    const lRes = await loyaltyCollection.deleteMany({ businessId: id });
    console.log(`🏅 Loyalties deleted: ${lRes.deletedCount}`);

    if (email) {
      const uRes = await usersCollection.deleteOne({ email });
      console.log(uRes.deletedCount > 0
        ? `👤 User with email ${email} deleted.`
        : `⚠️ No user found with email ${email}.`);
    }

    console.log("✅ Deletion complete.");
  } catch (err) {
    console.error("❌ Error occurred:", err);
  } finally {
    await client.close();
    console.log("🔌 Disconnected from MongoDB");
  }
};

// Example usage
removeBusiness({
  businessId: "67912227430290001d774ae8", // optional
  email: "dellarosa.perfumeria@gmail.com",         // optional
});
