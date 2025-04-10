const { MongoClient } = require("mongodb");
const fs = require("fs");
require("dotenv").config();
const Papa = require("papaparse");

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const businessCollectionName = "businesses";

const removeBusiness = async (filePath) => {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const businessCollection = db.collection(businessCollectionName);

    const file = fs.readFileSync(filePath, "utf8");
    const results = Papa.parse(file, { header: true });

    const emailCounts = new Map();
    const emails = [];

    // Extract emails and track occurrences
    results.data.forEach((row) => {
      try {
        const obj = JSON.parse(row?.team);
        const email = obj?.[0]?.email?.trim();
        if (email && email.includes("@")) {
          emails.push(email);
          emailCounts.set(email, (emailCounts.get(email) || 0) + 1);
        }
      } catch (e) {
        // Ignore errors
      }
    });

    const uniqueEmails = Array.from(new Set(emails));
    console.log(`Extracted ${uniqueEmails.length} unique emails.`);

    const result = await businessCollection
      .find(
        { "team.email": { $in: uniqueEmails }, "team.role": "OWNER" },
        { projection: { _id: 1, "team.email": 1 } }
      )
      .toArray();

    console.log(`Found ${result.length} matching records.`);

    // Extract _id and emails
    const idEmailPairs = [];
    result.forEach((item) => {
      const emailList = item.team?.map((t) => t.email).filter(Boolean) || [];
      emailList.forEach((email) => {
        console.log(email);
        console.log(emailCounts.get(email));
        if (uniqueEmails.includes(email)) {
          idEmailPairs.push({
            _id: item._id.toString(),
            email,
          });
        }
      });
    });

    console.log(`Processed ${idEmailPairs.length} records for CSV export.`);

    // Convert to CSV format
    const csv = Papa.unparse(idEmailPairs);
    console.log(`CSV length: ${csv.length}`);

    // Write the result to a CSV file
    fs.writeFileSync("New-Business.csv", csv);
    console.log("Extracted _id values with emails saved to new_businessIds.csv");

    // Uncomment to delete businesses from the database
    // const deleteResult = await businessCollection.deleteMany({
    //   "team.email": { $in: uniqueEmails },
    // });

    // console.log(
    //   `Deleted ${deleteResult?.deletedCount} businesses from the database.`
    // );
  } catch (error) {
    console.error("Error:", error);
  } finally {
    await client.close();
  }
};

removeBusiness("business.csv");
