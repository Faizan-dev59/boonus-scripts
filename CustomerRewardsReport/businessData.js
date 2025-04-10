const { MongoClient, ObjectId } = require("mongodb");
const { Parser } = require("json2csv");
const fs = require("fs").promises;
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const dbName = "boonus";
const businessCollectionName = "businesses";
const businessId = "65c9d1fa041c07001d99217d"; // Replace with the specific business ID

async function extractBranchStatuses(businessId) {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const businessCollection = db.collection(businessCollectionName);

    // Query to find the specific business by _id
    const business = await businessCollection.findOne({
      _id: new ObjectId(businessId),
    });

    if (!business) {
      console.log(`Business with ID ${businessId} not found.`);
      return;
    }

    // Extract the branches
    const branches = business.branches || [];

    if (branches.length === 0) {
      console.log(`No branches found for business ID ${businessId}`);
      return;
    }

    // Find duplicate references and IDs
    const referenceCounts = {};
    const idCounts = {};

    branches.forEach((branch) => {
      referenceCounts[branch.reference] =
        (referenceCounts[branch.reference] || 0) + 1;
      idCounts[branch.sourceId] = (idCounts[branch.sourceId] || 0) + 1;
    });

    // Prepare data for CSV (including duplicates)
    const data = branches.map((branch, index) => ({
      index: index + 1,
      id: branch.sourceId,
      reference: branch.reference,
      branchName: branch.name,
      phone: branch.phone.number,
      status: branch.active ? "Active" : "Inactive",
      referenceDuplicate: referenceCounts[branch.reference] > 1 ? "Yes" : "No",
      idDuplicate: idCounts[branch.sourceId] > 1 ? "Yes" : "No",
      
    }));

    // Define CSV fields
    const fields = [
      "index",
      "id",
      "reference",
      "branchName",
      "phone",
      "status",
      "referenceDuplicate",
      "idDuplicate",
    ];

    // Convert data to CSV
    const json2csvParser = new Parser({ fields });
    const csv = json2csvParser.parse(data);

    // Write CSV to file using async fs
    const outputFile = `Updated-Branch-Status-ESSO Coffee-1-13-25.csv`;
    await fs.writeFile(outputFile, csv);
    console.log(`Branches status data successfully written to ${outputFile}`);
  } catch (error) {
    console.error("Error while extracting branch status data:", error);
  } finally {
    await client.close();
  }
}

// Replace with the specific business ID
extractBranchStatuses(businessId);
