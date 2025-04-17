const mongodb = require("mongodb").MongoClient;
const csvtojson = require("csvtojson");
const Json2csvParser = require("json2csv").Parser;
const fs = require("fs");
const { ObjectId, MongoClient } = require("mongodb");
require("dotenv").config();

const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);

async function fetchBusiness(db, businessId) {
  const businesses = db.collection("businesses");
  const business = await businesses.findOne({
    _id: new ObjectId(businessId),
  });
  return {
    business,
    cashiers: business.team,
    branches: business.branches,
  };
}

async function processCSVFile(filePath, cashiers, branches) {
  const csvData = await csvtojson().fromFile(filePath);

  csvData.forEach((customer) => {
  

    const cashier = cashiers.find((team) => {
          //  console.log(team)     
      return team?.userId?.toString() === customer?.transactionUserId?.toString();
    });
    let branch;
    // console.log(cashier)
    if (cashier)
      branch = branches.find(
        (branch) => branch._id.toString() === cashier.branchId?.toString()
      );
      // console.log(branch)
    customer.branch = branch
      ? { name: branch.name, arabicName: branch.arabicName }
      : null;
  customer.cashier = cashier ? { email: cashier.email } : null;
  });

  return csvData;
}

function writeCSVFile(data, outputFileName) {
  const json2csvParser = new Json2csvParser({ header: true });
  const csvData = json2csvParser.parse(data);
  fs.writeFile(outputFileName, csvData, (err) => {
    if (err) throw err;
    console.log(`Write to ${outputFileName} successfully!`);
  });
}

async function main() {
  const businessId = "6329b37ab92ed5001f78993d";
  const inputFilePath =
    "Roaskava(Jan2025-10Apr2025).csv";
  const outputFileName = "Roaskava(Jan2025-10Apr2025)-with-branch.csv";

  try {
    await client.connect();
    const db = client.db("boonus");

    const { business, cashiers, branches } = await fetchBusiness(
      db,
      businessId
    );

    // console.log({ cashiers, branches });

    const processedData = await processCSVFile(
      inputFilePath,
      cashiers,
      branches
    );

    writeCSVFile(processedData, outputFileName);
  } catch (error) {
    console.error(`Something went wrong: ${error}`);
  } finally {
    if (client) {
      client.close();
    }
  }
}

main();
