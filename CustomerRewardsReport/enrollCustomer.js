const fs = require("fs/promises");
const axios = require("axios");
const path = require("path");

// Constants
const COUNTRY_CODE = "966"; // Removed `+` to handle it programmatically
const BUSINESS_ID = "67bc68b4c5ef56001d39f3f3";
const WALLET_ID = "67bc6bf9c5ef56001d3ae415";
const INPUT_FILE = path.join(__dirname, "demoSheetForPoints-01.csv");
const CONCURRENCY_LIMIT = 1; // Number of parallel API calls

async function processCustomers() {
  try {
    // Read and parse the CSV file
    const data = await fs.readFile(INPUT_FILE, "utf8");
    const rows = data
      .split("\n")
      .slice(1) // Skip header row
      .filter(Boolean); // Remove empty lines

    // Preprocess customer data
    const customers = rows?.map((row, index) => {
      const [name, phone] = row.split(",").map((value) => value.trim());
      const [firstName, ...lastNameParts] = name.split(" ");
      const lastName = lastNameParts.join(" ") || "";

      // Normalize phone number
      let phoneNumber = phone.replace(/[^0-9]/g, ""); // Remove non-numeric characters
      if (phoneNumber.startsWith(COUNTRY_CODE)) {
        phoneNumber = phoneNumber.slice(COUNTRY_CODE.length); // Remove duplicate country code
      }
      phoneNumber = `+${COUNTRY_CODE}${phoneNumber}`; // Add the country code back with '+'

      // Log the processed data for debugging
      console.log(
        `[${
          index + 1
        }] Processed customer - Name: ${firstName} ${lastName}, Phone: ${phoneNumber}`
      );

      return { firstName, lastName, phoneNumber };
    });

    // Function to handle API calls with limited concurrency
    const processInBatches = async (items, handler, limit) => {
      const batches = [];
      for (let i = 0; i < items.length; i += limit) {
        const batch = items.slice(i, i + limit).map(handler);
        batches.push(Promise.all(batch));
      }
      for (const batch of batches) {
        await batch; // Wait for each batch to complete
      }
    };

    // API call handler
    const enrollCustomer = async ({ firstName, lastName, phoneNumber }) => {
      const enrollCustomerPayload = {
        firstName,
        lastName,
        walletId: WALLET_ID,
        consentPermission: true,
        businessId: BUSINESS_ID,
        phone: {
          countryCode: COUNTRY_CODE,
          number: phoneNumber.replace(`+${COUNTRY_CODE}`, ""), // Ensure number is sent without '+'
        },
        language: "EN",
        points: 16,
        totalPoints: 16,
      };

      try {
        const response = await axios.post(
          "https://api.boonus.app/api/v1/buyer/auth/enroll-customer-wallet",
          enrollCustomerPayload
        );
        console.log(`Customer enrolled successfully: ${response?.data}`);
      } catch (error) {
        console.error(
          `Error enrolling customer (${phoneNumber}):`,
          error.response?.data || error.message
        );
      }
    };

    // Process customers in batches
    await processInBatches(customers, enrollCustomer, CONCURRENCY_LIMIT);

    console.log("Customer processing completed.");
  } catch (error) {
    console.error(
      "Error processing customers:",
      error.response?.data || error.message
    );
  }
}

processCustomers();
