const axios = require("axios");
const fs = require("fs/promises");

// API endpoint for the customer data
const API_URL =
  "https://api.boonus.app/api/v1/members/619bd7a663fb467995beb75b/635f534872e59f001f77593e";
const AUTH_TOKEN =
  "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5MjE5OWM4OS0xNDU2LTQ3NTctYTQzMi1jYmYwNGFkNWM3NjIiLCJqdGkiOiJhNTZhNmM3Y2YyZTI5ODBjODYwMzkzYTQ5NjJjYWZkMDNjMzAzMTFjZmM1YjY4ZDE4NTdkZTdhNTQ3Yjk2ZDMwMjgxZGM3ODE4MWViODBlYiIsImlhdCI6MTczMzEyMjQyOC4yMTU1MDksIm5iZiI6MTczMzEyMjQyOC4yMTU1MDksImV4cCI6MTg5MDg4ODgyOC4xNjkyMzksInN1YiI6IjkxNWM5NTQ5LTFkNzYtNDgxNi04YTkxLTNkOTZkNzdhZTA4MCIsInNjb3BlcyI6W10sImJ1c2luZXNzIjoiOTE1Yzk1NDktMWRiNC00NWI3LTg3MGItYzY2YjliMDBhNDBiIiwicmVmZXJlbmNlIjoiODU2MjcyIn0.aDrSnlDBtwzkPaK1Nn_vBmWSjEODlTr-g4hQ7EosL1qqkQvL3JeFt3oPJf2vJHBvD3vMA1SYYGblsYR5V0q6c8yRs9_phtZ3VPeQ_RrBz5yRkmbDu9suKRamTO1eMgYXg4L7GcTG0MCZuN1-yopK9ux9hyf0IBL3YFDKcqbICOdNyN02FNlN9AEMnb6mOEDXHdqcytJBQvHnRMFNzuupUjajY4HpOIfJO2mtHd70XEFEy3MGQPLX4zwzm2-FUXaqO3zCAgC17A5RmFDikv3_scGe1egzSC6JrOi26z90xyMOAC9GQx7BPaEC_gItguFFQkQt-v1F-I-eK-oZiTQo4LHNg0qWE9EWwPUZJp5BFARdNLn3YlpxZ-sd0R4KwyswFG8gljZ1avZl61kPGWNLgIywd_Zt60FKwkK_269wKgDLhwjr7vPMfMwZq6kryaT0KHkq08lgya6sjbq1Q8oGJ8s2P-jbMglD4eBR0hfKo6sBwbUTzlbjUFFDxmysPk6COognV59RjUzkJ23kC09Ys7DKLs25kgBxF8yR66jPdeO_MKtGzDa8jtlYvcxSrdkxW4c23_YbR7Ur2lEenSK-p2Gz5sCeADI81qH2KjEKAJ29OVqsspfnoyiEKWTo0WCh1egt34OifRX5i1qjOnbiN0Voc9zUoFb8BJ703V3XNr8";

// CSV headers
const csvHeaders = [
  "Customer ID",
  "First Name",
  "Last Name",
  "Phone Number",
  "Language",
  "Gender",
  "City",
  "District",
  "Birth Date",
  "Current Level",
  "Points",
  "Total Points",
  "Total Visits",
  "Total Spendings",
  "Average Spending",
  "Last Visit",
  "Enrollment Date",
  "Rewards Redeemed",
  "Rewards Gained",
  "Redemption Rate",
  "Banned",
  "Marketing Email",
];

// Convert API data to CSV format
function convertToCsv(data) {
  const row = [
    data.customer._id,
    data.customer.firstName,
    data.customer.lastName,
    `${data.customer.phone.countryCode}${data.customer.phone.number}`,
    data.customer.settings.language,
    data.customer.gender,
    data.customer.city || "",
    data.customer.district || "",
    data.customer.birthDate,
    data.currentLevel,
    data.points,
    data.totalPoints,
    data.totalVisits,
    data.totalSpendings,
    data.averageSpending,
    data.lastVisit,
    data.enrollmentDate,
    data.rewardsRedeemed,
    data.rewardsGained,
    data.redemptionRate,
    data.banned,
    data.customer.permissions.marketingEmail,
  ];

  return [csvHeaders.join(","), row.join(",")].join("\n");
}

// Fetch customer data from the API
async function fetchCustomerData() {
  try {
    const response = await axios.get(API_URL, {
      headers: {
        Authorization: `Bearer ${AUTH_TOKEN}`,
        "Content-Type": "application/json",
      },
    });
    const customerData = response.data.data;

    const csvData = convertToCsv(customerData);

    await fs.writeFile("customerData.csv", csvData, "utf8");
    console.log("Data successfully written to customerData.csv");
  } catch (error) {
    console.error("Error fetching customer data:", error.message);
  }
}

// Execute the function
fetchCustomerData();
