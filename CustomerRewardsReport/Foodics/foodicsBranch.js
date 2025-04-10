const axios = require("axios");
const fs = require('fs/promises');

// Set up the API endpoint and authorization token
const API_URL = "https://api.foodics.com/v5/branches";
const AUTH_TOKEN =
  "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5MjE5OWM4OS0xNDU2LTQ3NTctYTQzMi1jYmYwNGFkNWM3NjIiLCJqdGkiOiJhNTZhNmM3Y2YyZTI5ODBjODYwMzkzYTQ5NjJjYWZkMDNjMzAzMTFjZmM1YjY4ZDE4NTdkZTdhNTQ3Yjk2ZDMwMjgxZGM3ODE4MWViODBlYiIsImlhdCI6MTczMzEyMjQyOC4yMTU1MDksIm5iZiI6MTczMzEyMjQyOC4yMTU1MDksImV4cCI6MTg5MDg4ODgyOC4xNjkyMzksInN1YiI6IjkxNWM5NTQ5LTFkNzYtNDgxNi04YTkxLTNkOTZkNzdhZTA4MCIsInNjb3BlcyI6W10sImJ1c2luZXNzIjoiOTE1Yzk1NDktMWRiNC00NWI3LTg3MGItYzY2YjliMDBhNDBiIiwicmVmZXJlbmNlIjoiODU2MjcyIn0.aDrSnlDBtwzkPaK1Nn_vBmWSjEODlTr-g4hQ7EosL1qqkQvL3JeFt3oPJf2vJHBvD3vMA1SYYGblsYR5V0q6c8yRs9_phtZ3VPeQ_RrBz5yRkmbDu9suKRamTO1eMgYXg4L7GcTG0MCZuN1-yopK9ux9hyf0IBL3YFDKcqbICOdNyN02FNlN9AEMnb6mOEDXHdqcytJBQvHnRMFNzuupUjajY4HpOIfJO2mtHd70XEFEy3MGQPLX4zwzm2-FUXaqO3zCAgC17A5RmFDikv3_scGe1egzSC6JrOi26z90xyMOAC9GQx7BPaEC_gItguFFQkQt-v1F-I-eK-oZiTQo4LHNg0qWE9EWwPUZJp5BFARdNLn3YlpxZ-sd0R4KwyswFG8gljZ1avZl61kPGWNLgIywd_Zt60FKwkK_269wKgDLhwjr7vPMfMwZq6kryaT0KHkq08lgya6sjbq1Q8oGJ8s2P-jbMglD4eBR0hfKo6sBwbUTzlbjUFFDxmysPk6COognV59RjUzkJ23kC09Ys7DKLs25kgBxF8yR66jPdeO_MKtGzDa8jtlYvcxSrdkxW4c23_YbR7Ur2lEenSK-p2Gz5sCeADI81qH2KjEKAJ29OVqsspfnoyiEKWTo0WCh1egt34OifRX5i1qjOnbiN0Voc9zUoFb8BJ703V3XNr8";

// Define the CSV headers
const csvHeaders = [
  "ID",
  "Name",
  "Reference",
  "Type",
  "Latitude",
  "Longitude",
  "Phone",
  "Opening From",
  "Opening To",
  "Inventory End Of Day Time",
  "Receipt Header",
  "Address",
  "City",
  "District",
  "Postal Code",
  "Street Name",
  "Building Number",
  "Additional Number",
  "Created At",
  "Updated At",
  "Receives Online Orders",
  "Accepts Reservations",
];

// Convert data to CSV format
function convertToCsv(data) {
  const rows = data.map((branch) =>
    [
      branch.id,
      branch.name,
      branch.reference,
      branch.type,
      branch.latitude,
      branch.longitude,
      branch.phone,
      branch.opening_from,
      branch.opening_to,
      branch.inventory_end_of_day_time,
      branch.receipt_header,
      branch.address,
      branch.settings?.sa_zatca_branch_address?.city || "",
      branch.settings?.sa_zatca_branch_address?.district || "",
      branch.settings?.sa_zatca_branch_address?.postal_code || "",
      branch.settings?.sa_zatca_branch_address?.street_name || "",
      branch.settings?.sa_zatca_branch_address?.building_number || "",
      branch.settings?.sa_zatca_branch_address?.additional_number || "",
      branch.created_at,
      branch.updated_at,
      branch.receives_online_orders,
      branch.accepts_reservations,
    ].join(",")
  );

  return [csvHeaders.join(","), ...rows].join("\n");
}

// Fetch data from the API
async function fetchBranches() {
  try {
    const response = await axios.get(API_URL, {
      headers: {
        Authorization: `Bearer ${AUTH_TOKEN}`,
        "Content-Type": "application/json",
      },
    });

    const branches = response.data.data;
    const csvData = convertToCsv(branches);

    await fs.writeFile("foodicsBranches.csv", csvData, "utf8");
    console.log("Data successfully written to branches.csv");
  } catch (error) {
    console.error("Error fetching branches:", error.message);
  }
}

// Execute the function
fetchBranches();
