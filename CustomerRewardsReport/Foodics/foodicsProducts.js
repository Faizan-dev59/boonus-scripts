const axios = require("axios");
const fs = require("fs/promises");

// Set up the API endpoint and authorization token
const API_URL = "https://api.foodics.com/v5/products";
const AUTH_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5MjE5OWM4OS0xNDU2LTQ3NTctYTQzMi1jYmYwNGFkNWM3NjIiLCJqdGkiOiJlM2IwNmQ4YTM0NmRjZWNiZTliOTg0OGMwODY2Y2IxOGQ1NzNkOTNmODU5NjVlMDE2MzI4MDY1NWU4NmMwNWEyNTA1MTJjOTc5OTU2MGEyMyIsImlhdCI6MTczNzY2MjA5NC40NTkxMDIsIm5iZiI6MTczNzY2MjA5NC40NTkxMDIsImV4cCI6MTg5NTQyODQ5NC4zNDQzMjMsInN1YiI6IjlhZmMzZWM1LWZiYTItNDdkYy1hNjhlLTgzYmY1NmJiNDVmNiIsInNjb3BlcyI6W10sImJ1c2luZXNzIjoiOWFmYzNlYzUtZmJlZS00ZDM0LTkwNjItMDVjZTY5ZjkwN2VkIiwicmVmZXJlbmNlIjoiNDcyMzk5In0.G-w_6wtf5HjSfyfUCWBC7pfhzb-wCgCL1H84-HNzfh7lgOib-sxJU1OB2y6Uaf3fvyMiAU9LVUOYBBtmIzMkS07pu3bAwNyITmhPnXTWsz0m1bmw50Pcq9XCVdIV0uw0H4Rea_zCKK0K-2wKcrdjT9RjY3QTFNf6mjTllxuh23a8zXnqvj0wMExgWKfD36D4yaCv8msnA5uUhjd7xz05Cmuk7chcJ5Y0sWzFsVOQnrm8CiTu5mdyTtPsNcKZ1rNu0S7lQ7qXFQwTccHoPbi41RWW3Kfb2nrKGXHhLCc_wTPX4kdMW8WPk7lh6f2xsdPD75VQ9eV260SIRASsR_1Hyi-keGxOexi0GvJcE4WuVQCYyhABLioPBDv7BuQ_r1Y0V4om-SdPIDyVN8_Up_K4seP5ul6J5EiUTmCthkMF1A-Z1W7vx4-1UHGdU_7Nx68rtrgeQhKe8vZFR3-tuySmjrdW2Moda8JHlk8KEaHTAvYoTEVCY3OKyFZsI5LaF1XT-dqXXCJRZ0wHe75jDKDJHGN7RdfNCiZInw1wHUTntAN21i6Ml0fv6rREVb-H1or0ULtoZP-Cmciimr0_D76fNTOoOPCQGYsGIIrfK_KZNhRnW0XMR2QzR7-UDLubpmm3X95UI6uE0aP4R7P9JDylfcXUWc-l5nxWqXdW86gWfic"; // Replace with your actual token

// Define the CSV headers
const csvHeaders = [
  "ID",
  "SKU",
  "Barcode",
  "Name",
  "Localized Name",
  "Description",
  "Localized Description",
  "Image",
  "Is Active",
  "Is Stock Product",
  "Is Non-Revenue",
  "Is Ready",
  "Pricing Method",
  "Selling Method",
  "Costing Method",
  "Preparation Time",
  "Price",
  "Cost",
  "Calories",
  "Walking Minutes to Burn Calories",
  "Is High Salt",
  "Created At",
  "Updated At",
  "Deleted At"
];

// Convert data to CSV format
function convertToCsv(data) {
  const rows = data.map((product) =>
    [
      product.id,
      product.sku,
      product.barcode || "",
      product.name,
      product.name_localized,
      product.description || "",
      product.description_localized || "",
      product.image || "",
      product.is_active,
      product.is_stock_product,
      product.is_non_revenue,
      product.is_ready,
      product.pricing_method,
      product.selling_method,
      product.costing_method,
      product.preparation_time || "",
      product.price,
      product.cost || "",
      product.calories || "",
      product.walking_minutes_to_burn_calories || "",
      product.is_high_salt,
      product.created_at,
      product.updated_at,
      product.deleted_at || ""
    ].join(",")
  );

  return [csvHeaders.join(","), ...rows].join("\n");
}

// Fetch data from the API
async function fetchProducts() {
  try {
    const response = await axios.get(API_URL, {
      headers: {
        Authorization: `Bearer ${AUTH_TOKEN}`,
        "Content-Type": "application/json",
      },
      params: {
        page: 1,
      },
    });

    const products = response.data.data;
    const csvData = convertToCsv(products);

    await fs.writeFile("foodicsProducts-LEIDEN-Page1.csv", csvData, "utf8");
    console.log("Data successfully written to foodicsProducts.csv");
  } catch (error) {
    console.error("Error fetching products:", error.message);
  }
}

// Execute the function
fetchProducts();