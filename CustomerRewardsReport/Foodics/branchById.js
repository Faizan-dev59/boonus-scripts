const axios = require("axios");
const fs = require("fs/promises");

const API_URL =
  "https://api.foodics.com/v5/branches/9b8bf6e5-d408-4d47-8879-a192e7cc71b7";
const AUTH_TOKEN =
  "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5MjE5OWM4OS0xNDU2LTQ3NTctYTQzMi1jYmYwNGFkNWM3NjIiLCJqdGkiOiJjZmIwMGE1NGYyOWQxMDM4ZjYyYTg0NjEyZjUyZjNiMTIwMzViYTc0YTY5ZTRhYzU5NTQ0Mzg3MTU3YzAwYjFjYzBlYTQxMTM0MjZjNTY3NSIsImlhdCI6MTczMDExMjM1My4wODQ1MTIsIm5iZiI6MTczMDExMjM1My4wODQ1MTIsImV4cCI6MTg4Nzg3ODc1My4wNDIzNDYsInN1YiI6IjlhZmU0MzcwLWVmOGMtNGI4MS04NjhhLThlMmFhZGQ4NTk1NiIsInNjb3BlcyI6W10sImJ1c2luZXNzIjoiOWFmZTQzNzAtZmM0Yi00NDAyLWJkMWItZDc2ZjBlMTQyMWUzIiwicmVmZXJlbmNlIjoiNTk2Njg3In0.ohFnAAiWY6gfvckEXSZvExUzsH--Cwft6Q_kQewddDnbRj_njJrCevslZok6wdD4kt7V6LtG9JPoDKgpjeWW0zrewF7hC4NNauc_2D1uGFqUL822J7lBT6m3HZ2isH4boLZa6FSMipnC4KWsYcBNrNiCqHSKhvKk1cM-u7AjuQjwrENyrlQCaDDrMF7KyguQL4LU55TRVYx0GKe86XZE_zqTOBjK3L1r5brAqBs7YSmE-9vngVVkujr8lZ3v09qCZgjdqyCVSc99JFSwc9kxWacMg5OkJtINEbQguEzklSeVhL4N_9GAigcvCY_fIgAeQuTp5MiyO9zRng_0O2O4aFX9Ccj80VhyYk7lm3duX7bax_1pfIVcesNsb_j4F7lQUM0_UJxwxuuxODw9U8ThR4Pbu6qFKH5fS8MI__-ZKGQlMxQEZZiwR4FmelNbMBfEpXxvM0jOIjkOqSTYuY9gK_ORzlMOQTfrAwWNPDEMtE1rBBp2CjoDMv6rrdrrR2wImgLbPWHVivUAgS_TZMvcKIPtKCz7CoNOqJeMVOrAx7kXJYfx8_1A5lXtR7tIomHsafw3E4gaW0pVOf5EuIgmxEvG0A3F4I3LDx9U7nr_z66vYGSafTHuWBuKlMEYcZVzD9bncSpVzF9QzXbzTMoX0p1g4Qtj93U94rT04aYlmjY";

const csvHeaders = [
  "User ID",
  "Branch ID", // Added branch ID from pivot
  "Pivot User ID",
  "Name",
  "Email",
  "Phone",
  "Language",
  "Is Owner",
  "Last Console Login",
  "Last Cashier Login",
  "Created At",
  "Updated At",
  "Branch Name",
];

function convertToCsv(users, branchName) {
  const rows = users.map((user) =>
    [
      user.id || "N/A",
      user.pivot?.branch_id || "N/A", // Handle nested pivot data
      user.pivot?.user_id || "N/A",
      user.name || "N/A",
      user.email || "N/A",
      user.phone || "N/A",
      user.lang || "N/A",
      user.is_owner || "N/A",
      user.last_console_login_at || "N/A",
      user.last_cashier_login_at || "N/A",
      user.created_at || "N/A",
      user.updated_at || "N/A",
      branchName || "N/A",
    ].join(",")
  );

  return [csvHeaders.join(","), ...rows].join("\n");
}

async function fetchUsers() {
  try {
    const response = await axios.get(API_URL, {
      headers: {
        Authorization: `Bearer ${AUTH_TOKEN}`,
        "Content-Type": "application/json",
      },
    });
    console.log(response.data);

    // Adjust to the correct API response structure
    const users = response.data.data.users || [];
    const branchName = response.data.data.name_localized || "N/A";

    if (!Array.isArray(users) || users.length === 0) {
      console.log("No users found.");
      return;
    }

    const csvData = convertToCsv(users, branchName);
    await fs.writeFile("foodicsUsers-Madina - Hijra.csv", csvData, "utf8");
    console.log("Data successfully written to foodicsUsers.csv");
  } catch (error) {
    console.error("Error fetching users:", error.message);
  }
}

fetchUsers();
