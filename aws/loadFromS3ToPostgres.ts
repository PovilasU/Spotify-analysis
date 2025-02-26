import AWS from "aws-sdk";
import { Client } from "pg";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import { parse } from "csv-parse/sync"; // Import parse from the sync version of csv-parse

// Load environment variables from .env file
dotenv.config();

// Set up AWS S3 client
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID, // AWS credentials
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION, // Example: 'us-east-1'
});

// Set up PostgreSQL client
const pgClient = new Client({
  host: "localhost", // Database host
  port: 5432, // Default PostgreSQL port
  user: process.env.PG_USER, // Your DB user
  password: process.env.PG_PASSWORD, // Your DB password
  database: process.env.PG_DATABASE, // Your DB name
});

const bucketName = process.env.S3_BUCKET_NAME || ""; // S3 bucket name
const fileKey = "transformed_tracks.csv"; // The file to be downloaded from S3

// Connect to PostgreSQL
pgClient
  .connect()
  .catch((err) => console.error("Failed to connect to PostgreSQL:", err));

// Download file from S3 and load into PostgreSQL
async function loadFromS3ToPostgres(): Promise<void> {
  try {
    // Download file from S3
    const params: AWS.S3.Types.GetObjectRequest = {
      Bucket: bucketName,
      Key: fileKey,
    };

    // Fetch the file from S3
    const data = await s3.getObject(params).promise();

    // If the file content exists, process it
    if (data.Body) {
      const fileContent = data.Body.toString(); // Convert buffer to string

      // Parse the CSV content (using 'csv-parse' package)
      const records = parse(fileContent, { columns: true, delimiter: "," });

      console.log("Parsed data:", records);

      // Insert data into PostgreSQL
      for (const row of records) {
        const query =
          "INSERT INTO your_table_name (column1, column2) VALUES ($1, $2)";
        const values = [row.column1, row.column2]; // Adjust this according to your CSV structure

        try {
          await pgClient.query(query, values);
          console.log("Row inserted into PostgreSQL:", row);
        } catch (insertError) {
          console.error("Error inserting row into PostgreSQL:", insertError);
        }
      }

      console.log("All data has been successfully inserted into PostgreSQL!");
    } else {
      console.error("No file content found in the S3 object.");
    }
  } catch (error) {
    console.error("Error downloading or processing file from S3:", error);
  } finally {
    await pgClient.end(); // Close the PostgreSQL connection
  }
}

// Invoke the load function
loadFromS3ToPostgres()
  .then(() => console.log("Process completed successfully!"))
  .catch((err) => console.error("Error during process:", err));
