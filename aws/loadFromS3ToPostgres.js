"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const aws_sdk_1 = __importDefault(require("aws-sdk"));
const pg_1 = require("pg");
const dotenv_1 = __importDefault(require("dotenv"));
const csv_parse_1 = __importDefault(require("csv-parse"));
// Load environment variables from .env file
dotenv_1.default.config();
// Set up AWS S3 client
const s3 = new aws_sdk_1.default.S3({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID, // AWS credentials
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION, // Example: 'us-east-1'
});
// Set up PostgreSQL client
const pgClient = new pg_1.Client({
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
function loadFromS3ToPostgres() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            // Download file from S3
            const params = {
                Bucket: bucketName,
                Key: fileKey,
            };
            // Fetch the file from S3
            const data = yield s3.getObject(params).promise();
            // If the file content exists, process it
            if (data.Body) {
                const fileContent = data.Body.toString(); // Convert buffer to string
                // Parse the CSV content (using 'csv-parse' package)
                (0, csv_parse_1.default)(fileContent, { columns: true, delimiter: "," }, (err, records) => __awaiter(this, void 0, void 0, function* () {
                    if (err) {
                        console.error("Error parsing CSV:", err);
                        return;
                    }
                    console.log("Parsed data:", records);
                    // Insert data into PostgreSQL
                    for (const row of records) {
                        const query = "INSERT INTO your_table_name (column1, column2) VALUES ($1, $2)";
                        const values = [row.column1, row.column2]; // Adjust this according to your CSV structure
                        try {
                            yield pgClient.query(query, values);
                            console.log("Row inserted into PostgreSQL:", row);
                        }
                        catch (insertError) {
                            console.error("Error inserting row into PostgreSQL:", insertError);
                        }
                    }
                    console.log("All data has been successfully inserted into PostgreSQL!");
                }));
            }
            else {
                console.error("No file content found in the S3 object.");
            }
        }
        catch (error) {
            console.error("Error downloading or processing file from S3:", error);
        }
        finally {
            yield pgClient.end(); // Close the PostgreSQL connection
        }
    });
}
// Invoke the load function
loadFromS3ToPostgres()
    .then(() => console.log("Process completed successfully!"))
    .catch((err) => console.error("Error during process:", err));
