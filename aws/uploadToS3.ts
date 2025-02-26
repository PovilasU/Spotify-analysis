import AWS from "aws-sdk";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";

dotenv.config();

// Load AWS credentials from .env
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
});

const filePath = path.join(__dirname, "..", "data", "transformed_tracks.csv");
const bucketName = process.env.S3_BUCKET_NAME || "";

async function uploadFileToS3() {
  try {
    const fileContent = fs.readFileSync(filePath);

    const params = {
      Bucket: bucketName,
      Key: "transformed_tracks.csv",
      Body: fileContent,
      ContentType: "text/csv",
    };

    const result = await s3.upload(params).promise();
    console.log("File uploaded successfully to S3:", result.Location);
  } catch (error) {
    console.error("Error uploading file:", error);
  }
}

uploadFileToS3();
