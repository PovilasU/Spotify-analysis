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
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
// Load AWS credentials from .env
const s3 = new aws_sdk_1.default.S3({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION,
});
const filePath = path_1.default.join(__dirname, "data", "transformed_tracks.csv");
const bucketName = process.env.S3_BUCKET_NAME || "";
function uploadFileToS3() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const fileContent = fs_1.default.readFileSync(filePath);
            const params = {
                Bucket: bucketName,
                Key: "transformed_tracks.csv",
                Body: fileContent,
                ContentType: "text/csv",
            };
            const result = yield s3.upload(params).promise();
            console.log("File uploaded successfully to S3:", result.Location);
        }
        catch (error) {
            console.error("Error uploading file:", error);
        }
    });
}
uploadFileToS3();
