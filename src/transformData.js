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
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const csv_parser_1 = __importDefault(require("csv-parser"));
const cli_progress_1 = __importDefault(require("cli-progress"));
const sync_1 = require("csv-stringify/sync");
const tracks = [];
const artists = [];
const loadTracks = () => {
    return new Promise((resolve, reject) => {
        console.log("Loading tracks...");
        const progressBar = new cli_progress_1.default.SingleBar({}, cli_progress_1.default.Presets.shades_classic);
        let totalTracks = 0;
        let processedTracks = 0;
        fs_1.default.createReadStream(path_1.default.resolve(__dirname, "../data/tracks.csv"))
            .pipe((0, csv_parser_1.default)())
            .on("data", () => {
            totalTracks++;
        })
            .on("end", () => {
            progressBar.start(totalTracks, 0);
            fs_1.default.createReadStream(path_1.default.resolve(__dirname, "../data/tracks.csv"))
                .pipe((0, csv_parser_1.default)())
                .on("data", (row) => {
                if (row.name && row.duration_ms >= 60000) {
                    const [year, month, day] = row.release_date.split("-");
                    row.year = year;
                    row.month = month;
                    row.day = day;
                    row.danceability_level =
                        row.danceability < 0.5
                            ? "Low"
                            : row.danceability <= 0.6
                                ? "Medium"
                                : "High";
                    tracks.push(row);
                }
                processedTracks++;
                progressBar.update(processedTracks);
            })
                .on("end", () => {
                progressBar.stop();
                console.log(`Tracks loaded: ${tracks.length}`);
                resolve();
            })
                .on("error", reject);
        })
            .on("error", reject);
    });
};
const loadArtists = () => {
    return new Promise((resolve, reject) => {
        console.log("Loading artists...");
        const progressBar = new cli_progress_1.default.SingleBar({}, cli_progress_1.default.Presets.shades_classic);
        let totalArtists = 0;
        let processedArtists = 0;
        const artistIds = new Set(tracks.map((track) => track.artist_id));
        fs_1.default.createReadStream(path_1.default.resolve(__dirname, "../data/artists.csv"))
            .pipe((0, csv_parser_1.default)())
            .on("data", () => {
            totalArtists++;
        })
            .on("end", () => {
            progressBar.start(totalArtists, 0);
            fs_1.default.createReadStream(path_1.default.resolve(__dirname, "../data/artists.csv"))
                .pipe((0, csv_parser_1.default)())
                .on("data", (row) => {
                if (artistIds.has(row.id)) {
                    artists.push(row);
                }
                processedArtists++;
                progressBar.update(processedArtists);
            })
                .on("end", () => {
                progressBar.stop();
                console.log(`Artists loaded: ${artists.length}`);
                resolve();
            })
                .on("error", reject);
        })
            .on("error", reject);
    });
};
const saveToCSV = (filename, data) => {
    const csvData = (0, sync_1.stringify)(data, { header: true });
    fs_1.default.writeFileSync(path_1.default.resolve(__dirname, `../data/${filename}`), csvData);
};
const main = () => __awaiter(void 0, void 0, void 0, function* () {
    console.log("Starting data transformation...");
    yield loadTracks();
    yield loadArtists();
    console.log("Saving filtered tracks...");
    saveToCSV("transformed_tracks.csv", tracks);
    console.log("Saving filtered artists...");
    saveToCSV("filtered_artists.csv", artists);
    console.log("Data transformation complete");
});
main().catch(console.error);
