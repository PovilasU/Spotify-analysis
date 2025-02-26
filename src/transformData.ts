import fs from "fs";
import path from "path";
import csv from "csv-parser";
import cliProgress from "cli-progress";
import { stringify } from "csv-stringify/sync";

interface Track {
  id: string;
  name: string;
  duration_ms: number;
  release_date: string;
  danceability: number;
  artist_id: string;
  year?: string;
  month?: string;
  day?: string;
  danceability_level?: string;
}

interface Artist {
  id: string;
  followers: number;
  genres: string;
  name: string;
  popularity: number;
}

const tracks: Track[] = [];
const artists: Artist[] = [];

const loadTracks = () => {
  return new Promise<void>((resolve, reject) => {
    console.log("Loading tracks...");

    const progressBar = new cliProgress.SingleBar(
      {},
      cliProgress.Presets.shades_classic
    );
    let totalTracks = 0;
    let processedTracks = 0;

    fs.createReadStream(path.resolve(__dirname, "../data/tracks.csv"))
      .pipe(csv())
      .on("data", () => {
        totalTracks++;
      })
      .on("end", () => {
        progressBar.start(totalTracks, 0);

        fs.createReadStream(path.resolve(__dirname, "../data/tracks.csv"))
          .pipe(csv())
          .on("data", (row: Track) => {
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
  return new Promise<void>((resolve, reject) => {
    console.log("Loading artists...");

    const progressBar = new cliProgress.SingleBar(
      {},
      cliProgress.Presets.shades_classic
    );
    let totalArtists = 0;
    let processedArtists = 0;

    const artistIds = new Set(tracks.map((track) => track.artist_id));

    fs.createReadStream(path.resolve(__dirname, "../data/artists.csv"))
      .pipe(csv())
      .on("data", () => {
        totalArtists++;
      })
      .on("end", () => {
        progressBar.start(totalArtists, 0);

        fs.createReadStream(path.resolve(__dirname, "../data/artists.csv"))
          .pipe(csv())
          .on("data", (row: Artist) => {
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

const saveToCSV = (filename: string, data: any[]) => {
  const csvData = stringify(data, { header: true });
  fs.writeFileSync(path.resolve(__dirname, `../data/${filename}`), csvData);
};

const main = async () => {
  console.log("Starting data transformation...");
  await loadTracks();
  await loadArtists();

  console.log("Saving filtered tracks...");
  saveToCSV("transformed_tracks.csv", tracks);

  console.log("Saving filtered artists...");
  saveToCSV("filtered_artists.csv", artists);

  console.log("Data transformation complete");
};

main().catch(console.error);
