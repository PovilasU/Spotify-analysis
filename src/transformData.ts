import fs from "fs";
import path from "path";
import csv from "csv-parser";
import cliProgress from "cli-progress";

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

    // Create a progress bar instance
    const progressBar = new cliProgress.SingleBar(
      {},
      cliProgress.Presets.shades_classic
    );
    let totalTracks = 0;
    let processedTracks = 0;

    // First, count the total number of tracks
    fs.createReadStream(path.resolve(__dirname, "../data/tracks.csv"))
      .pipe(csv())
      .on("data", () => {
        totalTracks++;
      })
      .on("end", () => {
        // Start the progress bar
        progressBar.start(totalTracks, 0);

        // Now process the tracks
        fs.createReadStream(path.resolve(__dirname, "../data/tracks.csv"))
          .pipe(csv())
          .on("data", (row: Track) => {
            if (row.name && row.duration_ms >= 60000) {
              const [year, month, day] = row.release_date.split("-");
              row.year = year;
              row.month = month;
              row.day = day;

              if (row.danceability < 0.5) {
                row.danceability_level = "Low";
              } else if (row.danceability <= 0.6) {
                row.danceability_level = "Medium";
              } else {
                row.danceability_level = "High";
              }

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

    // Create a progress bar instance
    const progressBar = new cliProgress.SingleBar(
      {},
      cliProgress.Presets.shades_classic
    );
    let totalArtists = 0;
    let processedArtists = 0;

    // Create a Set of artist IDs from the tracks
    const artistIds = new Set(tracks.map((track) => track.artist_id));

    // First, count the total number of artists
    fs.createReadStream(path.resolve(__dirname, "../data/artists.csv"))
      .pipe(csv())
      .on("data", () => {
        totalArtists++;
      })
      .on("end", () => {
        // Start the progress bar
        progressBar.start(totalArtists, 0);

        // Now process the artists
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

const main = async () => {
  console.log("Starting data transformation...");
  await loadTracks();
  await loadArtists();

  console.log("Saving filtered tracks...");
  fs.writeFileSync(
    path.resolve(__dirname, "../data/filtered_tracks.json"),
    JSON.stringify(tracks, null, 2)
  );

  console.log("Saving filtered artists...");
  fs.writeFileSync(
    path.resolve(__dirname, "../data/filtered_artists.json"),
    JSON.stringify(artists, null, 2)
  );

  console.log("Data transformation complete");
};

main().catch(console.error);
