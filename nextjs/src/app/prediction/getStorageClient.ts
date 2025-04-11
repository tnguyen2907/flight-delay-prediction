import { Storage } from '@google-cloud/storage';

let storageClient: Storage | null = null;

export function getStorageClient(): Storage {
  if (!storageClient) {
    console.log("Creating new GCS Storage client");
    storageClient = new Storage({
      projectId: process.env.GCP_PROJECT_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    });
  }
  return storageClient;
}