import { Client } from 'minio';
import Run from '../models/Run';

// Initialize MinIO client
const minioClient = new Client({
  endPoint: process.env.MINIO_ENDPOINT || 'localhost',
  port: parseInt(process.env.MINIO_PORT || '9000'),
  useSSL: false,
  accessKey: process.env.MINIO_ACCESS_KEY || 'minio-access-key',
  secretKey: process.env.MINIO_SECRET_KEY || 'minio-secret-key',
});

// Check connection and bucket existence
minioClient.bucketExists('maxun-test')
  .then((exists) => {
    if (exists) {
      console.log('MinIO connected successfully.');
    } else {
      console.log('MinIO connected successfully.');
    }
  })
  .catch((err) => {
    console.error('Error connecting to MinIO:', err);
  });

// Constants for bucket names
const SCREENSHOTS_BUCKET = 'maxun-run-screenshots';
const CHECKPOINT_BUCKET = 'maxun-checkpoints';
const SCRAPING_RESULTS_BUCKET = 'maxun-scraping-results';

/**
 * Creates a bucket with the specified policy if it doesn't exist
 * @param bucketName The name of the bucket to create
 * @param policy The policy to apply ('public-read' or undefined for private)
 */
async function createBucketWithPolicy(bucketName: string, policy = 'public-read'): Promise<void> {
  try {
    const bucketExists = await minioClient.bucketExists(bucketName);
    if (!bucketExists) {
      await minioClient.makeBucket(bucketName);
      console.log(`Bucket ${bucketName} created successfully.`);
    } else {
      console.log(`Bucket ${bucketName} already exists.`);
    }

    if (policy === 'public-read') {
      // Apply public-read policy after confirming the bucket exists
      const policyJSON = {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Principal: "*",
            Action: ["s3:GetObject"],
            Resource: [`arn:aws:s3:::${bucketName}/*`]
          }
        ]
      };
      await minioClient.setBucketPolicy(bucketName, JSON.stringify(policyJSON));
      console.log(`Public-read policy applied to bucket ${bucketName}.`);
    }
  } catch (error) {
    console.error('Error in bucket creation or policy application:', error);
    throw error;
  }
}

// Initialize required buckets
async function initializeBuckets(): Promise<void> {
  await createBucketWithPolicy(SCREENSHOTS_BUCKET, 'public-read');
  await createBucketWithPolicy(CHECKPOINT_BUCKET, undefined); // Private
  await createBucketWithPolicy(SCRAPING_RESULTS_BUCKET, undefined); // Private
}

// Call initialization
initializeBuckets().catch(err => {
  console.error('Failed to initialize MinIO buckets:', err);
});

/**
 * Service class for handling binary output (screenshots, etc.)
 */
class BinaryOutputService {
  private bucketName: string;

  constructor(bucketName: string = SCREENSHOTS_BUCKET) {
    this.bucketName = bucketName;
  }

  /**
   * Uploads binary data to Minio and stores references in PostgreSQL.
   * @param run - The run object representing the current process.
   * @param binaryOutput - The binary output object containing data to upload.
   * @returns A map of Minio URLs pointing to the uploaded binary data.
   */
  async uploadAndStoreBinaryOutput(run: Run, binaryOutput: Record<string, any>): Promise<Record<string, string>> {
    const uploadedBinaryOutput: Record<string, string> = {};
    const plainRun = run.toJSON();

    for (const key of Object.keys(binaryOutput)) {
      let binaryData = binaryOutput[key];

      if (!plainRun.runId) {
        console.error('Run ID is undefined. Cannot upload binary data.');
        continue;
      }

      console.log(`Processing binary output key: ${key}`);

      // Check if binaryData has a valid Buffer structure and parse it
      if (binaryData && typeof binaryData.data === 'string') {
        try {
          const parsedData = JSON.parse(binaryData.data);
          if (parsedData && parsedData.type === 'Buffer' && Array.isArray(parsedData.data)) {
            binaryData = Buffer.from(parsedData.data);
          } else {
            console.error(`Invalid Buffer format for key: ${key}`);
            continue;
          }
        } catch (error) {
          console.error(`Failed to parse JSON for key: ${key}`, error);
          continue;
        }
      }

      // Handle cases where binaryData might not be a Buffer
      if (!Buffer.isBuffer(binaryData)) {
        console.error(`Binary data for key ${key} is not a valid Buffer.`);
        continue;
      }

      try {
        const minioKey = `${plainRun.runId}/${key}`;

        await this.uploadBinaryOutputToMinioBucket(run, minioKey, binaryData);

        // Construct the public URL for the uploaded object
        // todo: use minio endpoint 
        const publicUrl = `http://localhost:${process.env.MINIO_PORT}/${this.bucketName}/${minioKey}`;

        // Save the public URL in the result object
        uploadedBinaryOutput[key] = publicUrl;
      } catch (error) {
        console.error(`Error uploading key ${key} to MinIO:`, error);
      }
    }

    console.log('Uploaded Binary Output:', uploadedBinaryOutput);

    try {
      await run.update({ binaryOutput: uploadedBinaryOutput });
      console.log('Run successfully updated with binary output');
    } catch (updateError) {
      console.error('Error updating run with binary output:', updateError);
    }

    return uploadedBinaryOutput;
  }

  async uploadBinaryOutputToMinioBucket(run: Run, key: string, data: Buffer): Promise<void> {
    await createBucketWithPolicy(this.bucketName, 'public-read');
    try {
      console.log(`Uploading to bucket ${this.bucketName} with key ${key}`);
      await minioClient.putObject(this.bucketName, key, data, data.length, { 'Content-Type': 'image/png' });
      const plainRun = run.toJSON();
      
      if (!plainRun.binaryOutput) {
        plainRun.binaryOutput = {};
      }
      
      plainRun.binaryOutput[key] = `minio://${this.bucketName}/${key}`;
      console.log(`Successfully uploaded to MinIO: minio://${this.bucketName}/${key}`);
    } catch (error) {
      console.error(`Error uploading to MinIO bucket: ${this.bucketName} with key: ${key}`, error);
      throw error;
    }
  }

  public async getBinaryOutputFromMinioBucket(key: string): Promise<Buffer> {
    try {
      console.log(`Fetching from bucket ${this.bucketName} with key ${key}`);
      const stream = await minioClient.getObject(this.bucketName, key);
      return new Promise((resolve, reject) => {
        const chunks: Buffer[] = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('end', () => resolve(Buffer.concat(chunks)));
        stream.on('error', (error) => {
          console.error('Error while reading the stream from MinIO:', error);
          reject(error);
        });
      });
    } catch (error) {
      console.error(`Error fetching from MinIO bucket: ${this.bucketName} with key: ${key}`, error);
      throw error;
    }
  }
}

/**
 * Service class for handling scraping checkpoints and results
 */
class ScrapingStateService {
  /**
   * Stores a checkpoint for a scraping job
   * @param jobId - Unique identifier for the scraping job
   * @param checkpointNumber - Checkpoint sequence number
   * @param state - The state to checkpoint
   * @returns Promise resolving to the checkpoint key
   */
  async storeCheckpoint(jobId: string, checkpointNumber: number, state: any): Promise<string> {
    await createBucketWithPolicy(CHECKPOINT_BUCKET, undefined); // Ensure bucket exists (private)
    
    const key = `${jobId}/checkpoint-${checkpointNumber}-${Date.now()}.json`;
    const data = Buffer.from(JSON.stringify(state));
    
    try {
      await minioClient.putObject(
        CHECKPOINT_BUCKET,
        key, 
        data,
        data.length,
        { 'Content-Type': 'application/json' }
      );
      
      console.log(`Successfully stored checkpoint: minio://${CHECKPOINT_BUCKET}/${key}`);
      return key;
    } catch (error) {
      console.error(`Error storing checkpoint for job ${jobId}:`, error);
      throw error;
    }
  }

  /**
   * Retrieves a checkpoint for a scraping job
   * @param key - The checkpoint key
   * @returns Promise resolving to the checkpoint state
   */
  async getCheckpoint<T = any>(key: string): Promise<T> {
    try {
      const stream = await minioClient.getObject(CHECKPOINT_BUCKET, key);
      
      return new Promise((resolve, reject) => {
        let dataString = '';
        
        stream.on('data', (chunk) => {
          dataString += chunk.toString();
        });
        
        stream.on('end', () => {
          try {
            const data = JSON.parse(dataString);
            resolve(data as T);
          } catch (parseError: any) {
            reject(new Error(`Failed to parse checkpoint data: ${parseError.message}`));
          }
        });
        
        stream.on('error', (error) => {
          console.error('Error reading checkpoint from MinIO:', error);
          reject(error);
        });
      });
    } catch (error) {
      console.error(`Error retrieving checkpoint with key ${key}:`, error);
      throw error;
    }
  }

  /**
   * Stores intermediate scraping results
   * @param jobId - Unique identifier for the scraping job
   * @param batchNumber - Batch sequence number
   * @param data - The scraping results to store
   * @returns Promise resolving to the storage key
   */
  async storeIntermediateResults(jobId: string, batchNumber: number, data: any): Promise<string> {
    await createBucketWithPolicy(SCRAPING_RESULTS_BUCKET, undefined); // Ensure bucket exists (private)
    
    const key = `${jobId}/batch-${batchNumber}-${Date.now()}.json`;
    const buffer = Buffer.from(JSON.stringify(data));
    
    try {
      await minioClient.putObject(
        SCRAPING_RESULTS_BUCKET,
        key, 
        buffer,
        buffer.length,
        { 'Content-Type': 'application/json' }
      );
      
      console.log(`Successfully stored intermediate results: minio://${SCRAPING_RESULTS_BUCKET}/${key}`);
      return key;
    } catch (error) {
      console.error(`Error storing intermediate results for job ${jobId}:`, error);
      throw error;
    }
  }

  /**
   * Retrieves intermediate scraping results
   * @param key - The storage key
   * @returns Promise resolving to the scraping results
   */
  async getIntermediateResults<T = any>(key: string): Promise<T> {
    try {
      const stream = await minioClient.getObject(SCRAPING_RESULTS_BUCKET, key);
      
      return new Promise((resolve, reject) => {
        let dataString = '';
        
        stream.on('data', (chunk) => {
          dataString += chunk.toString();
        });
        
        stream.on('end', () => {
          try {
            const data = JSON.parse(dataString);
            resolve(data as T);
          } catch (parseError: any) {
            reject(new Error(`Failed to parse intermediate results: ${parseError.message}`));
          }
        });
        
        stream.on('error', (error) => {
          console.error('Error reading intermediate results from MinIO:', error);
          reject(error);
        });
      });
    } catch (error) {
      console.error(`Error retrieving intermediate results with key ${key}:`, error);
      throw error;
    }
  }

  /**
   * Lists all intermediate results for a job
   * @param jobId - Unique identifier for the scraping job
   * @returns Promise resolving to an array of result keys
   */
  async listIntermediateResults(jobId: string): Promise<string[]> {
    try {
      const prefix = `${jobId}/`;
      const stream = minioClient.listObjects(SCRAPING_RESULTS_BUCKET, prefix, true);
      
      return new Promise((resolve, reject) => {
        const keys: string[] = [];
        
        stream.on('data', (obj) => {
          if (obj.name) {
            keys.push(obj.name);
          }
        });
        
        stream.on('end', () => {
          resolve(keys);
        });
        
        stream.on('error', (error) => {
          console.error(`Error listing intermediate results for job ${jobId}:`, error);
          reject(error);
        });
      });
    } catch (error) {
      console.error(`Error listing intermediate results for job ${jobId}:`, error);
      throw error;
    }
  }

  /**
   * Merges all intermediate results for a job
   * @param jobId - Unique identifier for the scraping job
   * @returns Promise resolving to the merged results
   */
  async mergeIntermediateResults<T = any>(jobId: string): Promise<T[]> {
    try {
      const keys = await this.listIntermediateResults(jobId);
      
      if (keys.length === 0) {
        return [];
      }
      
      // Create a Map to track unique items by a stable key
      const uniqueResults = new Map<string, any>();
      
      // Process each batch
      for (const key of keys) {
        const batchResults = await this.getIntermediateResults<T[]>(key);
        
        if (Array.isArray(batchResults)) {
          batchResults.forEach(item => {
            // Create a unique key for deduplication (JSON stringify or hash)
            const uniqueKey = JSON.stringify(item);
            if (!uniqueResults.has(uniqueKey)) {
              uniqueResults.set(uniqueKey, item);
            }
          });
        }
      }
      
      // Convert back to array
      return Array.from(uniqueResults.values()) as T[];
    } catch (error) {
      console.error(`Error merging intermediate results for job ${jobId}:`, error);
      throw error;
    }
  }

  /**
   * Cleans up intermediate results for a job
   * @param jobId - Unique identifier for the scraping job
   * @returns Promise resolving to true if successful
   */
  async cleanupIntermediateResults(jobId: string): Promise<boolean> {
    try {
      const keys = await this.listIntermediateResults(jobId);
      
      if (keys.length === 0) {
        return true;
      }
      
      await minioClient.removeObjects(SCRAPING_RESULTS_BUCKET, keys);
      console.log(`Successfully cleaned up ${keys.length} intermediate results for job ${jobId}`);
      
      return true;
    } catch (error) {
      console.error(`Error cleaning up intermediate results for job ${jobId}:`, error);
      return false;
    }
  }
}

export { 
  minioClient, 
  BinaryOutputService, 
  ScrapingStateService, 
  SCREENSHOTS_BUCKET, 
  CHECKPOINT_BUCKET, 
  SCRAPING_RESULTS_BUCKET 
};