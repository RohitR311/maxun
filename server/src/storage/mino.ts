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
const CHECKPOINT_BUCKET = 'maxun-scraping-checkpoints';
const SCRAPING_RESULTS_BUCKET = 'maxun-scraping-results';
const RUN_CHECKPOINT_BUCKET = 'maxun-run-checkpoints';

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
 * Enhanced to support browser session-based execution
 */
class ScrapingStateService {
  /**
   * Stores a scraping checkpoint
   * @param runId - Unique identifier for the run
   * @param scrapingId - Unique identifier for the specific scraping action
   * @param checkpoint - The checkpoint state to store
   * @returns Promise resolving to the checkpoint key
   */
  async storeScrapingCheckpoint(runId: string, scrapingId: string, checkpoint: any): Promise<string> {
    await createBucketWithPolicy(CHECKPOINT_BUCKET, undefined);
    
    const key = `${runId}/${scrapingId}-checkpoint-${Date.now()}.json`;
    const data = Buffer.from(JSON.stringify(checkpoint));
    
    try {
      await minioClient.putObject(
        CHECKPOINT_BUCKET,
        key, 
        data,
        data.length,
        { 'Content-Type': 'application/json' }
      );
      
      console.log(`Successfully stored scraping checkpoint: minio://${CHECKPOINT_BUCKET}/${key}`);
      return key;
    } catch (error) {
      console.error(`Error storing checkpoint for run ${runId}, scraping ${scrapingId}:`, error);
      throw error;
    }
  }

  /**
   * Gets the latest checkpoint for a specific scraping action
   * @param runId - Unique identifier for the run
   * @param scrapingId - Unique identifier for the specific scraping action
   * @returns Promise resolving to the latest checkpoint or null if none exists
   */
  async getLatestScrapingCheckpoint<T = any>(runId: string, scrapingId: string): Promise<T | null> {
    try {
      const prefix = `${runId}/${scrapingId}-checkpoint-`;
      const stream = minioClient.listObjects(CHECKPOINT_BUCKET, prefix, true);
      
      const keys: string[] = [];
      for await (const obj of stream) {
        if (obj.name) {
          keys.push(obj.name);
        }
      }
      
      if (keys.length === 0) {
        return null;
      }
      
      // Sort by timestamp (which is part of the filename) to get the latest checkpoint
      keys.sort((a, b) => {
        const timestampA = parseInt(a.split('-').pop()?.split('.')[0] || '0');
        const timestampB = parseInt(b.split('-').pop()?.split('.')[0] || '0');
        return timestampB - timestampA;
      });
      
      return await this.getCheckpoint<T>(keys[0]);
    } catch (error) {
      console.error(`Error retrieving latest checkpoint for run ${runId}, scraping ${scrapingId}:`, error);
      return null;
    }
  }

  /**
   * Retrieves a checkpoint by key
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
   * Stores the results from a scraping session
   * @param runId - Unique identifier for the run
   * @param scrapingId - Unique identifier for the specific scraping action
   * @param data - The scraping results to store
   * @returns Promise resolving to the storage key
   */
  async storeScrapingResults(runId: string, scrapingId: string, data: any): Promise<string> {
    await createBucketWithPolicy(SCRAPING_RESULTS_BUCKET, undefined);
    
    const key = `${runId}/${scrapingId}-results-${Date.now()}.json`;
    const buffer = Buffer.from(JSON.stringify(data));
    
    try {
      await minioClient.putObject(
        SCRAPING_RESULTS_BUCKET,
        key, 
        buffer,
        buffer.length,
        { 'Content-Type': 'application/json' }
      );
      
      console.log(`Successfully stored scraping results: minio://${SCRAPING_RESULTS_BUCKET}/${key}`);
      return key;
    } catch (error) {
      console.error(`Error storing results for run ${runId}, scraping ${scrapingId}:`, error);
      throw error;
    }
  }

  /**
   * Lists all result files for a specific scraping action
   * @param runId - Unique identifier for the run
   * @param scrapingId - Unique identifier for the specific scraping action
   * @returns Promise resolving to an array of result keys
   */
  async listScrapingResults(runId: string, scrapingId: string): Promise<string[]> {
    try {
      const prefix = `${runId}/${scrapingId}-results-`;
      const stream = minioClient.listObjects(SCRAPING_RESULTS_BUCKET, prefix, true);
      
      const keys: string[] = [];
      for await (const obj of stream) {
        if (obj.name) {
          keys.push(obj.name);
        }
      }
      
      return keys;
    } catch (error) {
      console.error(`Error listing results for run ${runId}, scraping ${scrapingId}:`, error);
      return [];
    }
  }

  /**
   * Merges all scraping results from multiple browser sessions for a specific scraping action
   * @param runId - Unique identifier for the run
   * @param scrapingId - Unique identifier for the specific scraping action
   * @returns Promise resolving to the merged results
   */
  async mergeScrapingResults<T = any>(runId: string, scrapingId: string): Promise<T[]> {
    try {
      const keys = await this.listScrapingResults(runId, scrapingId);
      
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
      console.error(`Error merging results for run ${runId}, scraping ${scrapingId}:`, error);
      throw error;
    }
  }

  /**
   * Retrieves intermediate results
   * @param key - The storage key
   * @returns Promise resolving to the results
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
            reject(new Error(`Failed to parse results: ${parseError.message}`));
          }
        });
        
        stream.on('error', (error) => {
          console.error('Error reading results from MinIO:', error);
          reject(error);
        });
      });
    } catch (error) {
      console.error(`Error retrieving results with key ${key}:`, error);
      throw error;
    }
  }

  /**
   * Lists all scraping IDs for a run
   * @param runId - Unique identifier for the run
   * @returns Promise resolving to an array of scraping IDs
   */
  async getAllScrapingIds(runId: string): Promise<string[]> {
    try {
      const prefix = `${runId}/`;
      const stream = minioClient.listObjects(CHECKPOINT_BUCKET, prefix, true);
      
      const ids = new Set<string>();
      for await (const obj of stream) {
        if (obj.name) {
          // Extract scraping ID from the key pattern: runId/scrapingId-checkpoint-timestamp.json
          const parts = obj.name.substring(prefix.length).split('-checkpoint-');
          if (parts.length >= 1) {
            ids.add(parts[0]);
          }
        }
      }
      
      return Array.from(ids);
    } catch (error) {
      console.error(`Error listing scraping IDs for run ${runId}:`, error);
      return [];
    }
  }
  
  /**
   * Get all scraping checkpoints for a run
   * @param runId - Unique identifier for the run
   * @returns Promise resolving to a record mapping scraping IDs to their checkpoints
   */
  async getAllScrapingCheckpoints(runId: string): Promise<Record<string, any>> {
    try {
      const scrapingIds = await this.getAllScrapingIds(runId);
      const checkpoints: Record<string, any> = {};
      
      for (const scrapingId of scrapingIds) {
        const checkpoint = await this.getLatestScrapingCheckpoint(runId, scrapingId);
        if (checkpoint) {
          checkpoints[scrapingId] = checkpoint;
        }
      }
      
      return checkpoints;
    } catch (error) {
      console.error(`Failed to get all checkpoints for run ${runId}: ${error}`);
      return {};
    }
  }
  
  /**
   * Deletes a specific scraping checkpoint 
   * @param runId - Unique identifier for the run
   * @param scrapingId - Unique identifier for the specific scraping action
   */
  async deleteScrapingCheckpoint(runId: string, scrapingId: string): Promise<void> {
    try {
      const keys = await this.listScrapingCheckpoints(runId, scrapingId);
      
      for (const key of keys) {
        await minioClient.removeObject(CHECKPOINT_BUCKET, key);
      }
      
      console.log(`Successfully deleted checkpoints for run ${runId}, scraping ${scrapingId}`);
    } catch (error) {
      console.error(`Error deleting checkpoints for run ${runId}, scraping ${scrapingId}:`, error);
      throw error;
    }
  }
  
  /**
   * Lists all checkpoint files for a specific scraping action
   * @param runId - Unique identifier for the run
   * @param scrapingId - Unique identifier for the specific scraping action
   * @returns Promise resolving to an array of checkpoint keys
   */
  async listScrapingCheckpoints(runId: string, scrapingId: string): Promise<string[]> {
    try {
      const prefix = `${runId}/${scrapingId}-checkpoint-`;
      const stream = minioClient.listObjects(CHECKPOINT_BUCKET, prefix, true);
      
      const keys: string[] = [];
      for await (const obj of stream) {
        if (obj.name) {
          keys.push(obj.name);
        }
      }
      
      return keys;
    } catch (error) {
      console.error(`Error listing checkpoints for run ${runId}, scraping ${scrapingId}:`, error);
      return [];
    }
  }
  
  /**
   * Deletes all data for a specific scraping operation (checkpoints and results)
   * @param runId - Unique identifier for the run
   * @param scrapingId - Unique identifier for the specific scraping action
   */
  async deleteScrapingData(runId: string, scrapingId: string): Promise<void> {
    try {
      // Delete checkpoints
      await this.deleteScrapingCheckpoint(runId, scrapingId);
      
      // Delete results
      const resultKeys = await this.listScrapingResults(runId, scrapingId);
      for (const key of resultKeys) {
        await minioClient.removeObject(SCRAPING_RESULTS_BUCKET, key);
      }
      
      console.log(`Successfully deleted all data for run ${runId}, scraping ${scrapingId}`);
    } catch (error) {
      console.error(`Error deleting data for run ${runId}, scraping ${scrapingId}:`, error);
      throw error;
    }
  }
  
  /**
   * Deletes all scraping data for a run
   * @param runId - Unique identifier for the run
   */
  async deleteAllScrapingCheckpoints(runId: string): Promise<void> {
    try {
      const scrapingIds = await this.getAllScrapingIds(runId);
      
      for (const scrapingId of scrapingIds) {
        await this.deleteScrapingData(runId, scrapingId);
      }
      
      console.log(`Successfully deleted all scraping data for run ${runId}`);
    } catch (error) {
      console.error(`Error deleting all scraping data for run ${runId}:`, error);
      throw error;
    }
  }

  /**
 * Merges schema scraping results from multiple browser sessions
 * @param runId - Unique identifier for the run
 * @param scrapingId - Unique identifier for the specific schema scraping action
 * @returns Promise resolving to the merged schema results
 */
async mergeSchemaScrapingResults(runId: string, scrapingId: string): Promise<Record<string, any>> {
  try {
    const keys = await this.listScrapingResults(runId, scrapingId);
    
    if (keys.length === 0) {
      return {};
    }
    
    // Create a merged result object
    const mergedResults: Record<string, any> = {};
    
    // Process each batch and merge properties
    for (const key of keys) {
      const batchResults = await this.getIntermediateResults<any[]>(key);
      
      if (Array.isArray(batchResults) && batchResults.length > 0) {
        // If the batch has results, merge them into the final object
        for (const result of batchResults) {
          if (typeof result === 'object' && result !== null) {
            // Merge each property
            Object.entries(result).forEach(([propKey, propValue]) => {
              // Only update if the property doesn't exist or is empty
              if (!(propKey in mergedResults) || 
                  mergedResults[propKey] === undefined || 
                  mergedResults[propKey] === null || 
                  mergedResults[propKey] === '') {
                mergedResults[propKey] = propValue;
              }
            });
          }
        }
      }
    }
    
    return mergedResults;
  } catch (error) {
    console.error(`Error merging schema results for run ${runId}, scraping ${scrapingId}:`, error);
    return {};
  }
}

/**
 * Determines if a schema scraping operation is complete
 * Checks if all expected fields in the schema have values
 * 
 * @param runId - Unique identifier for the run
 * @param scrapingId - Unique identifier for the specific schema scraping action
 * @param expectedFields - Array of field names expected in the completed schema
 * @returns Promise resolving to whether the schema is complete
 */
async isSchemaScrapingComplete(
  runId: string, 
  scrapingId: string, 
  expectedFields: string[]
): Promise<boolean> {
  try {
    const mergedResults = await this.mergeSchemaScrapingResults(runId, scrapingId);
    
    // Check if all expected fields exist and have values
    return expectedFields.every(field => 
      field in mergedResults && 
      mergedResults[field] !== undefined && 
      mergedResults[field] !== null && 
      mergedResults[field] !== ''
    );
  } catch (error) {
    console.error(`Error checking schema completion for run ${runId}, scraping ${scrapingId}:`, error);
    return false;
  }
}

/**
 * Gets the missing fields from a schema scraping operation
 * 
 * @param runId - Unique identifier for the run
 * @param scrapingId - Unique identifier for the specific schema scraping action
 * @param expectedFields - Array of field names expected in the completed schema
 * @returns Promise resolving to array of missing field names
 */
async getMissingSchemaFields(
  runId: string, 
  scrapingId: string, 
  expectedFields: string[]
): Promise<string[]> {
  try {
    const mergedResults = await this.mergeSchemaScrapingResults(runId, scrapingId);
    
    // Return fields that are missing or have empty values
    return expectedFields.filter(field => 
      !(field in mergedResults) || 
      mergedResults[field] === undefined || 
      mergedResults[field] === null || 
      mergedResults[field] === ''
    );
  } catch (error) {
    console.error(`Error getting missing schema fields for run ${runId}, scraping ${scrapingId}:`, error);
    return expectedFields; // Return all fields as missing if there's an error
  }
}
}

export { 
  minioClient, 
  BinaryOutputService, 
  ScrapingStateService, 
  SCREENSHOTS_BUCKET, 
  CHECKPOINT_BUCKET, 
  SCRAPING_RESULTS_BUCKET,
};