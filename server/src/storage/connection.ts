import IORedis from 'ioredis';
import { WorkflowFile } from 'maxun-core';

// Initialize Redis connection
const redisClient = new IORedis({
  host: process.env.REDIS_HOST || 'localhost',
  port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT, 10) : 6379,
  maxRetriesPerRequest: null,
  password: process.env.REDIS_PASSWORD ? process.env.REDIS_PASSWORD : undefined,
});

redisClient.on('connect', () => {
  console.log('Redis connected successfully.');
});

redisClient.on('error', (err) => {
  console.error('Error connecting to Redis:', err);
});

/**
 * Service class for managing scraper state in Redis
 */
class ScraperStateService {
  private readonly keyPrefix: string = 'scraper:state:';
  private readonly expiryTime: number = 86400; // 24 hours in seconds

  /**
   * Stores the state of a scraping job in Redis
   * @param jobId - Unique identifier for the scraping job
   * @param state - The state object to store
   * @returns Promise resolving to true if successful
   */
  async storeState(jobId: string, state: any): Promise<boolean> {
    try {
      const key = this.getStateKey(jobId);
      await redisClient.set(key, JSON.stringify(state), 'EX', this.expiryTime);
      return true;
    } catch (error) {
      console.error(`Error storing state for job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Retrieves the state of a scraping job from Redis
   * @param jobId - Unique identifier for the scraping job
   * @returns Promise resolving to the stored state or null if not found
   */
  async getState<T = any>(jobId: string): Promise<T | null> {
    try {
      const key = this.getStateKey(jobId);
      const stateJson = await redisClient.get(key);
      
      if (!stateJson) {
        return null;
      }
      
      return JSON.parse(stateJson) as T;
    } catch (error) {
      console.error(`Error retrieving state for job ${jobId}:`, error);
      return null;
    }
  }

  /**
   * Updates a specific field in the state
   * @param jobId - Unique identifier for the scraping job
   * @param field - The field to update
   * @param value - The new value
   * @returns Promise resolving to true if successful
   */
  async updateStateField(jobId: string, field: string, value: any): Promise<boolean> {
    try {
      const state = await this.getState(jobId);
      
      if (!state) {
        return false;
      }
      
      const updatedState = {
        ...state,
        [field]: value
      };
      
      return await this.storeState(jobId, updatedState);
    } catch (error) {
      console.error(`Error updating state field for job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Stores the checkpoint information for a Lambda continuation
   * @param jobId - Unique identifier for the scraping job
   * @param checkpointKey - The S3/MinIO key for the stored checkpoint data
   * @param checkpoint - Additional checkpoint metadata
   * @returns Promise resolving to true if successful
   */
  async storeCheckpoint(
    jobId: string, 
    checkpointKey: string, 
    checkpoint: { 
      timestamp: number, 
      iterationCount: number,
      executionTimeMs: number 
    }
  ): Promise<boolean> {
    try {
      const key = this.getCheckpointKey(jobId);
      const checkpoints = await this.getCheckpoints(jobId) || [];
      
      checkpoints.push({
        key: checkpointKey,
        ...checkpoint
      });
      
      await redisClient.set(key, JSON.stringify(checkpoints), 'EX', this.expiryTime);
      return true;
    } catch (error) {
      console.error(`Error storing checkpoint for job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Retrieves the checkpoint information for a job
   * @param jobId - Unique identifier for the scraping job
   * @returns Promise resolving to an array of checkpoint metadata or null
   */
  async getCheckpoints(jobId: string): Promise<Array<{
    key: string,
    timestamp: number,
    iterationCount: number,
    executionTimeMs: number
  }> | null> {
    try {
      const key = this.getCheckpointKey(jobId);
      const checkpointsJson = await redisClient.get(key);
      
      if (!checkpointsJson) {
        return null;
      }
      
      return JSON.parse(checkpointsJson);
    } catch (error) {
      console.error(`Error retrieving checkpoints for job ${jobId}:`, error);
      return null;
    }
  }

  /**
   * Stores information about a scraping run
   * @param jobId - Unique identifier for the scraping job
   * @param workflow - The workflow being executed
   * @param options - Additional options for the run
   * @returns Promise resolving to true if successful
   */
  async storeJobInfo(
    jobId: string, 
    workflow: WorkflowFile, 
    options?: any
  ): Promise<boolean> {
    try {
      const key = this.getJobInfoKey(jobId);
      const jobInfo = {
        jobId,
        workflow,
        options,
        startedAt: Date.now(),
        status: 'running'
      };
      
      await redisClient.set(key, JSON.stringify(jobInfo), 'EX', this.expiryTime);
      return true;
    } catch (error) {
      console.error(`Error storing job info for job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Updates the status of a scraping job
   * @param jobId - Unique identifier for the scraping job
   * @param status - The new status
   * @param additionalData - Any additional data to store
   * @returns Promise resolving to true if successful
   */
  async updateJobStatus(
    jobId: string, 
    status: 'running' | 'paused' | 'checkpointing' | 'completed' | 'failed' | 'stopped',
    additionalData?: any
  ): Promise<boolean> {
    try {
      const key = this.getJobInfoKey(jobId);
      const jobInfoJson = await redisClient.get(key);
      
      if (!jobInfoJson) {
        return false;
      }
      
      const jobInfo = JSON.parse(jobInfoJson);
      
      const updatedJobInfo = {
        ...jobInfo,
        status,
        lastUpdated: Date.now(),
        ...(additionalData ? additionalData : {})
      };
      
      if (status === 'completed' || status === 'failed' || status === 'stopped') {
        updatedJobInfo.finishedAt = Date.now();
      }
      
      await redisClient.set(key, JSON.stringify(updatedJobInfo), 'EX', this.expiryTime);
      return true;
    } catch (error) {
      console.error(`Error updating job status for job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Gets information about a scraping job
   * @param jobId - Unique identifier for the scraping job
   * @returns Promise resolving to the job info or null
   */
  async getJobInfo(jobId: string): Promise<any | null> {
    try {
      const key = this.getJobInfoKey(jobId);
      const jobInfoJson = await redisClient.get(key);
      
      if (!jobInfoJson) {
        return null;
      }
      
      return JSON.parse(jobInfoJson);
    } catch (error) {
      console.error(`Error retrieving job info for job ${jobId}:`, error);
      return null;
    }
  }

  /**
   * Stores a WebSocket event for a job
   * @param jobId - Unique identifier for the scraping job
   * @param event - The event name
   * @param data - The event data
   * @returns Promise resolving to true if successful
   */
  async storeSocketEvent(jobId: string, event: string, data: any): Promise<boolean> {
    try {
      const key = this.getSocketEventsKey(jobId);
      const eventsJson = await redisClient.get(key);
      
      const events = eventsJson ? JSON.parse(eventsJson) : [];
      
      events.push({
        event,
        data,
        timestamp: Date.now()
      });
      
      await redisClient.set(key, JSON.stringify(events), 'EX', this.expiryTime);
      return true;
    } catch (error) {
      console.error(`Error storing socket event for job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Retrieves and clears socket events for a job
   * @param jobId - Unique identifier for the scraping job
   * @returns Promise resolving to an array of events or null
   */
  async getAndClearSocketEvents(jobId: string): Promise<Array<{
    event: string,
    data: any,
    timestamp: number
  }> | null> {
    try {
      const key = this.getSocketEventsKey(jobId);
      const eventsJson = await redisClient.get(key);
      
      if (!eventsJson) {
        return null;
      }
      
      const events = JSON.parse(eventsJson);
      
      // Clear the events
      await redisClient.del(key);
      
      return events;
    } catch (error) {
      console.error(`Error retrieving and clearing socket events for job ${jobId}:`, error);
      return null;
    }
  }

  /**
   * Subscribes a socket to a job
   * @param jobId - Unique identifier for the scraping job
   * @param socketId - The socket ID to subscribe
   * @returns Promise resolving to true if successful
   */
  async subscribeSocketToJob(jobId: string, socketId: string): Promise<boolean> {
    try {
      const key = this.getJobSubscribersKey(jobId);
      await redisClient.sadd(key, socketId);
      await redisClient.expire(key, this.expiryTime);
      return true;
    } catch (error) {
      console.error(`Error subscribing socket ${socketId} to job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Unsubscribes a socket from a job
   * @param jobId - Unique identifier for the scraping job
   * @param socketId - The socket ID to unsubscribe
   * @returns Promise resolving to true if successful
   */
  async unsubscribeSocketFromJob(jobId: string, socketId: string): Promise<boolean> {
    try {
      const key = this.getJobSubscribersKey(jobId);
      await redisClient.srem(key, socketId);
      return true;
    } catch (error) {
      console.error(`Error unsubscribing socket ${socketId} from job ${jobId}:`, error);
      return false;
    }
  }

  /**
   * Gets all sockets subscribed to a job
   * @param jobId - Unique identifier for the scraping job
   * @returns Promise resolving to an array of socket IDs
   */
  async getJobSubscribers(jobId: string): Promise<string[]> {
    try {
      const key = this.getJobSubscribersKey(jobId);
      return await redisClient.smembers(key);
    } catch (error) {
      console.error(`Error getting subscribers for job ${jobId}:`, error);
      return [];
    }
  }

  /**
   * Gets all active jobs
   * @returns Promise resolving to an array of job IDs
   */
  async getActiveJobs(): Promise<string[]> {
    try {
      const pattern = `${this.keyPrefix}job:*`;
      const keys = await redisClient.keys(pattern);
      
      return keys.map(key => {
        const parts = key.split(':');
        return parts[parts.length - 1];
      });
    } catch (error) {
      console.error('Error getting active jobs:', error);
      return [];
    }
  }

  // Helper methods for key generation
  private getStateKey(jobId: string): string {
    return `${this.keyPrefix}state:${jobId}`;
  }

  private getCheckpointKey(jobId: string): string {
    return `${this.keyPrefix}checkpoints:${jobId}`;
  }

  private getJobInfoKey(jobId: string): string {
    return `${this.keyPrefix}job:${jobId}`;
  }

  private getSocketEventsKey(jobId: string): string {
    return `${this.keyPrefix}socket-events:${jobId}`;
  }

  private getJobSubscribersKey(jobId: string): string {
    return `${this.keyPrefix}subscribers:${jobId}`;
  }
}

export { redisClient, ScraperStateService };