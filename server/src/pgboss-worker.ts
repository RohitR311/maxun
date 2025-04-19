/**
 * Direct run execution without PgBoss, supporting checkpoint-based scraping
 * Updated to work with Redis-backed BrowserPool and asynchronous operations
 */
import logger from './logger';
import {
  createRemoteBrowserForRun,
  destroyRemoteBrowser,
  interpretWholeWorkflow,
  stopRunningInterpretation,
  getRemoteBrowserRemainingTime,
} from './browser-management/controller';
import { WorkflowFile } from 'maxun-core';
import Run from './models/Run';
import Robot from './models/Robot';
import { browserPool } from './server';
import { Page } from 'playwright';
import { BinaryOutputService, ScrapingStateService } from './storage/mino';
import { capture } from './utils/analytics';
import { googleSheetUpdateTasks, processGoogleSheetUpdates } from './workflow-management/integrations/gsheet';
import { airtableUpdateTasks, processAirtableUpdates } from './workflow-management/integrations/airtable';
import { RemoteBrowser } from './browser-management/classes/RemoteBrowser';
import { io as serverIo } from "./server";
import PgBoss, { Job } from 'pg-boss';
import { redisClient } from './storage/connection';

// Redis keys for tracking run execution
const REDIS_KEYS = {
  RUN_STATUS: (runId: string) => `run:${runId}:status`,
  RUN_EXECUTION: (runId: string) => `run:${runId}:execution`,
  RUN_QUEUE: 'runs:queue',
  USER_RUNS: (userId: string) => `user:${userId}:runs`,
};

const pgBossConnectionString = `postgres://${process.env.DB_USER}:${process.env.DB_PASSWORD}@${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_NAME}`;
const pgBoss = new PgBoss({connectionString: pgBossConnectionString });

interface ExecuteRunData {
  userId: string;
  runId: string;
  browserId: string;
}

function AddGeneratedFlags(workflow: WorkflowFile) {
  const copy = JSON.parse(JSON.stringify(workflow));
  for (let i = 0; i < workflow.workflow.length; i++) {
    copy.workflow[i].what.unshift({
      action: 'flag',
      args: ['generated'],
    });
  }
  return copy;
}

/**
 * Function to reset browser state without creating a new browser
 */
async function resetBrowserState(browser: RemoteBrowser): Promise<boolean> {
  try {
    const currentPage = browser.getCurrentPage();
    if (!currentPage) {
      logger.log('error', 'No current page available to reset browser state');
      return false;
    }
    
    // Navigate to blank page to reset state
    await currentPage.goto('about:blank');
    
    // Clear browser storage
    await currentPage.evaluate(() => {
      try {
        localStorage.clear();
        sessionStorage.clear();
      } catch (e) {
        // Ignore errors in cleanup
      }
    });
    
    // Clear cookies
    const context = currentPage.context();
    await context.clearCookies();
    
    return true;
  } catch (error) {
    logger.log('error', `Failed to reset browser state`);
    return false;
  }
}

/**
 * Check for queued runs and process them
 */
async function checkAndProcessQueuedRun(userId: string, browserId: string): Promise<boolean> {
  try {
    // Find the oldest queued run for this specific browser
    const queuedRun = await Run.findOne({
      where: {
        browserId: browserId,
        runByUserId: userId,
        status: 'queued'
      },
      order: [['startedAt', 'ASC']]
    });
    
    if (!queuedRun) {
      logger.log('info', `No queued runs found for browser ${browserId}`);
      return false;
    }
    
    // Reset the browser state before next run
    const browser = await browserPool.getRemoteBrowser(browserId);
    if (browser) {
      logger.log('info', `Resetting browser state for browser ${browserId} before next run`);
      await resetBrowserState(browser);
    }
    
    // Update the queued run to running status
    await queuedRun.update({
      status: 'running',
      log: 'Run started - using browser from previous run'
    });
    
    // Update run status in Redis
    await redisClient.set(REDIS_KEYS.RUN_STATUS(queuedRun.runId), 'running');
    
    // Process the run directly
    processRunExecution({
      userId: userId,
      runId: queuedRun.runId,
      browserId: browserId
    });
    
    logger.log('info', `Started processing queued run ${queuedRun.runId} using browser ${browserId}`);
    return true;
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.log('error', `Error checking for queued runs: ${errorMessage}`);
    return false;
  }
}

/**
 * Process a run execution directly (no pgBoss)
 */
export async function processRunExecution(data: ExecuteRunData) {
  try {
    logger.log('info', `Processing run execution for runId: ${data.runId}, browserId: ${data.browserId}`);
    
    // Store execution status in Redis
    await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
      status: 'running',
      userId: data.userId,
      browserId: data.browserId,
      startedAt: Date.now().toString()
    });
    
    // Find the run
    const run = await Run.findOne({ where: { runId: data.runId } });
    if (!run) {
      logger.log('error', `Run ${data.runId} not found in database`);
      
      // Clean up Redis data
      await redisClient.del(REDIS_KEYS.RUN_EXECUTION(data.runId));
      await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'failed');
      
      return { success: false };
    }

    const plainRun = run.toJSON();

    // Find the recording
    const recording = await Robot.findOne({ where: { 'recording_meta.id': plainRun.robotMetaId }, raw: true });
    if (!recording) {
      logger.log('error', `Recording for run ${data.runId} not found`);
      
      // Update run status to failed
      await run.update({
        status: 'failed',
        finishedAt: new Date().toLocaleString(),
        log: 'Failed: Recording not found',
      });
      
      // Update Redis status
      await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'failed');
      await redisClient.del(REDIS_KEYS.RUN_EXECUTION(data.runId));
      
      // Check for queued runs even if this one failed
      await checkAndProcessQueuedRun(data.userId, data.browserId);
      
      return { success: false };
    }

    // Get the browser and execute the run
    const browser = await browserPool.getRemoteBrowser(plainRun.browserId);
    let currentPage = browser?.getCurrentPage();
    
    if (!browser || !currentPage) {
      logger.log('error', `Browser or page not available for run ${data.runId}`);
      
      // Update run status to failed
      await run.update({
        status: 'failed',
        finishedAt: new Date().toLocaleString(),
        log: 'Failed: Browser or page not available',
      });
      
      // Update Redis status
      await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'failed');
      await redisClient.del(REDIS_KEYS.RUN_EXECUTION(data.runId));
      
      // Even if this run failed, check for queued runs
      await checkAndProcessQueuedRun(data.userId, data.browserId);
      
      return { success: false };
    }

    try {
      // Check if browser session has enough time remaining
      const remainingTime = await getRemoteBrowserRemainingTime(plainRun.browserId);
      if (remainingTime !== null && remainingTime < 60000) { // Less than 1 minute
        logger.log('error', `Browser session will expire soon for run ${data.runId}`);
        
        // Update run status to failed
        await run.update({
          status: 'failed',
          finishedAt: new Date().toLocaleString(),
          log: 'Failed: Browser session will expire soon',
        });
        
        // Update Redis status
        await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'failed');
        await redisClient.del(REDIS_KEYS.RUN_EXECUTION(data.runId));
        
        return { success: false };
      }
      
      // Reset the browser state before executing this run
      await resetBrowserState(browser);
      
      // Add runId to interpreter settings for checkpoint-based scraping
      plainRun.interpreterSettings.runId = data.runId;
      
      // Execute the workflow
      const workflow = AddGeneratedFlags(recording.recording);
      const interpretationInfo = await browser.interpreter.InterpretRecording(
        workflow, 
        currentPage, 
        (newPage: Page) => currentPage = newPage, 
        plainRun.interpreterSettings
      );
      
      // Process the results
      const binaryOutputService = new BinaryOutputService('maxun-run-screenshots');
      const uploadedBinaryOutput = await binaryOutputService.uploadAndStoreBinaryOutput(run, interpretationInfo.binaryOutput);
      
      // Update the run record with results
      await run.update({
        status: 'success',
        finishedAt: new Date().toLocaleString(),
        browserId: plainRun.browserId,
        log: interpretationInfo.log.join('\n'),
        serializableOutput: interpretationInfo.serializableOutput,
        binaryOutput: uploadedBinaryOutput,
      });
      
      // Update Redis status
      await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'success');
      await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
        status: 'completed',
        finishedAt: Date.now().toString()
      });

      // Track extraction metrics
      let totalRowsExtracted = 0;
      let extractedScreenshotsCount = 0;
      let extractedItemsCount = 0;

      if (run.dataValues.binaryOutput && run.dataValues.binaryOutput["item-0"]) {
        extractedScreenshotsCount = 1;
      }

      if (run.dataValues.serializableOutput && run.dataValues.serializableOutput["item-0"]) {
        const itemsArray = run.dataValues.serializableOutput["item-0"];
        extractedItemsCount = itemsArray.length;

        totalRowsExtracted = itemsArray.reduce((total, item) => {
          return total + Object.keys(item).length;
        }, 0);
      }

      console.log(`Extracted Items Count: ${extractedItemsCount}`);
      console.log(`Extracted Screenshots Count: ${extractedScreenshotsCount}`);
      console.log(`Total Rows Extracted: ${totalRowsExtracted}`);
      
      // Capture metrics
      capture(
        'maxun-oss-run-created-manual',
        {
          runId: data.runId,
          user_id: data.userId,
          created_at: new Date().toISOString(),
          status: 'success',
          totalRowsExtracted,
          extractedItemsCount,
          extractedScreenshotsCount,
        }
      );

      // Schedule updates for Google Sheets and Airtable
      try {
        googleSheetUpdateTasks[plainRun.runId] = {
          robotId: plainRun.robotMetaId,
          runId: plainRun.runId,
          status: 'pending',
          retries: 5,
        };

        airtableUpdateTasks[plainRun.runId] = {
          robotId: plainRun.robotMetaId,
          runId: plainRun.runId,
          status: 'pending',
          retries: 5,
        };

        processAirtableUpdates();
        processGoogleSheetUpdates();
      } catch (err: any) {
        logger.log('error', `Failed to update Google Sheet for run: ${plainRun.runId}: ${err.message}`);
      }

      serverIo.of(plainRun.browserId).emit('run-completed', {
        runId: data.runId,
        robotMetaId: plainRun.robotMetaId,
        robotName: recording.recording_meta.name,
        status: 'success',
        finishedAt: new Date().toLocaleString()
      });
      
      // Check for and process queued runs before destroying the browser
      const queuedRunProcessed = await checkAndProcessQueuedRun(data.userId, plainRun.browserId);
      
      // Only destroy the browser if no queued run was found
      if (!queuedRunProcessed) {
        await destroyRemoteBrowser(plainRun.browserId, data.userId);
        logger.log('info', `No queued runs found for browser ${plainRun.browserId}, browser destroyed`);
      }
      
      return { success: true };
    } catch (executionError: any) {
      logger.log('error', `Run execution failed for run ${data.runId}: ${executionError.message}`);
      
      await run.update({
        status: 'failed',
        finishedAt: new Date().toLocaleString(),
        log: `Failed: ${executionError.message}`,
      });
      
      // Update Redis status
      await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'failed');
      await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
        status: 'failed',
        error: executionError.message,
        finishedAt: Date.now().toString()
      });
      
      // Check for queued runs before destroying the browser
      const queuedRunProcessed = await checkAndProcessQueuedRun(data.userId, plainRun.browserId);
      
      // Only destroy the browser if no queued run was found
      if (!queuedRunProcessed) {
        try {
          await destroyRemoteBrowser(plainRun.browserId, data.userId);
          logger.log('info', `No queued runs found for browser ${plainRun.browserId}, browser destroyed`);
        } catch (cleanupError: any) {
          logger.log('warn', `Failed to clean up browser for failed run ${data.runId}: ${cleanupError.message}`);
        }
      }

      // Capture failure metrics
      capture(
        'maxun-oss-run-created-manual',
        {
          runId: data.runId,
          user_id: data.userId,
          created_at: new Date().toISOString(),
          status: 'failed',
          error_message: executionError.message,
        }
      );
      
      return { success: false };
    }
    
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.log('error', `Failed to process run execution: ${errorMessage}`);
    
    // Update Redis status on top-level error
    try {
      await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'failed');
      await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
        status: 'failed',
        error: errorMessage,
        finishedAt: Date.now().toString()
      });
    } catch (redisError) {
      logger.log('error', `Failed to update Redis on run error: ${redisError}`);
    }
    
    return { success: false };
  }
}

/**
 * Handle browser initialization (no pgBoss)
 */
export async function initializeRemoteBrowserForRun(userId: string): Promise<string> {
  try {
    logger.log('info', `Starting browser initialization for user: ${userId}`);
    const browserId = await createRemoteBrowserForRun(userId);
    logger.log('info', `Browser initialized with ID: ${browserId}`);
    return browserId;
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.log('error', `Browser initialization failed: ${errorMessage}`);
    throw error;
  }
}

/**
 * Handle browser destruction (no pgBoss)
 */
export async function destroyRemoteBrowserForRun(browserId: string, userId: string): Promise<boolean> {
  try {
    logger.log('info', `Starting browser destruction for browser: ${browserId}`);
    const success = await destroyRemoteBrowser(browserId, userId);
    logger.log('info', `Browser destruction completed with result: ${success}`);
    return success;
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.log('error', `Destroy browser failed: ${errorMessage}`);
    throw error;
  }
}

/**
 * Handle workflow interpretation (no pgBoss)
 */
export async function interpretWorkflowForRun(userId: string): Promise<boolean> {
  try {
    logger.log('info', 'Starting workflow interpretation');
    await interpretWholeWorkflow(userId);
    logger.log('info', 'Workflow interpretation completed');
    return true;
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.log('error', `Interpret workflow failed: ${errorMessage}`);
    throw error;
  }
}

/**
 * Handle stopping workflow interpretation (no pgBoss)
 */
export async function stopWorkflowInterpretation(userId: string): Promise<boolean> {
  try {
    logger.log('info', 'Starting stop interpretation');
    await stopRunningInterpretation(userId);
    logger.log('info', 'Stop interpretation completed');
    return true;
  } catch (error: unknown) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.log('error', `Stop interpretation failed: ${errorMessage}`);
    throw error;
  }
}

/**
 * Get run status from Redis
 */
export async function getRunStatus(runId: string): Promise<string | null> {
  try {
    return await redisClient.get(REDIS_KEYS.RUN_STATUS(runId));
  } catch (error) {
    logger.log('error', `Failed to get run status from Redis: ${error}`);
    return null;
  }
}

/**
 * Get run execution details from Redis
 */
export async function getRunExecutionDetails(runId: string): Promise<Record<string, string> | null> {
  try {
    return await redisClient.hgetall(REDIS_KEYS.RUN_EXECUTION(runId));
  } catch (error) {
    logger.log('error', `Failed to get run execution details from Redis: ${error}`);
    return null;
  }
}

// Initialize direct execution system (no workers needed without pgBoss)
logger.log('info', 'Initializing direct run execution system with Redis tracking');

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  logger.log('info', 'SIGTERM received, cleaning up and shutting down...');
  
  try {
    // Clean up any active run executions in Redis
    const activeRuns = await redisClient.keys('run:*:execution');
    for (const runKey of activeRuns) {
      const runId = runKey.split(':')[1];
      await redisClient.hmset(runKey, {
        status: 'aborted',
        error: 'Server shutdown',
        finishedAt: Date.now().toString()
      });
      await redisClient.set(REDIS_KEYS.RUN_STATUS(runId), 'aborted');
    }
  } catch (error) {
    logger.log('error', `Error during shutdown cleanup: ${error}`);
  }
  
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.log('info', 'SIGINT received, cleaning up and shutting down...');
  
  try {
    // Clean up any active run executions in Redis
    const activeRuns = await redisClient.keys('run:*:execution');
    for (const runKey of activeRuns) {
      const runId = runKey.split(':')[1];
      await redisClient.hmset(runKey, {
        status: 'aborted',
        error: 'Server shutdown',
        finishedAt: Date.now().toString()
      });
      await redisClient.set(REDIS_KEYS.RUN_STATUS(runId), 'aborted');
    }
  } catch (error) {
    logger.log('error', `Error during shutdown cleanup: ${error}`);
  }
  
  process.exit(0);
});

// Export the functions for use in other files
export { pgBoss, checkAndProcessQueuedRun };