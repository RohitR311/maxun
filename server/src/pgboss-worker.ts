import logger from './logger';
import {
  createRemoteBrowserForRun,
  destroyRemoteBrowser,
  interpretWholeWorkflow,
  stopRunningInterpretation,
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
import { redisClient } from './storage/connection';
import { io } from "socket.io-client";

const REDIS_KEYS = {
  RUN_STATUS: (runId: string) => `run:${runId}:status`,
  RUN_EXECUTION: (runId: string) => `run:${runId}:execution`,
  RUN_QUEUE: 'runs:queue',
  USER_RUNS: (userId: string) => `user:${userId}:runs`,
};

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
    
    const browser = await browserPool.getRemoteBrowser(browserId);
    if (browser) {
      logger.log('info', `Resetting browser state for browser ${browserId} before next run`);
      await resetBrowserState(browser);
    }
    
    await queuedRun.update({
      status: 'running',
      log: 'Run started - using browser from previous run'
    });
    
    await redisClient.set(REDIS_KEYS.RUN_STATUS(queuedRun.runId), 'running');
    
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

export async function processRunExecution(data: ExecuteRunData) {
  try {
    logger.log('info', `Processing run execution for runId: ${data.runId}, browserId: ${data.browserId}`);
    
    await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
      status: 'running',
      userId: data.userId,
      browserId: data.browserId,
      startedAt: Date.now().toString()
    });
    
    const run = await Run.findOne({ where: { runId: data.runId } });
    if (!run) {
      logger.log('error', `Run ${data.runId} not found in database`);
      
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

    const MAX_BROWSER_RECREATIONS = 10;
    let browserRecreationCount = 0;
    let currentBrowserId = data.browserId;
    let isScrapingComplete = false;
    let allLogs: string[] = [];
    let accumulatedSerializableOutput = {};
    let accumulatedBinaryOutput = {};
  
    const scrapingStateService = new ScrapingStateService();

    const workflow = AddGeneratedFlags(recording.recording);
    
    while (!isScrapingComplete && browserRecreationCount < MAX_BROWSER_RECREATIONS) {
      try {
        logger.log('info', `Run ${data.runId}: Browser iteration ${browserRecreationCount + 1} with browser ${currentBrowserId}`);
        
        // Function to wait for the browser to be ready
        const waitForBrowserReady = (browserId: string): Promise<void> => {
          return new Promise((resolve, reject) => {
            const socket = io(`${process.env.BACKEND_URL ? process.env.BACKEND_URL : 'http://localhost:8080'}/${browserId}`, {
              transports: ['websocket'],
              rejectUnauthorized: false
            });

            let isResolved = false;
            
            // Listen for the ready-for-run event
            socket.on('ready-for-run', () => {
              if (!isResolved) {
                isResolved = true;
                logger.log('info', `Browser ${browserId} ready for run ${data.runId}`);
                resolve();
              }
            });
            
            // Also check if the browser already exists in the pool
            browserPool.getRemoteBrowser(browserId).then(browser => {
              if (browser && !isResolved) {
                isResolved = true;
                logger.log('info', `Browser ${browserId} already exists in pool for run ${data.runId}`);
                resolve();
              }
            });
            
            // Set timeout to avoid hanging forever
            setTimeout(() => {
              if (!isResolved) {
                isResolved = true;
                reject(new Error(`Timeout waiting for browser ${browserId} to be ready`));
              }
            }, 30000); // 30 second timeout
          });
        };
        
        // Wait for the current browser to be ready or create a new one
        let browser = await browserPool.getRemoteBrowser(currentBrowserId);
        
        if (browser) {
          logger.log('info', `Found existing browser ${currentBrowserId} for run ${data.runId}`);
        } else {
          logger.log('info', `Browser ${currentBrowserId} not found for run ${data.runId}, waiting for it to be ready or creating new one`);
          
          try {
            // Try to wait for the existing browser ID to become ready
            await waitForBrowserReady(currentBrowserId);
            browser = await browserPool.getRemoteBrowser(currentBrowserId);
          } catch (timeoutError: any) {
            logger.log('warn', `Timeout waiting for browser ${currentBrowserId}: ${timeoutError.message}`);
            
            // Create a new browser since the current one didn't become ready
            currentBrowserId = await createRemoteBrowserForRun(data.userId);
            logger.log('info', `Created new browser ${currentBrowserId} for run ${data.runId}`);
            
            // Update run with new browserId
            await run.update({ browserId: currentBrowserId });
            
            // Wait for this new browser to be ready
            try {
              await waitForBrowserReady(currentBrowserId);
            } catch (newBrowserError: any) {
              throw new Error(`New browser ${currentBrowserId} also failed to become ready: ${newBrowserError.message}`);
            }
            
            browser = await browserPool.getRemoteBrowser(currentBrowserId);
            browserRecreationCount++;
          }
        }
        
        if (!browser) {
          throw new Error(`Failed to get browser ${currentBrowserId} after waiting for ready event`);
        }
        
        // Get the current page
        let currentPage = browser.getCurrentPage();
        if (!currentPage) {
          throw new Error('No current page available for browser');
        }
        
        // Reset the browser state before executing this run
        await resetBrowserState(browser);
        
        // Add runId to interpreter settings for checkpoint-based scraping
        plainRun.interpreterSettings.runId = data.runId;
        
        // Execute the workflow
        const interpretationInfo = await browser.interpreter.InterpretRecording(
          workflow, 
          currentPage, 
          (newPage: Page) => currentPage = newPage, 
          plainRun.interpreterSettings
        );
        
        // Check if scraping is complete
        isScrapingComplete = interpretationInfo.scrapingCompleted === true;
        
        // Add logs from this browser iteration
        allLogs = [...allLogs, ...interpretationInfo.log];
        
        // Merge serializable and binary outputs
        accumulatedSerializableOutput = {
          ...accumulatedSerializableOutput,
          ...interpretationInfo.serializableOutput
        };
        
        accumulatedBinaryOutput = {
          ...accumulatedBinaryOutput,
          ...interpretationInfo.binaryOutput
        };
        
        // Update the run record with intermediate results
        await run.update({
          status: isScrapingComplete ? 'success' : 'running',
          browserId: currentBrowserId,
          log: allLogs.join('\n'),
          serializableOutput: accumulatedSerializableOutput
        });
        
        // Update Redis status
        if (isScrapingComplete) {
          // Process the final results
          const binaryOutputService = new BinaryOutputService('maxun-run-screenshots');
          const uploadedBinaryOutput = await binaryOutputService.uploadAndStoreBinaryOutput(run, accumulatedBinaryOutput);
          
          // Update the run with final results
          await run.update({
            status: 'success',
            finishedAt: new Date().toLocaleString(),
            binaryOutput: uploadedBinaryOutput,
          });
          
          // Update Redis status
          await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'success');
          await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
            status: 'completed',
            finishedAt: Date.now().toString()
          });
          
          // Clean up any checkpoints since we're done
          await scrapingStateService.deleteAllScrapingCheckpoints(data.runId);
          
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

          logger.log('info', `Extracted Items Count: ${extractedItemsCount}`);
          logger.log('info', `Extracted Screenshots Count: ${extractedScreenshotsCount}`);
          logger.log('info', `Total Rows Extracted: ${totalRowsExtracted}`);
          
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
              browserRecreations: browserRecreationCount,
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

          serverIo.of(currentBrowserId).emit('run-completed', {
            runId: data.runId,
            robotMetaId: plainRun.robotMetaId,
            robotName: recording.recording_meta.name,
            status: 'success',
            finishedAt: new Date().toLocaleString(),
            browserIterations: browserRecreationCount + 1
          });
          
          logger.log('info', `Run ${data.runId} completed successfully after ${browserRecreationCount + 1} browser iterations`);
        } else {
          // Let the client know about progress
          await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
            status: 'running',
            browserId: currentBrowserId,
            browserRecreationCount: browserRecreationCount.toString(),
            lastUpdated: Date.now().toString()
          });
          
          // Prepare for next browser iteration
          logger.log('info', `Browser ${currentBrowserId} session ended, preparing for next browser iteration for run ${data.runId}`);
          
          // Try to destroy this browser gracefully
          try {
            await destroyRemoteBrowser(currentBrowserId, data.userId);
          } catch (destroyError: any) {
            logger.log('warn', `Failed to gracefully destroy browser ${currentBrowserId}: ${destroyError.message}`);
          }
          
          // Create a new browser for next iteration
          currentBrowserId = await createRemoteBrowserForRun(data.userId);
          browserRecreationCount++;
          
          // Update run with new browserId
          await run.update({ browserId: currentBrowserId });
        }
        
      } catch (executionError: any) {
        logger.log('error', `Browser ${currentBrowserId} encountered an error for run ${data.runId}: ${executionError.message}`);
        
        // Try to determine if this is a fatal error or if we can continue with a new browser
        const isFatalError = 
          executionError.message.includes("Recording not found") || 
          executionError.message.includes("Failed to initialize") ||
          (browserRecreationCount >= MAX_BROWSER_RECREATIONS) ||
          executionError.message.includes("Maximum recursion");
        
        if (isFatalError) {
          // This is a fatal error, fail the run
          await run.update({
            status: 'failed',
            finishedAt: new Date().toLocaleString(),
            log: [...allLogs, `Fatal error: ${executionError.message}`].join('\n'),
          });
          
          // Update Redis status
          await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'failed');
          await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
            status: 'failed',
            error: executionError.message,
            finishedAt: Date.now().toString()
          });
          
          // Capture failure metrics
          capture(
            'maxun-oss-run-created-manual',
            {
              runId: data.runId,
              user_id: data.userId,
              created_at: new Date().toISOString(),
              status: 'failed',
              error_message: executionError.message,
              browserRecreations: browserRecreationCount,
            }
          );
          
          // Exit the loop
          break;
        } else {
          allLogs.push(`Browser ${currentBrowserId} error: ${executionError.message}. Creating new browser.`);
          
          try {
            await destroyRemoteBrowser(currentBrowserId, data.userId);
          } catch (destroyError: any) {
            logger.log('warn', `Failed to gracefully destroy browser ${currentBrowserId}: ${destroyError.message}`);
          }
        
          try {
            currentBrowserId = await createRemoteBrowserForRun(data.userId);
            browserRecreationCount++;
            
            // Update run with new browserId and intermediate results
            await run.update({
              browserId: currentBrowserId,
              log: allLogs.join('\n'),
              serializableOutput: accumulatedSerializableOutput,
            });
            
            // Update execution info in Redis
            await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
              status: 'running',
              browserId: currentBrowserId,
              browserRecreationCount: browserRecreationCount.toString(),
              lastUpdated: Date.now().toString(),
            });
            
            logger.log('info', `Created new browser ${currentBrowserId} for run ${data.runId}, iteration ${browserRecreationCount}`);
          } catch (createError: any) {
            logger.log('error', `Failed to create new browser for run ${data.runId}: ${createError.message}`);
            
            // This is now a fatal error
            await run.update({
              status: 'failed',
              finishedAt: new Date().toLocaleString(),
              log: [...allLogs, `Failed to create new browser: ${createError.message}`].join('\n'),
            });
            
            // Update Redis status
            await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'failed');
            await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
              status: 'failed',
              error: createError.message,
              finishedAt: Date.now().toString()
            });
            
            // Exit the loop
            break;
          }
        }
      }
    }
    
    // Check if we've hit the maximum browser recreations without completing
    if (browserRecreationCount >= MAX_BROWSER_RECREATIONS && !isScrapingComplete) {
      await run.update({
        status: 'failed',
        finishedAt: new Date().toLocaleString(),
        log: [...allLogs, `Failed: Exceeded maximum browser recreations (${MAX_BROWSER_RECREATIONS})`].join('\n'),
      });
      
      // Update Redis status
      await redisClient.set(REDIS_KEYS.RUN_STATUS(data.runId), 'failed');
      await redisClient.hmset(REDIS_KEYS.RUN_EXECUTION(data.runId), {
        status: 'failed',
        error: `Exceeded maximum browser recreations (${MAX_BROWSER_RECREATIONS})`,
        finishedAt: Date.now().toString()
      });
    }
    
    // After all execution attempts, check for queued runs
    const queuedRunProcessed = await checkAndProcessQueuedRun(data.userId, currentBrowserId);
    
    // Only destroy the browser if no queued run was found
    if (!queuedRunProcessed) {
      try {
        await destroyRemoteBrowser(currentBrowserId, data.userId);
        logger.log('info', `No queued runs found for browser ${currentBrowserId}, browser destroyed`);
      } catch (cleanupError: any) {
        logger.log('warn', `Failed to clean up browser for run ${data.runId}: ${cleanupError.message}`);
      }
    }
    
    return { success: isScrapingComplete };
    
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

export { checkAndProcessQueuedRun };