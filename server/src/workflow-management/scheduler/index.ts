import { uuid } from "uuidv4";
import { chromium } from 'playwright-extra';
import stealthPlugin from 'puppeteer-extra-plugin-stealth';
import { io, Socket } from "socket.io-client";
import { createRemoteBrowserForRun, destroyRemoteBrowser } from '../../browser-management/controller';
import logger from '../../logger';
import { browserPool } from "../../server";
import { googleSheetUpdateTasks, processGoogleSheetUpdates } from "../integrations/gsheet";
import Robot from "../../models/Robot";
import Run from "../../models/Run";
import { getDecryptedProxyConfig } from "../../routes/proxy";
import { BinaryOutputService } from "../../storage/mino";
import { capture } from "../../utils/analytics";
import { WorkflowFile } from "maxun-core";
import { Page } from "playwright";
import { redisClient } from "../../storage/connection";
chromium.use(stealthPlugin());

// Redis keys for scheduling
const REDIS_KEYS = {
  SCHEDULED_RUNS: 'scheduled:runs',
  RUN_SCHEDULE: (runId: string) => `run:${runId}:schedule`,
  ROBOT_SCHEDULE: (robotId: string) => `robot:${robotId}:schedule`,
  USER_SCHEDULED_RUNS: (userId: string) => `user:${userId}:scheduled:runs`,
};

async function createWorkflowAndStoreMetadata(id: string, userId: string) {
  try {
    const recording = await Robot.findOne({
      where: {
        'recording_meta.id': id
      },
      raw: true
    });

    if (!recording || !recording.recording_meta || !recording.recording_meta.id) {
      return {
        success: false,
        error: 'Recording not found'
      };
    }

    const proxyConfig = await getDecryptedProxyConfig(userId);
    let proxyOptions: any = {};

    if (proxyConfig.proxy_url) {
      proxyOptions = {
        server: proxyConfig.proxy_url,
        ...(proxyConfig.proxy_username && proxyConfig.proxy_password && {
          username: proxyConfig.proxy_username,
          password: proxyConfig.proxy_password,
        }),
      };
    }

    // Create a browser asynchronously
    const browserId = await createRemoteBrowserForRun(userId);
    const runId = uuid();

    // Store schedule information in Redis
    await redisClient.sadd(REDIS_KEYS.SCHEDULED_RUNS, runId);
    await redisClient.sadd(REDIS_KEYS.USER_SCHEDULED_RUNS(userId), runId);
    await redisClient.hmset(REDIS_KEYS.RUN_SCHEDULE(runId), {
      robotId: id,
      userId,
      browserId,
      createdAt: Date.now().toString(),
      status: 'scheduled'
    });

    const run = await Run.create({
      status: 'scheduled',
      name: recording.recording_meta.name,
      robotId: recording.id,
      robotMetaId: recording.recording_meta.id,
      startedAt: new Date().toLocaleString(),
      finishedAt: '',
      browserId,
      interpreterSettings: { maxConcurrency: 1, maxRepeats: 1, debug: true },
      log: '',
      runId,
      runByScheduleId: uuid(),
      serializableOutput: {},
      binaryOutput: {},
    });

    const plainRun = run.toJSON();

    return {
      browserId,
      runId: plainRun.runId,
    }

  } catch (e) {
    const { message } = e as Error;
    logger.log('info', `Error while scheduling a run with id: ${id}`);
    console.log(`Error while scheduling a run with id: ${id}:`, message);
    return {
      success: false,
      error: message,
    };
  }
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
};

async function executeRun(id: string, userId: string) {
  try {
    const run = await Run.findOne({ where: { runId: id } });
    if (!run) {
      return {
        success: false,
        error: 'Run not found'
      }
    }

    const plainRun = run.toJSON();

    const recording = await Robot.findOne({ where: { 'recording_meta.id': plainRun.robotMetaId }, raw: true });
    if (!recording) {
      return {
        success: false,
        error: 'Recording not found'
      }
    }

    // Update run status in Redis and database
    await redisClient.hmset(REDIS_KEYS.RUN_SCHEDULE(id), {
      status: 'running',
      startedAt: Date.now().toString()
    });
    
    plainRun.status = 'running';
    await run.update({ status: 'running' });

    // Get browser asynchronously
    const browser = await browserPool.getRemoteBrowser(plainRun.browserId);
    if (!browser) {
      throw new Error('Could not access browser');
    }

    let currentPage = await browser.getCurrentPage();
    if (!currentPage) {
      throw new Error('Could not create a new page');
    }

    const workflow = AddGeneratedFlags(recording.recording);
    const interpretationInfo = await browser.interpreter.InterpretRecording(
      workflow, currentPage, (newPage: Page) => currentPage = newPage, plainRun.interpreterSettings
    );

    const binaryOutputService = new BinaryOutputService('maxun-run-screenshots');
    const uploadedBinaryOutput = await binaryOutputService.uploadAndStoreBinaryOutput(run, interpretationInfo.binaryOutput);

    await destroyRemoteBrowser(plainRun.browserId, userId);

    // Update run in database
    await run.update({
      ...run,
      status: 'success',
      finishedAt: new Date().toLocaleString(),
      browserId: plainRun.browserId,
      log: interpretationInfo.log.join('\n'),
      serializableOutput: interpretationInfo.serializableOutput,
      binaryOutput: uploadedBinaryOutput,
    });

    // Update Redis status
    await redisClient.hmset(REDIS_KEYS.RUN_SCHEDULE(id), {
      status: 'success',
      finishedAt: Date.now().toString()
    });

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

    capture(
      'maxun-oss-run-created-scheduled',
      {
        runId: id,
        created_at: new Date().toISOString(),
        status: 'success',
        totalRowsExtracted,
        extractedItemsCount,
        extractedScreenshotsCount,
      }
    );

    googleSheetUpdateTasks[id] = {
      robotId: plainRun.robotMetaId,
      runId: id,
      status: 'pending',
      retries: 5,
    };
    processGoogleSheetUpdates();
    return true;
  } catch (error: any) {
    logger.log('info', `Error while running a robot with id: ${id} - ${error.message}`);
    console.log(error.message);
    
    // Update run status to failed in database
    const run = await Run.findOne({ where: { runId: id } });
    if (run) {
      await run.update({
        status: 'failed',
        finishedAt: new Date().toLocaleString(),
      });
    }
    
    // Update Redis status
    await redisClient.hmset(REDIS_KEYS.RUN_SCHEDULE(id), {
      status: 'failed',
      error: error.message,
      finishedAt: Date.now().toString()
    });
    
    capture(
      'maxun-oss-run-created-scheduled',
      {
        runId: id,
        created_at: new Date().toISOString(),
        status: 'failed',
      }
    );
    return false;
  }
}

async function readyForRunHandler(browserId: string, id: string, userId: string) {
  try {
    const interpretation = await executeRun(id, userId);

    if (interpretation) {
      logger.log('info', `Interpretation of ${id} succeeded`);
    } else {
      logger.log('error', `Interpretation of ${id} failed`);
      await destroyRemoteBrowser(browserId, userId);
    }

    resetRecordingState(browserId, id);

  } catch (error: any) {
    logger.error(`Error during readyForRunHandler: ${error.message}`);
    
    // Update Redis status on error
    await redisClient.hmset(REDIS_KEYS.RUN_SCHEDULE(id), {
      status: 'failed',
      error: error.message,
      finishedAt: Date.now().toString()
    });
    
    await destroyRemoteBrowser(browserId, userId);
  }
}

function resetRecordingState(browserId: string, id: string) {
  browserId = '';
  id = '';
}

export async function handleRunRecording(id: string, userId: string) {
  try {
    const result = await createWorkflowAndStoreMetadata(id, userId);
    const { browserId, runId: newRunId } = result;

    if (!browserId || !newRunId || !userId) {
      throw new Error('browserId or runId or userId is undefined');
    }

    // Store the workflow execution mapping in Redis
    await redisClient.hmset(REDIS_KEYS.ROBOT_SCHEDULE(id), {
      runId: newRunId,
      userId,
      browserId,
      status: 'initializing',
      startedAt: Date.now().toString()
    });

    const socket = io(`${process.env.BACKEND_URL ? process.env.BACKEND_URL : 'http://localhost:8080'}/${browserId}`, {
      transports: ['websocket'],
      rejectUnauthorized: false
    });

    socket.on('ready-for-run', () => readyForRunHandler(browserId, newRunId, userId));

    logger.log('info', `Running robot: ${id}`);

    socket.on('disconnect', () => {
      cleanupSocketListeners(socket, browserId, newRunId, userId);
    });

  } catch (error: any) {
    logger.error('Error running recording:', error);
    
    // Clean up any Redis entries on error
    try {
      if (error.runId) {
        await redisClient.hmset(REDIS_KEYS.RUN_SCHEDULE(error.runId), {
          status: 'failed',
          error: error.message,
          finishedAt: Date.now().toString()
        });
      }
    } catch (redisError) {
      logger.error('Error updating Redis during error handling:', redisError);
    }
  }
}

function cleanupSocketListeners(socket: Socket, browserId: string, id: string, userId: string) {
  socket.off('ready-for-run', () => readyForRunHandler(browserId, id, userId));
  logger.log('info', `Cleaned up listeners for browserId: ${browserId}, runId: ${id}`);
}

/**
 * Schedule a workflow to run at specified intervals
 */
export async function scheduleWorkflow(robotId: string, userId: string, cronExpression: string, timezone: string): Promise<string> {
  try {
    const scheduleId = uuid();
    
    // Store schedule info in Redis
    await redisClient.hmset(REDIS_KEYS.ROBOT_SCHEDULE(robotId), {
      scheduleId,
      userId,
      cronExpression,
      timezone,
      status: 'active',
      createdAt: Date.now().toString()
    });
    
    logger.log('info', `Scheduled workflow ${robotId} with schedule ID ${scheduleId}`);
    
    return scheduleId;
  } catch (error) {
    logger.error(`Failed to schedule workflow: ${error}`);
    throw error;
  }
}

/**
 * Cancel a scheduled workflow
 */
export async function cancelScheduledWorkflow(robotId: string): Promise<boolean> {
  try {
    const scheduleExists = await redisClient.exists(REDIS_KEYS.ROBOT_SCHEDULE(robotId));
    
    if (scheduleExists) {
      await redisClient.hmset(REDIS_KEYS.ROBOT_SCHEDULE(robotId), {
        status: 'canceled',
        canceledAt: Date.now().toString()
      });
      
      logger.log('info', `Canceled scheduled workflow for robot ${robotId}`);
      return true;
    } else {
      logger.log('warn', `No schedule found for robot ${robotId}`);
      return false;
    }
  } catch (error) {
    logger.error(`Failed to cancel scheduled workflow: ${error}`);
    return false;
  }
}

/**
 * Get all scheduled runs for a user
 */
export async function getScheduledRunsForUser(userId: string): Promise<string[]> {
  try {
    return await redisClient.smembers(REDIS_KEYS.USER_SCHEDULED_RUNS(userId));
  } catch (error) {
    logger.error(`Failed to get scheduled runs for user ${userId}: ${error}`);
    return [];
  }
}

export { createWorkflowAndStoreMetadata };