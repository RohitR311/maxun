import { readFile, readFiles } from "../workflow-management/storage";
import { Router, Request, Response } from 'express';
import { chromium } from "playwright-extra";
import stealthPlugin from 'puppeteer-extra-plugin-stealth';
import { requireAPIKey } from "../middlewares/api";
import Robot from "../models/Robot";
import Run from "../models/Run";
const router = Router();
import { getDecryptedProxyConfig } from "../routes/proxy";
import { uuid } from "uuidv4";
import { createRemoteBrowserForRun, destroyRemoteBrowser, getRemoteBrowserRemainingTime } from "../browser-management/controller";
import logger from "../logger";
import { browserPool } from "../server";
import { io, Socket } from "socket.io-client";
import { BinaryOutputService } from "../storage/mino";
import { AuthenticatedRequest } from "../routes/record"
import {capture} from "../utils/analytics";
import { Page } from "playwright";
import { WorkflowFile } from "maxun-core";
import { redisClient } from "../storage/connection";
chromium.use(stealthPlugin());

// Redis keys for API requests
const REDIS_KEYS = {
  API_RUN: (runId: string) => `api:run:${runId}`,
  API_ROBOT_RUNS: (robotId: string) => `api:robot:${robotId}:runs`,
  API_USER_RUNS: (userId: string) => `api:user:${userId}:runs`,
  RUN_STATUS: (runId: string) => `run:${runId}:status`,
};

const formatRecording = (recordingData: any) => {
    const recordingMeta = recordingData.recording_meta;
    const workflow = recordingData.recording.workflow || [];
    const firstWorkflowStep = workflow[0]?.where?.url || '';

    const inputParameters = [
        {
            type: "string",
            name: "originUrl",
            label: "Origin URL",
            required: true,
            defaultValue: firstWorkflowStep,
        },
    ];

    return {
        id: recordingMeta.id,
        name: recordingMeta.name,
        createdAt: new Date(recordingMeta.createdAt).getTime(),
        inputParameters,
    };
};

/**
 * @swagger
 * /api/robots:
 *   get:
 *     summary: Get all robots
 *     description: Retrieve a list of all robots.
 *     security:
 *       - api_key: []
 *     responses:
 *       200:
 *         description: A list of robots.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 200
 *                 messageCode:
 *                   type: string
 *                   example: success
 *                 robots:
 *                   type: object
 *                   properties:
 *                     totalCount:
 *                       type: integer
 *                       example: 5
 *                     items:
 *                       type: array
 *                       items:
 *                         type: object
 *                         properties:
 *                           id:
 *                             type: string
 *                             example: "12345"
 *                           name:
 *                             type: string
 *                             example: "Sample Robot"
 *       500:
 *         description: Error retrieving robots.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 500
 *                 messageCode:
 *                   type: string
 *                   example: error
 *                 message:
 *                   type: string
 *                   example: "Failed to retrieve robots"
 */
router.get("/robots", requireAPIKey, async (req: Request, res: Response) => {
    try {
        const robots = await Robot.findAll({ raw: true });
        const formattedRecordings = robots.map(formatRecording);

        const response = {
            statusCode: 200,
            messageCode: "success",
            robots: {
                totalCount: formattedRecordings.length,
                items: formattedRecordings,
            },
        };

        res.status(200).json(response);
    } catch (error) {
        console.error("Error fetching robots:", error);
        res.status(500).json({
            statusCode: 500,
            messageCode: "error",
            message: "Failed to retrieve robots",
        });
    }
});


const formatRecordingById = (recordingData: any) => {
    const recordingMeta = recordingData.recording_meta;
    const workflow = recordingData.recording.workflow || [];
    const firstWorkflowStep = workflow[0]?.where?.url || '';

    const inputParameters = [
        {
            type: "string",
            name: "originUrl",
            label: "Origin URL",
            required: true,
            defaultValue: firstWorkflowStep,
        },
    ];

    return {
        id: recordingMeta.id,
        name: recordingMeta.name,
        createdAt: new Date(recordingMeta.createdAt).getTime(),
        inputParameters,
    };
};

/**
 * @swagger
 * /api/robots/{id}:
 *   get:
 *     summary: Get robot by ID
 *     description: Retrieve a robot by its ID.
 *     security:
 *       - api_key: []
 *     parameters:
 *       - in: path
 *         name: id
 *         schema:
 *           type: string
 *         required: true
 *         description: The ID of the robot to retrieve.
 *     responses:
 *       200:
 *         description: Robot details.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 200
 *                 messageCode:
 *                   type: string
 *                   example: success
 *                 robot:
 *                   type: object
 *                   properties:
 *                     id:
 *                       type: string
 *                       example: "12345"
 *                     name:
 *                       type: string
 *                       example: "Sample Robot"
 *       404:
 *         description: Robot not found.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 404
 *                 messageCode:
 *                   type: string
 *                   example: not_found
 *                 message:
 *                   type: string
 *                   example: "Recording with ID not found."
 */
router.get("/robots/:id", requireAPIKey, async (req: Request, res: Response) => {
    try {
        const robot = await Robot.findOne({
            where: {
                'recording_meta.id': req.params.id
            },
            raw: true
        });

        const formattedRecording = formatRecordingById(robot);

        const response = {
            statusCode: 200,
            messageCode: "success",
            robot: formattedRecording,
        };

        res.status(200).json(response);
    } catch (error) {
        console.error("Error fetching robot:", error);
        res.status(404).json({
            statusCode: 404,
            messageCode: "not_found",
            message: `Robot with ID "${req.params.id}" not found.`,
        });
    }
});

/**
 * @swagger
 * /api/robots/{id}/runs:
 *   get:
 *     summary: Get all runs for a robot
 *     description: Retrieve all runs associated with a specific robot.
 *     security:
 *       - api_key: []
 *     parameters:
 *       - in: path
 *         name: id
 *         schema:
 *           type: string
 *         required: true
 *         description: The ID of the robot.
 *     responses:
 *       200:
 *         description: A list of runs for the robot.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 200
 *                 messageCode:
 *                   type: string
 *                   example: success
 *                 runs:
 *                   type: object
 *                   properties:
 *                     totalCount:
 *                       type: integer
 *                       example: 5
 *                     items:
 *                       type: array
 *                       items:
 *                         type: object
 *                         properties:
 *                           runId:
 *                             type: string
 *                             example: "67890"
 *                           status:
 *                             type: string
 *                             example: "completed"
 *       500:
 *         description: Error retrieving runs.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 500
 *                 messageCode:
 *                   type: string
 *                   example: error
 *                 message:
 *                   type: string
 *                   example: "Failed to retrieve runs"
 */
router.get("/robots/:id/runs", requireAPIKey, async (req: Request, res: Response) => {
    try {
        const runs = await Run.findAll({
            where: {
                robotMetaId: req.params.id
            },
            raw: true
        });

        const formattedRuns = runs.map(formatRunResponse);

        const response = {
            statusCode: 200,
            messageCode: "success",
            runs: {
                totalCount: formattedRuns.length,
                items: formattedRuns,
            },
        };

        // Cache runs in Redis for quicker access
        await redisClient.set(
            REDIS_KEYS.API_ROBOT_RUNS(req.params.id), 
            JSON.stringify(formattedRuns),
            'EX',
            300 // Cache for 5 minutes
        );

        res.status(200).json(response);
    } catch (error) {
        console.error("Error fetching runs:", error);
        res.status(500).json({
            statusCode: 500,
            messageCode: "error",
            message: "Failed to retrieve runs",
        });
    }
});


function formatRunResponse(run: any) {
    const formattedRun = {
        id: run.id,
        status: run.status,
        name: run.name,
        robotId: run.robotMetaId, // Renaming robotMetaId to robotId
        startedAt: run.startedAt,
        finishedAt: run.finishedAt,
        runId: run.runId,
        runByUserId: run.runByUserId,
        runByScheduleId: run.runByScheduleId,
        runByAPI: run.runByAPI,
        data: {},
        screenshot: null,
    };

    if (run.serializableOutput && run.serializableOutput['item-0']) {
        formattedRun.data = run.serializableOutput['item-0'];
    } else if (run.binaryOutput && run.binaryOutput['item-0']) {
        formattedRun.screenshot = run.binaryOutput['item-0']; 
    }

    return formattedRun;
}


/**
 * @swagger
 * /api/robots/{id}/runs/{runId}:
 *   get:
 *     summary: Get a specific run by ID for a robot
 *     description: Retrieve details of a specific run by its ID.
 *     security:
 *       - api_key: []
 *     parameters:
 *       - in: path
 *         name: id
 *         schema:
 *           type: string
 *         required: true
 *         description: The ID of the robot.
 *       - in: path
 *         name: runId
 *         schema:
 *           type: string
 *         required: true
 *         description: The ID of the run.
 *     responses:
 *       200:
 *         description: Run details.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 200
 *                 messageCode:
 *                   type: string
 *                   example: success
 *                 run:
 *                   type: object
 *                   properties:
 *                     runId:
 *                       type: string
 *                       example: "67890"
 *                     status:
 *                       type: string
 *                       example: "completed"
 *       404:
 *         description: Run not found.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 404
 *                 messageCode:
 *                   type: string
 *                   example: not_found
 *                 message:
 *                   type: string
 *                   example: "Run with id not found."
 */
router.get("/robots/:id/runs/:runId", requireAPIKey, async (req: Request, res: Response) => {
    try {
        // Check Redis cache first
        const cachedRun = await redisClient.get(REDIS_KEYS.API_RUN(req.params.runId));
        
        if (cachedRun) {
            const response = {
                statusCode: 200,
                messageCode: "success",
                run: JSON.parse(cachedRun),
            };
            return res.status(200).json(response);
        }
        
        const run = await Run.findOne({
            where: {
                runId: req.params.runId,
                robotMetaId: req.params.id,
            },
            raw: true
        });

        const formattedRun = formatRunResponse(run);
        
        // Cache the formatted run in Redis
        await redisClient.set(
            REDIS_KEYS.API_RUN(req.params.runId), 
            JSON.stringify(formattedRun),
            'EX',
            300 // Cache for 5 minutes
        );

        const response = {
            statusCode: 200,
            messageCode: "success",
            run: formattedRun,
        };

        res.status(200).json(response);
    } catch (error) {
        console.error("Error fetching run:", error);
        res.status(404).json({
            statusCode: 404,
            messageCode: "not_found",
            message: `Run with id "${req.params.runId}" for robot with id "${req.params.id}" not found.`,
        });
    }
});

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

        // Get browser ID asynchronously
        const browserId = await createRemoteBrowserForRun(userId);
        const runId = uuid();

        // Store run info in Redis
        await redisClient.hmset(REDIS_KEYS.API_RUN(runId), {
            robotId: id,
            userId,
            browserId,
            status: 'running',
            startedAt: Date.now().toString(),
        });
        
        // Add to user's API runs set
        await redisClient.sadd(REDIS_KEYS.API_USER_RUNS(userId), runId);
        
        // Add to robot's API runs set
        await redisClient.sadd(REDIS_KEYS.API_ROBOT_RUNS(id), runId);

        const run = await Run.create({
            status: 'running',
            name: recording.recording_meta.name,
            robotId: recording.id,
            robotMetaId: recording.recording_meta.id,
            startedAt: new Date().toLocaleString(),
            finishedAt: '',
            browserId,
            interpreterSettings: { maxConcurrency: 1, maxRepeats: 1, debug: true },
            log: '',
            runId,
            runByAPI: true,
            serializableOutput: {},
            binaryOutput: {},
        });

        const plainRun = run.toJSON();

        return {
            browserId,
            runId: plainRun.runId,
        };

    } catch (e) {
        const { message } = e as Error;
        logger.log('info', `Error while scheduling a run with id: ${id}`);
        console.log(`Error scheduling run:`, message);
        return {
            success: false,
            error: message,
        };
    }
}

async function readyForRunHandler(browserId: string, id: string, userId: string){
    try {
        const result = await executeRun(id, userId);

        if (result && result.success) {
            logger.log('info', `Interpretation of ${id} succeeded`);
            resetRecordingState(browserId, id);
            return result.interpretationInfo;
        } else {
            logger.log('error', `Interpretation of ${id} failed`);
            await destroyRemoteBrowser(browserId, userId);
            resetRecordingState(browserId, id);
            return null;
        }

    } catch (error: any) {
        logger.error(`Error during readyForRunHandler: ${error.message}`);
        
        // Update Redis status on error
        await redisClient.hmset(REDIS_KEYS.API_RUN(id), {
            status: 'failed',
            error: error.message,
            finishedAt: Date.now().toString()
        });
        
        await destroyRemoteBrowser(browserId, userId);
        return null;
    }
}


function resetRecordingState(browserId: string, id: string) {
    browserId = '';
    id = '';
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
            };
        }

        const plainRun = run.toJSON();

        const recording = await Robot.findOne({ where: { 'recording_meta.id': plainRun.robotMetaId }, raw: true });
        if (!recording) {
            return {
                success: false,
                error: 'Recording not found'
            };
        }

        // Update run status in both database and Redis
        plainRun.status = 'running';
        await redisClient.hmset(REDIS_KEYS.API_RUN(id), {
            status: 'running',
            startedAt: Date.now().toString()
        });
        await redisClient.set(REDIS_KEYS.RUN_STATUS(id), 'running');

        // Get browser asynchronously
        const browser = await browserPool.getRemoteBrowser(plainRun.browserId);
        if (!browser) {
            throw new Error('Could not access browser');
        }

        // Check if browser session has enough time remaining
        const remainingTime = await getRemoteBrowserRemainingTime(plainRun.browserId);
        if (remainingTime !== null && remainingTime < 60000) { // Less than 1 minute
            throw new Error('Browser session will expire soon');
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

        const updatedRun = await run.update({
            ...run,
            status: 'success',
            finishedAt: new Date().toLocaleString(),
            browserId: plainRun.browserId,
            log: interpretationInfo.log.join('\n'),
            serializableOutput: interpretationInfo.serializableOutput,
            binaryOutput: uploadedBinaryOutput,
        });

        // Update run status in Redis
        await redisClient.hmset(REDIS_KEYS.API_RUN(id), {
            status: 'success',
            finishedAt: Date.now().toString()
        });
        await redisClient.set(REDIS_KEYS.RUN_STATUS(id), 'success');

        let totalRowsExtracted = 0;
        let extractedScreenshotsCount = 0;
        let extractedItemsCount = 0;

        if (updatedRun.dataValues.binaryOutput && updatedRun.dataValues.binaryOutput["item-0"]) {
            extractedScreenshotsCount = 1;
        }

        if (updatedRun.dataValues.serializableOutput && updatedRun.dataValues.serializableOutput["item-0"]) {
            const itemsArray = run.dataValues.serializableOutput["item-0"];
            extractedItemsCount = itemsArray.length;

            totalRowsExtracted = itemsArray.reduce((total, item) => {
                return total + Object.keys(item).length;
            }, 0);
        }

        console.log(`Extracted Items Count: ${extractedItemsCount}`);
        console.log(`Extracted Screenshots Count: ${extractedScreenshotsCount}`);
        console.log(`Total Rows Extracted: ${totalRowsExtracted}`);

        capture('maxun-oss-run-created-api', {
                runId: id,
                created_at: new Date().toISOString(),
                status: 'success',
                extractedItemsCount,
                totalRowsExtracted,
                extractedScreenshotsCount,
            }
        );

        return {
            success: true,
            interpretationInfo: updatedRun.toJSON()
        };

    } catch (error: any) {
        logger.log('info', `Error while running a robot with id: ${id} - ${error.message}`);
        
        // Update run status in both database and Redis
        const run = await Run.findOne({ where: { runId: id } });
        if (run) {
            await run.update({
                status: 'failed',
                finishedAt: new Date().toLocaleString(),
            });
        }
        
        await redisClient.hmset(REDIS_KEYS.API_RUN(id), {
            status: 'failed',
            error: error.message,
            finishedAt: Date.now().toString()
        });
        await redisClient.set(REDIS_KEYS.RUN_STATUS(id), 'failed');
        
        capture(
           'maxun-oss-run-created-api',
           {
                runId: id,
                created_at: new Date().toISOString(),
                status: 'failed',
            }
        );
        return {
            success: false,
            error: error.message,
        };
    }
}

export async function handleRunRecording(id: string, userId: string) {
    try {
        const result = await createWorkflowAndStoreMetadata(id, userId);
        const { browserId, runId: newRunId } = result;

        if (!browserId || !newRunId || !userId) {
            throw new Error('browserId or runId or userId is undefined');
        }

        // Store the connection info in Redis
        await redisClient.hmset(REDIS_KEYS.API_RUN(newRunId), {
            browserId,
            userId,
            robotId: id,
            status: 'initializing',
            connectionStartedAt: Date.now().toString()
        });

        const socket = io(`${process.env.BACKEND_URL ? process.env.BACKEND_URL : 'http://localhost:8080'}/${browserId}`, {
            transports: ['websocket'],
            rejectUnauthorized: false
        });

        socket.on('ready-for-run', () => readyForRunHandler(browserId, newRunId, userId));

        logger.log('info', `Running Robot: ${id}`);

        socket.on('disconnect', () => {
            cleanupSocketListeners(socket, browserId, newRunId, userId);
        });

        // Return the runId immediately, so the client knows the run is started
        return newRunId;

    } catch (error: any) {
        logger.error('Error running robot:', error);
    }
}

function cleanupSocketListeners(socket: Socket, browserId: string, id: string, userId: string) {
    socket.off('ready-for-run', () => readyForRunHandler(browserId, id, userId));
    logger.log('info', `Cleaned up listeners for browserId: ${browserId}, runId: ${id}`);
}

async function waitForRunCompletion(runId: string, interval: number = 2000) {
    const MAX_WAIT_TIME = 5 * 60 * 1000; // 5 minutes
    const startTime = Date.now();
    
    while (Date.now() - startTime < MAX_WAIT_TIME) {
        // Check Redis first for status (faster)
        const redisStatus = await redisClient.get(REDIS_KEYS.RUN_STATUS(runId));
        
        if (redisStatus === 'success') {
            const run = await Run.findOne({ where: { runId }, raw: true });
            if (!run) throw new Error('Run not found');
            return run;
        } else if (redisStatus === 'failed') {
            throw new Error('Run failed');
        }
        
        // If Redis doesn't have the status, check the database
        const run = await Run.findOne({ where: { runId }, raw: true });
        if (!run) throw new Error('Run not found');

        if (run.status === 'success') {
            return run;
        } else if (run.status === 'failed') {
            throw new Error('Run failed');
        }

        // Wait for the next polling interval
        await new Promise(resolve => setTimeout(resolve, interval));
    }
    
    throw new Error('Run timed out after waiting 5 minutes');
}

/**
 * @swagger
 * /api/robots/{id}/runs:
 *   post:
 *     summary: Run a robot by ID
 *     description: When you need to run a robot and get its captured data, you can use this endpoint to create a run for the robot. For now, you can poll the GET endpoint to retrieve a run's details as soon as it is finished. We are working on adding a webhook feature to notify you when a run is finished.
 *     security:
 *       - api_key: []
 *     parameters:
 *       - in: path
 *         name: id
 *         schema:
 *           type: string
 *         required: true
 *         description: The ID of the robot to run.
 *     responses:
 *       200:
 *         description: Robot run started successfully.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 200
 *                 messageCode:
 *                   type: string
 *                   example: success
 *                 run:
 *                   type: object
 *                   properties:
 *                     runId:
 *                       type: string
 *                       example: "67890"
 *                     status:
 *                       type: string
 *                       example: "in_progress"
 *       401:
 *         description: Unauthorized access.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 ok:
 *                   type: boolean
 *                   example: false
 *                 error:
 *                   type: string
 *                   example: "Unauthorized"
 *       500:
 *         description: Error running robot.
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 statusCode:
 *                   type: integer
 *                   example: 500
 *                 messageCode:
 *                   type: string
 *                   example: error
 *                 message:
 *                   type: string
 *                   example: "Failed to run robot"
 */
router.post("/robots/:id/runs", requireAPIKey, async (req: AuthenticatedRequest, res: Response) => {
    try {
        if (!req.user) {
            return res.status(401).json({ ok: false, error: 'Unauthorized' });
        }
        const runId = await handleRunRecording(req.params.id, req.user.dataValues.id);

        if (!runId) {
            throw new Error('Run ID is undefined');
        }
        
        // Use the enhanced wait function with Redis support
        const completedRun = await waitForRunCompletion(runId);

        const formattedRun = formatRunResponse(completedRun);
        
        // Cache the result in Redis
        await redisClient.set(
            REDIS_KEYS.API_RUN(runId),
            JSON.stringify(formattedRun),
            'EX',
            3600 // Cache for 1 hour
        );

        const response = {
            statusCode: 200,
            messageCode: "success",
            run: formattedRun,
        };

        res.status(200).json(response);
    } catch (error) {
        console.error("Error running robot:", error);
        res.status(500).json({
            statusCode: 500,
            messageCode: "error",
            message: "Failed to run robot",
        });
    }
});

// New endpoint to check run status without waiting for completion
router.get("/robots/:id/runs/:runId/status", requireAPIKey, async (req: Request, res: Response) => {
    try {
        // Check Redis first for status (faster)
        const redisStatus = await redisClient.hgetall(REDIS_KEYS.API_RUN(req.params.runId));
        
        if (redisStatus && Object.keys(redisStatus).length > 0) {
            return res.status(200).json({
                statusCode: 200,
                messageCode: "success",
                status: {
                    runId: req.params.runId,
                    robotId: req.params.id,
                    status: redisStatus.status || 'unknown',
                    startedAt: redisStatus.startedAt ? new Date(parseInt(redisStatus.startedAt)).toLocaleString() : undefined,
                    finishedAt: redisStatus.finishedAt ? new Date(parseInt(redisStatus.finishedAt)).toLocaleString() : undefined,
                }
            });
        }
        
        // If not in Redis, check database
        const run = await Run.findOne({
            where: {
                runId: req.params.runId,
                robotMetaId: req.params.id
            },
            raw: true
        });
        
        if (!run) {
            return res.status(404).json({
                statusCode: 404,
                messageCode: "not_found",
                message: `Run with id "${req.params.runId}" for robot with id "${req.params.id}" not found.`
            });
        }
        
        return res.status(200).json({
            statusCode: 200,
            messageCode: "success",
            status: {
                runId: run.runId,
                robotId: run.robotMetaId,
                status: run.status,
                startedAt: run.startedAt,
                finishedAt: run.finishedAt || undefined
            }
        });
        
    } catch (error) {
        console.error("Error checking run status:", error);
        res.status(500).json({
            statusCode: 500,
            messageCode: "error",
            message: "Failed to check run status"
        });
    }
 });
 
 // New endpoint to get active browser information for debugging
 router.get("/system/browser-status", requireAPIKey, async (req: AuthenticatedRequest, res: Response) => {
    try {
        if (!req.user) {
            return res.status(401).json({ ok: false, error: 'Unauthorized' });
        }
        
        // Get all browser IDs for this user
        const browserIds = await redisClient.keys(`browser:*:info`);
        const browsersInfo = [];
        
        for (const key of browserIds) {
            const browserInfo = await redisClient.hgetall(key);
            if (browserInfo && browserInfo.userId === req.user.dataValues.id) {
                const browserId = key.split(':')[1];
                const remainingTime = await getRemoteBrowserRemainingTime(browserId);
                
                browsersInfo.push({
                    browserId,
                    status: browserInfo.status || 'unknown',
                    state: browserInfo.state || 'unknown',
                    active: browserInfo.active === 'true',
                    startTime: browserInfo.startTime ? new Date(parseInt(browserInfo.startTime)).toLocaleString() : undefined,
                    remainingTime: remainingTime ? Math.floor(remainingTime / 1000) : null,
                });
            }
        }
        
        return res.status(200).json({
            statusCode: 200,
            messageCode: "success",
            browsers: browsersInfo
        });
        
    } catch (error) {
        console.error("Error getting browser status:", error);
        res.status(500).json({
            statusCode: 500,
            messageCode: "error",
            message: "Failed to get browser status"
        });
    }
 });
 
 export default router;