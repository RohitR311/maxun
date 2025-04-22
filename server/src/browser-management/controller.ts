/**
 * The main function group which determines the flow of remote browser management.
 * Holds the singleton instances of browser pool and socket.io server.
 * Integrated with Redis for persistent browser tracking and session management.
 */
import { Socket } from "socket.io";
import { uuid } from 'uuidv4';

import { createSocketConnection, createSocketConnectionForRun, registerBrowserUserContext } from "../socket-connection/connection";
import { io, browserPool } from "../server";
import { RemoteBrowser } from "./classes/RemoteBrowser";
import { RemoteBrowserOptions } from "../types";
import logger from "../logger";
import { redisClient } from "../storage/connection";

// Redis keys for browser management
const REDIS_KEYS = {
    BROWSER_SESSION_INFO: (id: string) => `browser:${id}:info`,
    BROWSER_USER_MAPPING: (id: string) => `browser:${id}:user`,
};

/**
 * Starts and initializes a {@link RemoteBrowser} instance.
 * Creates a new socket connection over a dedicated namespace
 * and registers all interaction event handlers.
 * Returns the id of an active browser or the new remote browser's generated id.
 * @param options {@link RemoteBrowserOptions} to be used when launching the browser
 * @returns Promise<string> Browser ID
 * @category BrowserManagement-Controller
 */
export const initializeRemoteBrowserForRecording = async (userId: string): Promise<string> => {
  // Get the active recording browser for this user, if any
  const activeId = await getActiveBrowserIdByState(userId, "recording");
  const id = activeId || uuid();
  
  createSocketConnection(
    io.of(id),
    async (socket: Socket) => {
      try {
        // Check again if browser is already active (could have changed during async operation)
        const existingActiveBrowserId = await getActiveBrowserIdByState(userId, "recording");
        
        if (existingActiveBrowserId) {
          logger.log('debug', `Using existing recording browser ${existingActiveBrowserId} for user ${userId}`);
          const remoteBrowser = await browserPool.getRemoteBrowser(existingActiveBrowserId);
          if (remoteBrowser) {
            remoteBrowser.updateSocket(socket);
            await remoteBrowser.makeAndEmitScreenshot();
            
            // Send remaining session time to frontend
            const remainingTime = await browserPool.getRemainingSessionTime(existingActiveBrowserId);
            socket.emit('sessionInfo', {
              browserId: existingActiveBrowserId,
              userId: userId,
              remainingTime: Math.floor((remainingTime || 0) / 1000), // Convert to seconds
              maxSessionTime: 10 * 60, // 10 minutes in seconds
            });
          } else {
            logger.log('error', `Browser ${existingActiveBrowserId} not found in pool, creating new one`);
            await createNewRecordingBrowser(id, socket, userId);
          }
        } else {
          // No active browser, create a new one
          await createNewRecordingBrowser(id, socket, userId);
        }
        
        socket.emit('loaded');
      } catch (error) {
        logger.log('error', `Error initializing remote browser for recording: ${error}`);
        socket.emit('browserError', { message: 'Failed to initialize browser session' });
      }
    });
  
  return id;
};

/**
 * Helper function to create a new recording browser
 */
async function createNewRecordingBrowser(id: string, socket: Socket, userId: string): Promise<void> {
  logger.log('debug', `Creating new recording browser ${id} for user ${userId}`);
  
  const browserSession = new RemoteBrowser(socket, userId, id);
  browserSession.interpreter.subscribeToPausing();
  await browserSession.initialize();
  await browserSession.registerEditorEvents();
  await browserSession.subscribeToScreencast();
  
  // Store browser in the pool
  await browserPool.addRemoteBrowser(id, browserSession, userId, false, "recording");
  
  // Send session information to frontend
  socket.emit('sessionInfo', {
    browserId: id,
    userId: userId,
    remainingTime: 10 * 60, // 10 minutes in seconds
    maxSessionTime: 10 * 60,
  });
}

/**
 * Starts and initializes a {@link RemoteBrowser} instance for interpretation.
 * Creates a new {@link Socket} connection over a dedicated namespace.
 * Returns the new remote browser's generated id.
 * @param userId User ID for browser ownership
 * @returns Promise<string> Browser ID
 * @category BrowserManagement-Controller
 */
export const createRemoteBrowserForRun = async (userId: string): Promise<string> => {
  const id = uuid();

  registerBrowserUserContext(id, userId);
  logger.log('debug', `Created new browser for run: ${id} for user: ${userId}`);
  
  createSocketConnectionForRun(
    io.of(id), 
    async (socket: Socket) => {
      try {
        const browserSession = new RemoteBrowser(socket, userId, id);
        await browserSession.initialize();
        await browserPool.addRemoteBrowser(id, browserSession, userId, false, "run");
        
        // Store the user ID for this browser in Redis for quick lookup
        await redisClient.set(REDIS_KEYS.BROWSER_USER_MAPPING(id), userId);
        
        socket.emit('ready-for-run');
      } catch (error: any) {
        logger.error(`Error initializing browser: ${error.message}`);
        socket.emit('browserError', { message: 'Failed to initialize browser session for run' });
      }
    });
  
  return id;
};

/**
 * Terminates a remote browser recording session
 * and removes the browser from the browser pool.
 * @param id instance id of the remote browser to be terminated
 * @returns {Promise<boolean>}
 * @category BrowserManagement-Controller
 */
export const destroyRemoteBrowser = async (id: string, userId: string): Promise<boolean> => {
  try {
    const browserSession = await browserPool.getRemoteBrowser(id);

    const result = await browserPool.deleteRemoteBrowser(id, userId);
    
    if (browserSession) {
      logger.log('debug', `Switching off the browser with id: ${id} for user: ${userId}`);
      await browserSession.stopCurrentInterpretation();
      await browserSession.switchOff();
    }
    
    return result;
  } catch (error) {
    logger.log('error', `Error destroying remote browser ${id}: ${error}`);
    return false;
  }
};

/**
 * Returns the id of an active browser or null.
 * Wrapper around {@link browserPool.getActiveBrowserId()} function.
 * @returns {Promise<string | null>}
 * @category  BrowserManagement-Controller
 */
export const getActiveBrowserId = async (userId: string): Promise<string | null> => {
  return await browserPool.getActiveBrowserId(userId);
};

/**
 * Returns the id of an active browser with the specified state or null.
 * @param userId the user ID to find the browser for
 * @param state the browser state to filter by ("recording" or "run")
 * @returns {Promise<string | null>}
 * @category  BrowserManagement-Controller
 */
export const getActiveBrowserIdByState = async (userId: string, state: "recording" | "run"): Promise<string | null> => {
  return await browserPool.getActiveBrowserId(userId, state);
};

/**
 * Returns the url string from a remote browser if exists in the browser pool.
 * @param id instance id of the remote browser
 * @returns {Promise<string | undefined>}
 * @category  BrowserManagement-Controller
 */
export const getRemoteBrowserCurrentUrl = async (id: string, userId: string): Promise<string | undefined> => {
  const browser = await browserPool.getRemoteBrowser(id);
  return browser?.getCurrentPage()?.url();
};

/**
 * Returns the array of tab strings from a remote browser if exists in the browser pool.
 * @param id instance id of the remote browser
 * @return {Promise<string[] | undefined>}
 * @category  BrowserManagement-Controller
 */
export const getRemoteBrowserCurrentTabs = async (id: string, userId: string): Promise<string[] | undefined> => {
  const browser = await browserPool.getRemoteBrowser(id);
  return browser?.getCurrentPage()?.context().pages()
    .map((page) => {
      const parsedUrl = new URL(page.url());
      const host = parsedUrl.hostname.match(/\b(?!www\.)[a-zA-Z0-9]+/g)?.join('.');
      if (host) {
        return host;
      }
      return 'new tab';
    });
};

/**
 * Gets the remaining session time for a browser
 * @param id instance id of the remote browser
 * @returns {Promise<number | null>} Remaining time in milliseconds or null if browser not found
 * @category BrowserManagement-Controller
 */
export const getRemoteBrowserRemainingTime = async (id: string): Promise<number | null> => {
  return await browserPool.getRemainingSessionTime(id);
};

/**
 * Interprets the currently generated workflow in the active browser instance.
 * If there is no active browser, the function logs an error.
 * @returns {Promise<void>}
 * @category  BrowserManagement-Controller
 */
export const interpretWholeWorkflow = async (userId: string): Promise<void> => {
  const id = await getActiveBrowserIdByState(userId, "recording");
  if (id) {
    const browser = await browserPool.getRemoteBrowser(id);
    if (browser) {
      await browser.interpretCurrentRecording();
    } else {
      logger.log('error', `No active browser with id ${id} found in the browser pool`);
    }
  } else {
    logger.log('error', `Cannot interpret the workflow: no active recording browser for user ${userId}.`);
  }
};

/**
 * Stops the interpretation of the current workflow in the active browser instance.
 * If there is no active browser, the function logs an error.
 * @returns {Promise<void>}
 * @category  BrowserManagement-Controller
 */
export const stopRunningInterpretation = async (userId: string): Promise<void> => {
  const id = await getActiveBrowserIdByState(userId, "recording");
  if (id) {
    const browser = await browserPool.getRemoteBrowser(id);
    if (browser) {
      await browser.stopCurrentInterpretation();
    } else {
      logger.log('error', `No active browser with id ${id} found in the browser pool`);
    }
  } else {
    logger.log('error', `Cannot stop interpretation: No active browser for user ${userId}.`);
  }
};

/**
 * Returns all browser IDs for a specific user
 * @param userId the user ID to find browsers for
 * @returns {Promise<string[]>} Array of browser IDs
 * @category BrowserManagement-Controller
 */
export const getAllUserBrowserIds = async (userId: string): Promise<string[]> => {
  return await browserPool.getAllBrowserIdsForUser(userId);
};

/**
 * Cleanup function to be called when the application is shutting down
 * Ensures all browsers are properly closed and Redis data is cleaned up
 */
export const cleanupAllBrowsers = async (): Promise<void> => {
  try {
    await browserPool.cleanup();
    logger.log('debug', 'All browsers have been cleaned up');
  } catch (error) {
    logger.log('error', `Error during browser cleanup: ${error}`);
  }
};