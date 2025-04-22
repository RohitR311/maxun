import { RemoteBrowser } from "./RemoteBrowser";
import logger from "../../logger";
import { redisClient } from "../../storage/connection";

/**
 * @category Types
 */
/**
 * Represents the possible states of a remote browser.
 * @category Types
 */
type BrowserState = "recording" | "run";

interface BrowserPoolInfo {
    /**
     * The instance of remote browser.
     */
    browser: RemoteBrowser,
    /**
     * States if the browser's instance is being actively used.
     * @default false
     */
    active: boolean,
    /**
     * The user ID that owns this browser instance.
     */
    userId: string,
    /**
     * The current state of the browser.
     * Can be "recording" or "run".
     * @default "recording"
     */
    state: BrowserState,
    /**
     * Timestamp when the browser session was started
     */
    startTime: number,
    /**
     * Maximum session duration in milliseconds
     */
    maxSessionDuration: number,
    /**
     * Whether a warning has been sent for this session
     */
    warningIssued: boolean,
}

/**
 * Dictionary of all the active remote browser's instances indexed by their id.
 * The value in this dictionary is of type BrowserPoolInfo,
 * which provides additional information about the browser's usage.
 * @category Types
 */
interface PoolDictionary {
    [key: string]: BrowserPoolInfo,
}

/**
 * Redis keys used by the BrowserPool
 */
const REDIS_KEYS = {
    BROWSER_INFO: (id: string) => `browser:${id}:info`,
    USER_BROWSERS: (userId: string) => `user:${userId}:browsers`,
    ALL_BROWSERS: 'all:browsers',
    SESSION_TIMER: (id: string) => `browser:${id}:timer`,
};

/**
 * A browser pool is a collection of remote browsers that are initialized and ready to be used.
 * Enforces a "1 User - 2 Browser" policy, while allowing multiple users to have their own browser instances.
 * Uses Redis to track browser instances and manage their lifecycle.
 * Implements session time limits with frontend notifications.
 * @category BrowserManagement
 */
export class BrowserPool {
    /**
     * Holds all the instances of remote browsers.
     * This is a local cache, while Redis is the source of truth.
     */
    private pool: PoolDictionary = {};

    /**
     * The maximum duration of a recording session in milliseconds (10 minutes)
     */
    private readonly MAX_SESSION_DURATION = 1 * 60 * 1000;
    
    /**
     * The warning time before session ends in milliseconds (1 minute)
     */
    private readonly WARNING_TIME = 0.5 * 60 * 1000;

    /**
     * Interval timers for session management
     */
    private sessionTimers: Map<string, NodeJS.Timeout> = new Map();

    /**
     * Initialize the browser pool
     * Recovers any existing browser sessions from Redis
     */
    constructor() {
        // Start session monitoring for active sessions
        this.initializeSessionMonitoring();
    }

    /**
     * Initialize session monitoring for existing browser sessions
     */
    private async initializeSessionMonitoring(): Promise<void> {
        try {
            const allBrowsers = await redisClient.smembers(REDIS_KEYS.ALL_BROWSERS);
            
            for (const browserId of allBrowsers) {
                const browserInfo = await redisClient.hgetall(REDIS_KEYS.BROWSER_INFO(browserId));
                
                if (browserInfo && browserInfo.state === 'recording') {
                    // Start session timer for existing recording sessions
                    this.startSessionTimer(browserId, browserInfo.userId);
                }
            }
            
            logger.log('debug', `Initialized session monitoring for ${allBrowsers.length} browsers`);
        } catch (error) {
            logger.log('error', `Failed to initialize session monitoring: ${error}`);
        }
    }

    /**
     * Start a timer to monitor session duration and send notifications
     */
    private startSessionTimer(browserId: string, userId: string): void {
        if (this.sessionTimers.has(browserId)) {
            clearInterval(this.sessionTimers.get(browserId)!);
        }

        const checkInterval = setInterval(async () => {
            try {
                const browserInfo = await redisClient.hgetall(REDIS_KEYS.BROWSER_INFO(browserId));
                
                if (!browserInfo || !browserInfo.startTime) {
                    clearInterval(this.sessionTimers.get(browserId)!);
                    this.sessionTimers.delete(browserId);
                    return;
                }

                const startTime = parseInt(browserInfo.startTime, 10);
                const maxDuration = parseInt(browserInfo.maxSessionDuration || this.MAX_SESSION_DURATION.toString(), 10);
                const elapsedTime = Date.now() - startTime;
                const remainingTime = maxDuration - elapsedTime;
                
                // If time remaining is less than WARNING_TIME and warning not issued
                if (remainingTime <= this.WARNING_TIME && browserInfo.warningIssued !== 'true') {
                    logger.log('debug', `Session warning for browser ${browserId} (user: ${userId})`);
                    
                    // Mark warning as issued
                    await redisClient.hset(REDIS_KEYS.BROWSER_INFO(browserId), 'warningIssued', 'true');
                    
                    // Also update local cache
                    if (this.pool[browserId]) {
                        this.pool[browserId].warningIssued = true;
                    }
                }
                
                // If session time exceeded, terminate the session
                if (remainingTime <= 0) {
                    logger.log('debug', `Session timeout for browser ${browserId} (user: ${userId})`);
                    
                    // Close and delete the browser
                    await this.closeAndDeleteBrowser(browserId);
                    
                    // Clear the interval
                    clearInterval(this.sessionTimers.get(browserId)!);
                    this.sessionTimers.delete(browserId);
                }
            } catch (error) {
                logger.log('error', `Error in session timer for browser ${browserId}: ${error}`);
            }
        }, 5000); // Check every 5 seconds
        
        this.sessionTimers.set(browserId, checkInterval);
        logger.log('debug', `Started session timer for browser ${browserId} (user: ${userId})`);
    }

    /**
     * Adds a remote browser instance to the pool for a specific user.
     * If the user already has two browsers, the oldest browser will be closed and replaced.
     * 
     * @param id remote browser instance's id
     * @param browser remote browser instance
     * @param userId the user ID that owns this browser instance
     * @param active states if the browser's instance is being actively used
     * @returns true if a new browser was added, false if an existing browser was replaced
     */
    public addRemoteBrowser = async (
        id: string, 
        browser: RemoteBrowser, 
        userId: string,
        active: boolean = false,
        state: BrowserState = "recording"
    ): Promise<boolean> => {
        try {
            // Check if browser with this ID already exists in Redis
            const existingBrowser = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
            
            if (existingBrowser && await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'userId') === userId) {
                // Update the existing browser in Redis
                await redisClient.hmset(REDIS_KEYS.BROWSER_INFO(id), {
                    active: active.toString(),
                    state: state,
                    userId: userId,
                });
                
                // Update local cache
                this.pool[id] = {
                    browser,
                    active,
                    userId,
                    state,
                    startTime: parseInt(await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'startTime') || Date.now().toString(), 10),
                    maxSessionDuration: parseInt(await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'maxSessionDuration') || this.MAX_SESSION_DURATION.toString(), 10),
                    warningIssued: await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'warningIssued') === 'true',
                };
                
                logger.log('debug', `Updated existing browser with id: ${id} for user: ${userId}`);
                return false;
            }
            
            // Get existing browsers for this user from Redis
            const userBrowserIds = await redisClient.smembers(REDIS_KEYS.USER_BROWSERS(userId));
            console.log("User browser IDs: ", userBrowserIds);
            
            // If trying to add a "recording" browser, check if one already exists
            if (state === "recording") {
                // Check if user already has a recording browser
                for (const browserId of userBrowserIds) {
                    const browserState = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'state');
                    if (browserState === "recording") {
                        logger.log('debug', `User ${userId} already has a browser in "recording" state`);
                        return false;
                    }
                }
            }
            
            // For "run" state, check if the user already has the maximum number of browsers (2)
            if (userBrowserIds.length >= 2 && !userBrowserIds.includes(id)) {
                logger.log('debug', "User already has the maximum number of browsers (2)");
                return false;
            }
            
            // Add browser info to Redis
            const startTime = Date.now();
            await redisClient.hmset(REDIS_KEYS.BROWSER_INFO(id), {
                userId,
                active: active.toString(),
                state,
                startTime: startTime.toString(),
                maxSessionDuration: this.MAX_SESSION_DURATION.toString(),
                warningIssued: 'false',
            });
            
            // Add browser to user's browser set
            if (!userBrowserIds.includes(id)) {
                await redisClient.sadd(REDIS_KEYS.USER_BROWSERS(userId), id);
            }
            
            // Add to global browser set
            await redisClient.sadd(REDIS_KEYS.ALL_BROWSERS, id);
            
            // Update local cache
            this.pool[id] = {
                browser,
                active,
                userId,
                state,
                startTime,
                maxSessionDuration: this.MAX_SESSION_DURATION,
                warningIssued: false,
            };
            
            // If this is a recording session, start the session timer
            if (state === "recording") {
                this.startSessionTimer(id, userId);
            }
            
            logger.log('debug', `Remote browser with id: ${id} added to the pool for user: ${userId}`);
            return true;
        } catch (error) {
            logger.log('error', `Failed to add remote browser: ${error}`);
            return false;
        }
    };

    /**
     * Removes the remote browser instance from the pool.
     * Closes the browser before removing it from Redis and local cache.
     * 
     * @param id remote browser instance's id
     * @returns true if the browser was removed successfully, false otherwise
     */
    public closeAndDeleteBrowser = async (id: string): Promise<boolean> => {
        try {
            // Check if browser exists in Redis
            const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
            if (!exists) {
                logger.log('warn', `Remote browser with id: ${id} does not exist in the pool`);
                return false;
            }
            
            // Get user ID from Redis
            const userId = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'userId');
            
            // Close the browser if it exists in local cache
            if (this.pool[id] && this.pool[id].browser) {
                try {
                    await this.pool[id].browser.switchOff();
                } catch (closeError) {
                    logger.log('warn', `Error closing browser with id ${id}: ${closeError}`);
                }
            }
            
            // Stop session timer if it exists
            if (this.sessionTimers.has(id)) {
                clearInterval(this.sessionTimers.get(id)!);
                this.sessionTimers.delete(id);
            }
            
            // Remove browser from user's browser set
            if (userId) {
                await redisClient.srem(REDIS_KEYS.USER_BROWSERS(userId), id);
            }
            
            // Remove browser from global browser set
            await redisClient.srem(REDIS_KEYS.ALL_BROWSERS, id);
            
            // Delete browser info from Redis
            await redisClient.del(REDIS_KEYS.BROWSER_INFO(id));
            
            // Remove from local cache
            delete this.pool[id];
            
            logger.log('debug', `Remote browser with id: ${id} removed from the pool`);
            return true;
        } catch (error) {
            logger.log('error', `Failed to close and delete browser: ${error}`);
            return false;
        }
    };

    /**
     * Removes the remote browser instance from the pool without attempting to close it.
     * 
     * @param id remote browser instance's id
     * @returns true if the browser was removed successfully, false otherwise
     */
    public deleteRemoteBrowser = async (id: string, userId: string): Promise<boolean> => {
        try {
            // Check if browser exists in Redis
            const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
            if (!exists) {
                logger.log('warn', `Remote browser with id: ${id} does not exist in the pool`);
                return false;
            }
            
            // Get user ID from Redis
            // const userId = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'userId');
            
            // Stop session timer if it exists
            if (this.sessionTimers.has(id)) {
                clearInterval(this.sessionTimers.get(id)!);
                this.sessionTimers.delete(id);
            }
            
            // Remove browser from user's browser set
            if (userId) {
                await redisClient.srem(REDIS_KEYS.USER_BROWSERS(userId), id);
            }
            
            // Remove browser from global browser set
            await redisClient.srem(REDIS_KEYS.ALL_BROWSERS, id);
            
            // Delete browser info from Redis
            await redisClient.del(REDIS_KEYS.BROWSER_INFO(id));
            
            // Remove from local cache
            delete this.pool[id];
            
            logger.log('debug', `Remote browser with id: ${id} deleted from the pool`);
            return true;
        } catch (error) {
            logger.log('error', `Failed to delete browser: ${error}`);
            return false;
        }
    };

    /**
     * Returns the remote browser instance from the pool.
     * 
     * @param id remote browser instance's id
     * @returns remote browser instance or undefined if it does not exist in the pool
     */
    public getRemoteBrowser = async (id: string): Promise<RemoteBrowser | undefined> => {
        // Check if browser exists in Redis
        const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
        if (!exists) {
            return undefined;
        }
        
        // Return from local cache if available
        if (this.pool[id]) {
            logger.log('debug', `Remote browser with id: ${id} retrieved from local cache`);
            return this.pool[id].browser;
        }
        
        logger.log('debug', `Remote browser with id: ${id} not found in local cache`);
        return undefined;
    };

    /**
     * Returns the active browser's instance id for a specific user.
     * If state is specified, only returns a browser with that exact state.
     * 
     * @param userId the user ID to find the browser for
     * @param state optional browser state filter ("recording" or "run")
     * @returns the browser ID for the user, or null if no browser exists with the required state
     */
    public getActiveBrowserId = async (userId: string, state?: BrowserState): Promise<string | null> => {
        try {
            // Get browser IDs from Redis
            const browserIds = await redisClient.smembers(REDIS_KEYS.USER_BROWSERS(userId));
            
            if (browserIds.length === 0) {
                logger.log('debug', `No browser found for user: ${userId}`);
                return null;
            }
            
            // Sort browsers by startTime (newest first)
            const browsersWithTime: Array<{id: string, startTime: number}> = [];
            
            for (const id of browserIds) {
                const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
                if (!exists) continue;
                
                const startTimeStr = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'startTime');
                const startTime = startTimeStr ? parseInt(startTimeStr, 10) : 0;
                
                browsersWithTime.push({ id, startTime });
            }
            
            // Sort by startTime, newest first
            browsersWithTime.sort((a, b) => b.startTime - a.startTime);
            
            // If state is specified, only return browsers with that exact state
            if (state) {
                for (const browser of browsersWithTime) {
                    const browserState = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browser.id), 'state');
                    
                    if (browserState === state) {
                        return browser.id;
                    }
                }
                
                logger.log('debug', `No browser with state ${state} found for user: ${userId}`);
                return null;
            }
            
            // If no state specified, return any browser (newest first)
            if (browsersWithTime.length > 0) {
                return browsersWithTime[0].id;
            }
            
            return null;
        } catch (error) {
            logger.log('error', `Failed to get active browser ID: ${error}`);
            return null;
        }
    };

    /**
     * Returns the user ID associated with a browser ID.
     * 
     * @param browserId the browser ID to find the user for
     * @returns the user ID for the browser, or null if the browser doesn't exist
     */
    public getUserForBrowser = async (browserId: string): Promise<string | null> => {
        try {
            const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(browserId));
            if (!exists) {
                return null;
            }
            
            const userId = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'userId');
            return userId;
        } catch (error) {
            logger.log('error', `Failed to get user for browser: ${error}`);
            return null;
        }
    };

    /**
     * Sets the active state of a browser.
     * 
     * @param id the browser ID
     * @param active the new active state
     * @returns true if successful, false if the browser wasn't found
     */
    public setActiveBrowser = async (id: string, active: boolean): Promise<boolean> => {
        try {
            const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
            if (!exists) {
                logger.log('warn', `Remote browser with id: ${id} does not exist in the pool`);
                return false;
            }
            
            // Update in Redis
            await redisClient.hset(REDIS_KEYS.BROWSER_INFO(id), 'active', active.toString());
            
            // Update in local cache
            if (this.pool[id]) {
                this.pool[id].active = active;
            }
            
            logger.log('debug', `Remote browser with id: ${id} set to ${active ? 'active' : 'inactive'}`);
            return true;
        } catch (error) {
            logger.log('error', `Failed to set active browser: ${error}`);
            return false;
        }
    };
    
    /**
     * Sets the state of a browser.
     * Only allows one browser in "recording" state per user.
     * 
     * @param id the browser ID
     * @param state the new state ("recording" or "run")
     * @returns true if successful, false if the browser wasn't found or state change not allowed
     */
    public setBrowserState = async (id: string, state: BrowserState): Promise<boolean> => {
        try {
            const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
            if (!exists) {
                logger.log('warn', `Remote browser with id: ${id} does not exist in the pool`);
                return false;
            }
            
            // If trying to set to "recording" state, check if another browser is already recording
            if (state === "recording") {
                const userId = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'userId');
                if (!userId) return false;
                
                const userBrowserIds = await redisClient.smembers(REDIS_KEYS.USER_BROWSERS(userId));
                
                // Check if any other browser for this user is already in recording state
                for (const browserId of userBrowserIds) {
                    if (browserId === id) continue;
                    
                    const browserState = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'state');
                    if (browserState === "recording") {
                        logger.log('warn', `Cannot set browser ${id} to "recording" state: User ${userId} already has a browser in recording state`);
                        return false;
                    }
                }
                
                // If changing to recording state, start the session timer
                this.startSessionTimer(id, userId);
            }
            
            // Update in Redis
            await redisClient.hset(REDIS_KEYS.BROWSER_INFO(id), 'state', state);
            
            // Update in local cache
            if (this.pool[id]) {
                this.pool[id].state = state;
            }
            
            logger.log('debug', `Remote browser with id: ${id} state set to ${state}`);
            return true;
        } catch (error) {
            logger.log('error', `Failed to set browser state: ${error}`);
            return false;
        }
    };
    
    /**
     * Gets the current state of a browser.
     * 
     * @param id the browser ID
     * @returns the current state or null if the browser wasn't found
     */
    public getBrowserState = async (id: string): Promise<BrowserState | null> => {
        try {
            const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
            if (!exists) {
                logger.log('warn', `Remote browser with id: ${id} does not exist in the pool`);
                return null;
            }
            
            const state = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'state') as BrowserState;
            return state || null;
        } catch (error) {
            logger.log('error', `Failed to get browser state: ${error}`);
            return null;
        }
    };

    /**
     * Returns all browser instances for a specific user.
     * With the "1 User - 2 Browser" policy, this can return up to 2 browsers.
     * 
     * @param userId the user ID to find browsers for
     * @returns an array of browser IDs belonging to the user
     */
    public getAllBrowserIdsForUser = async (userId: string): Promise<string[]> => {
        try {
            // Get browser IDs from Redis
            const browserIds = await redisClient.smembers(REDIS_KEYS.USER_BROWSERS(userId));
            
            // Filter to only include IDs that still exist in Redis
            const validBrowserIds: string[] = [];
            
            for (const id of browserIds) {
                const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
                if (exists) {
                    validBrowserIds.push(id);
                } else {
                    // Remove invalid IDs from the set
                    await redisClient.srem(REDIS_KEYS.USER_BROWSERS(userId), id);
                }
            }
            
            return validBrowserIds;
        } catch (error) {
            logger.log('error', `Failed to get all browser IDs for user: ${error}`);
            return [];
        }
    };

    /**
     * Returns the total number of browsers in the pool.
     */
    public getPoolSize = async (): Promise<number> => {
        try {
            return await redisClient.scard(REDIS_KEYS.ALL_BROWSERS);
        } catch (error) {
            logger.log('error', `Failed to get pool size: ${error}`);
            return 0;
        }
    };

    /**
     * Returns the total number of active users (users with browsers).
     */
    public getActiveUserCount = async (): Promise<number> => {
        try {
            // Get all browser IDs
            const allBrowsers = await redisClient.smembers(REDIS_KEYS.ALL_BROWSERS);
            
            // Extract unique user IDs
            const userIds = new Set<string>();
            
            for (const browserId of allBrowsers) {
                const userId = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'userId');
                if (userId) {
                    userIds.add(userId);
                }
            }
            
            return userIds.size;
        } catch (error) {
            logger.log('error', `Failed to get active user count: ${error}`);
            return 0;
        }
    };
    
    /**
     * Gets the current active browser for the system if there's only one active user.
     * This is a migration helper to support code that hasn't been updated to the user-browser model yet.
     * 
     * @param currentUserId The ID of the current user, which will be prioritized if multiple browsers exist
     * @param state Optional state filter to find browsers in a specific state
     * @returns A browser ID if one can be determined, or null
     */
    public getActiveBrowserForMigration = async (currentUserId?: string, state?: BrowserState): Promise<string | null> => {
        try {
            // If a current user ID is provided and they have a browser, return that
            if (currentUserId) {
                const browserForUser = await this.getActiveBrowserId(currentUserId, state);
                if (browserForUser) {
                    return browserForUser;
                }
                
                // If state is specified and no matching browser was found, return null
                if (state) {
                    return null;
                }
            }
            
            // Get all users with browsers
            const allBrowsers = await redisClient.smembers(REDIS_KEYS.ALL_BROWSERS);
            const userIds = new Set<string>();
            
            for (const browserId of allBrowsers) {
                const userId = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'userId');
                if (userId) {
                    userIds.add(userId);
                }
            }
            
            // If only one user has a browser, try to find a matching browser
            if (userIds.size === 1) {
                const userId = Array.from(userIds)[0];
                const browserIds = await redisClient.smembers(REDIS_KEYS.USER_BROWSERS(userId));
                
                // If state is specified, only look for that state
                if (state) {
                    for (const browserId of browserIds) {
                        const browserState = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'state');
                        const active = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'active');
                        
                        if (active === 'true' && browserState === state) {
                            return browserId;
                        }
                    }
                    
                    // If no active browser with matching state, try to find any browser with matching state
                    for (const browserId of browserIds) {
                        const browserState = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'state');
                        
                        if (browserState === state) {
                            return browserId;
                        }
                    }
                    
                    // If still no matching browser, return null
                    return null;
                }
                
                // If no state filter, find any active browser
                for (const browserId of browserIds) {
                    const active = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'active');
                    
                    if (active === 'true') {
                        return browserId;
                    }
                }
                
                // If no active browser, return the first one
                return browserIds.length > 0 ? browserIds[0] : null;
            }
            
            // Fall back to checking all browsers if multiple users have browsers
            if (state) {
                // Look for active browsers with the specific state
                for (const browserId of allBrowsers) {
                    const browserState = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'state');
                    const active = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'active');
                    
                    if (active === 'true' && browserState === state) {
                        return browserId;
                    }
                }
                
                // Then look for any browser with the specific state
                for (const browserId of allBrowsers) {
                    const browserState = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'state');
                    
                    if (browserState === state) {
                        return browserId;
                    }
                }
                
                // If no browser with the requested state is found, return null
                return null;
            }
            
            // If no state filter, find any active browser
            for (const browserId of allBrowsers) {
                const active = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'active');
                
                if (active === 'true') {
                    return browserId;
                }
            }
            
            // If all else fails, return the first browser
            return allBrowsers.length > 0 ? allBrowsers[0] : null;
        } catch (error) {
            logger.log('error', `Failed to get active browser for migration: ${error}`);
            return null;
        }
    };

    /**
    * Returns the first active browser's instance id from the pool.
    * If there is no active browser, it returns null.
    * If there are multiple active browsers, it returns the first one.
    * 
    * @returns the first remote active browser instance's id from the pool
    * @deprecated Use getBrowserIdForUser instead to enforce the 1 User - 2 Browser policy
    */
   public getActiveBrowserIdLegacy = async (): Promise<string | null> => {
    try {
        // Get all browser IDs
        const allBrowsers = await redisClient.smembers(REDIS_KEYS.ALL_BROWSERS);
        
        // Find the first active browser
        for (const browserId of allBrowsers) {
            const active = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(browserId), 'active');
            
            if (active === 'true') {
                return browserId;
            }
        }
        
        return null;
    } catch (error) {
        logger.log('error', `Failed to get active browser ID (legacy): ${error}`);
        return null;
    }
};

/**
 * Gets the remaining session time for a browser
 * 
 * @param id the browser ID
 * @returns the remaining time in milliseconds, or null if the browser wasn't found
 */
public getRemainingSessionTime = async (id: string): Promise<number | null> => {
    try {
        const exists = await redisClient.exists(REDIS_KEYS.BROWSER_INFO(id));
        if (!exists) {
            return null;
        }
        
        const startTimeStr = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'startTime');
        const maxDurationStr = await redisClient.hget(REDIS_KEYS.BROWSER_INFO(id), 'maxSessionDuration');
        
        if (!startTimeStr || !maxDurationStr) {
            return null;
        }
        
        const startTime = parseInt(startTimeStr, 10);
        const maxDuration = parseInt(maxDurationStr, 10);
        const elapsedTime = Date.now() - startTime;
        
        return Math.max(0, maxDuration - elapsedTime);
    } catch (error) {
        logger.log('error', `Failed to get remaining session time: ${error}`);
        return null;
    }
};

/**
 * Cleanup and release resources when shutting down
 */
public cleanup = async (): Promise<void> => {
    try {
        // Clear all session timers
        for (const [browserId, timer] of this.sessionTimers.entries()) {
            clearInterval(timer);
            this.sessionTimers.delete(browserId);
        }
        
        // Close all browsers in local cache
        for (const id in this.pool) {
            try {
                await this.pool[id].browser.switchOff();
            } catch (error) {
                logger.log('warn', `Error closing browser ${id} during cleanup: ${error}`);
            }
        }
        
        logger.log('debug', 'BrowserPool cleanup completed');
    } catch (error) {
        logger.log('error', `Error during BrowserPool cleanup: ${error}`);
    }
};
}