import {
    Page,
    Browser,
    CDPSession,
    BrowserContext,
} from 'playwright';
import { Socket } from "socket.io";
import { chromium } from 'playwright-extra';
import stealthPlugin from 'puppeteer-extra-plugin-stealth';
import { PlaywrightBlocker } from '@cliqz/adblocker-playwright';
import fetch from 'cross-fetch';
import sharp from 'sharp';
import logger from '../../logger';
import { InterpreterSettings } from "../../types";
import { WorkflowGenerator } from "../../workflow-management/classes/Generator";
import { WorkflowInterpreter } from "../../workflow-management/classes/Interpreter";
import { getDecryptedProxyConfig } from '../../routes/proxy';
import { getInjectableScript } from 'idcac-playwright';
import { redisClient } from '../../storage/connection';

chromium.use(stealthPlugin());

const MEMORY_CONFIG = {
    gcInterval: 20000, // Check memory more frequently (20s instead of 60s)
    maxHeapSize: 1536 * 1024 * 1024, // 1.5GB
    heapUsageThreshold: 0.7 // 70% (reduced threshold to react earlier)
};

const SCREENCAST_CONFIG: {
    format: "jpeg" | "png";
    maxWidth: number;
    maxHeight: number;
    targetFPS: number;
    compressionQuality: number;
    maxQueueSize: number;
} = {
    format: 'png', 
    maxWidth: 1280,
    maxHeight: 720,
    targetFPS: 15, 
    compressionQuality: 0.95, 
    maxQueueSize: 1 
};

// Redis keys for the RemoteBrowser
const REDIS_KEYS = {
    BROWSER_SESSION: (id: string) => `browser:${id}:session`,
    BROWSER_ACTIVITY: (id: string) => `browser:${id}:activity`,
    SESSION_EXPIRY: (id: string) => `browser:${id}:expiry`,
};

/**
 * The maximum duration of a recording session in milliseconds (10 minutes)
 */
const MAX_SESSION_DURATION = 1.5 * 60 * 1000;

/**
 * The warning time before session ends in milliseconds (1 minute)
 */
const WARNING_TIME = 1 * 60 * 1000;

/**
 * This class represents a remote browser instance.
 * It is used to allow a variety of interaction with the Playwright's browser instance.
 * Every remote browser holds an instance of a generator and interpreter classes with
 * the purpose of generating and interpreting workflows.
 * Integrated with Redis for session management and tracking.
 * @category BrowserManagement
 */
export class RemoteBrowser {

    /**
     * Playwright's [browser](https://playwright.dev/docs/api/class-browser) instance.
     * @private
     */
    private browser: Browser | null = null;

    private context: BrowserContext | null = null;

    /**
     * The Playwright's [CDPSession](https://playwright.dev/docs/api/class-cdpsession) instance,
     * used to talk raw Chrome Devtools Protocol.
     * @private
     */
    private client: CDPSession | null | undefined = null;

    /**
     * Socket.io socket instance enabling communication with the client (frontend) side.
     */
    public socket: Socket;

    /**
     * The Playwright's [Page](https://playwright.dev/docs/api/class-page) instance
     * as current interactive remote browser's page.
     * @private
     */
    private currentPage: Page | null | undefined = null;

    /**
     * Interpreter settings for any started interpretation.
     * @private
     */
    private interpreterSettings: InterpreterSettings = {
        debug: false,
        maxConcurrency: 1,
        maxRepeats: 1,
    };

    /**
     * The user ID that owns this browser instance
     */
    public userId: string;

    /**
     * The browser ID for this instance
     */
    private browserId: string;

    /**
     * Timestamp when the browser session was started
     */
    private startTime: number;

    /**
     * Session expiry timer
     */
    private sessionExpiryTimer: NodeJS.Timeout | null = null;

    /**
     * Session warning timer
     */
    private sessionWarningTimer: NodeJS.Timeout | null = null;

    private lastEmittedUrl: string | null = null;

    /**
     * {@link WorkflowGenerator} instance specific to the remote browser.
     */
    public generator: WorkflowGenerator;

    /**
     * {@link WorkflowInterpreter} instance specific to the remote browser.
     */
    public interpreter: WorkflowInterpreter;

    private screenshotQueue: Buffer[] = [];
    private isProcessingScreenshot = false;
    private screencastInterval: NodeJS.Timeout | null = null
    private isScreencastActive: boolean = false;

    /**
     * Initializes a new instances of the {@link Generator} and {@link WorkflowInterpreter} classes and
     * assigns the socket instance everywhere.
     * @param socket socket.io socket instance used to communicate with the client side
     * @param userId the user ID that owns this browser instance
     * @param browserId unique ID for this browser instance
     * @constructor
     */
    public constructor(socket: Socket, userId: string, browserId: string) {
        this.socket = socket;
        this.userId = userId;
        this.browserId = browserId;
        this.startTime = Date.now();
        this.interpreter = new WorkflowInterpreter(socket);
        this.generator = new WorkflowGenerator(socket);
    }

    /**
     * Setup session management timers
     */
    private async setupSessionManagement(): Promise<void> {
        try {
            // Store session information in Redis
            await redisClient.hmset(REDIS_KEYS.BROWSER_SESSION(this.browserId), {
                userId: this.userId,
                startTime: this.startTime.toString(),
                maxDuration: MAX_SESSION_DURATION.toString(),
                warningIssued: 'false',
            });
            
            // Set expiry timer
            const remainingTime = MAX_SESSION_DURATION;
            
            // Set session warning timer (1 minute before expiry)
            this.sessionWarningTimer = setTimeout(() => {
                this.emitSessionWarning(WARNING_TIME / 1000); // Convert to seconds
            }, remainingTime - WARNING_TIME);
            
            // Set session expiry timer
            this.sessionExpiryTimer = setTimeout(() => {
                this.emitSessionExpired();
            }, remainingTime);
            
            logger.log('debug', `Session management set up for browser ${this.browserId}, user ${this.userId}`);
            
            // Keep track of browser activity in Redis
            await this.updateActivityInRedis();
        } catch (error) {
            logger.log('error', `Failed to setup session management: ${error}`);
        }
    }
    
    /**
     * Update the browser's activity timestamp in Redis
     */
    private async updateActivityInRedis(): Promise<void> {
        try {
            await redisClient.set(REDIS_KEYS.BROWSER_ACTIVITY(this.browserId), Date.now().toString());
            
            // Set the expiry time a bit longer than the session to ensure we can track
            // whether the session ended normally or abnormally
            await redisClient.expire(REDIS_KEYS.BROWSER_ACTIVITY(this.browserId), 
                Math.ceil(MAX_SESSION_DURATION / 1000) + 300); // Add 5 minutes extra
        } catch (error) {
            logger.log('error', `Failed to update activity status in Redis: ${error}`);
        }
    }
    
    /**
     * Emit a warning to the frontend about session expiry
     */
    private emitSessionWarning(remainingSeconds: number): void {
        try {
            this.socket.emit('sessionWarning', {
                browserId: this.browserId,
                userId: this.userId,
                remainingTime: remainingSeconds,
            });
            
            // Mark warning as issued
            redisClient.hset(REDIS_KEYS.BROWSER_SESSION(this.browserId), 'warningIssued', 'true');
            logger.log('debug', `Session warning emitted for browser ${this.browserId}`);
        } catch (error) {
            logger.log('error', `Failed to emit session warning: ${error}`);
        }
    }
    
    /**
     * Emit session expiry notification and initiate shutdown
     */
    private async emitSessionExpired(): Promise<void> {
        try {
            this.socket.emit('sessionEnded', {
                browserId: this.browserId,
                userId: this.userId,
                reason: 'timeout',
            });
            
            logger.log('debug', `Session expired for browser ${this.browserId}`);
            
            // Shutdown browser
            await this.switchOff();
        } catch (error) {
            logger.log('error', `Failed to handle session expiration: ${error}`);
        }
    }

    private initializeMemoryManagement(): void {
        setInterval(() => {
            const memoryUsage = process.memoryUsage();
            const heapUsageRatio = memoryUsage.heapUsed / MEMORY_CONFIG.maxHeapSize;
            
            if (heapUsageRatio > MEMORY_CONFIG.heapUsageThreshold * 1.2) {
                logger.warn('Critical memory pressure detected, triggering emergency cleanup');
                this.performMemoryCleanup();
            } else if (heapUsageRatio > MEMORY_CONFIG.heapUsageThreshold) {
                logger.warn('High memory usage detected, triggering cleanup');
                
                if (this.screenshotQueue.length > 0) {
                    this.screenshotQueue = [];
                    logger.info('Screenshot queue cleared due to memory pressure');
                }
                
                if (global.gc && heapUsageRatio > MEMORY_CONFIG.heapUsageThreshold * 1.1) {
                    global.gc();
                }
            }
            
            if (this.screenshotQueue.length > SCREENCAST_CONFIG.maxQueueSize) {
                this.screenshotQueue = this.screenshotQueue.slice(-SCREENCAST_CONFIG.maxQueueSize);
            }
        }, MEMORY_CONFIG.gcInterval);
    }

    private async performMemoryCleanup(): Promise<void> {
        this.screenshotQueue = [];
        this.isProcessingScreenshot = false;
        
        if (global.gc) {
            try {
                global.gc();
                logger.info('Garbage collection requested');
            } catch (error) {
                logger.error('Error during garbage collection:', error);
            }
        }
        
        if (this.client) {
            try {
                await this.stopScreencast();
                
                await new Promise(resolve => setTimeout(resolve, 500));
                
                this.client = null;
                if (this.currentPage) {
                    this.client = await this.currentPage.context().newCDPSession(this.currentPage);
                    await this.startScreencast();
                    logger.info('CDP session reset completed');
                }
            } catch (error) {
                logger.error('Error resetting CDP session:', error);
            }
        }
        
        this.socket.emit('memory-cleanup', {
            userId: this.userId,
            browserId: this.browserId,
            timestamp: Date.now()
        });
    }

    /**
     * Normalizes URLs to prevent navigation loops while maintaining consistent format
     */
    private normalizeUrl(url: string): string {
        try {
            const parsedUrl = new URL(url);
            // Remove trailing slashes except for root path
            parsedUrl.pathname = parsedUrl.pathname.replace(/\/+$/, '') || '/';
            // Ensure consistent protocol handling
            parsedUrl.protocol = parsedUrl.protocol.toLowerCase();
            return parsedUrl.toString();
        } catch {
            return url;
        }
    }

    /**
     * Determines if a URL change is significant enough to emit
     */
    private shouldEmitUrlChange(newUrl: string): boolean {
        if (!this.lastEmittedUrl) {
            return true;
        }
        const normalizedNew = this.normalizeUrl(newUrl);
        const normalizedLast = this.normalizeUrl(this.lastEmittedUrl);
        return normalizedNew !== normalizedLast;
    }

    private async setupPageEventListeners(page: Page) {
        page.on('framenavigated', async (frame) => {
            if (frame === page.mainFrame()) {
                const currentUrl = page.url();
                if (this.shouldEmitUrlChange(currentUrl)) {
                    this.lastEmittedUrl = currentUrl;
                    this.socket.emit('urlChanged', {
                        url: currentUrl, 
                        userId: this.userId,
                        browserId: this.browserId
                    });
                }
            }
        });

        // Handle page load events with retry mechanism
        page.on('load', async () => {
            const injectScript = async (): Promise<boolean> => {
                try {
                    await page.waitForLoadState('networkidle', { timeout: 5000 });

                    await page.evaluate(getInjectableScript());
                    return true;
                } catch (error: any) {
                    logger.log('warn', `Script injection attempt failed: ${error.message}`);
                    return false;
                }
            };

            const success = await injectScript();
            console.log("Script injection result:", success);
            
            // Update activity timestamp on page load
            await this.updateActivityInRedis();
        });
    }

    private getUserAgent() {
        const userAgents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.140 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.1938.81 Safari/537.36 Edg/116.0.1938.81',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36 OPR/101.0.4843.25',
            'Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.5938.62 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0',
        ];

        return userAgents[Math.floor(Math.random() * userAgents.length)];
    }

    /**
     * An asynchronous constructor for asynchronously initialized properties.
     * Must be called right after creating an instance of RemoteBrowser class.
     * @returns {Promise<void>}
     */
    public initialize = async (): Promise<void> => {
        const MAX_RETRIES = 3;
        let retryCount = 0;
        let success = false;
    
        while (!success && retryCount < MAX_RETRIES) {
            try {
                this.browser = <Browser>(await chromium.launch({
                    headless: true,
                    args: [
                        "--disable-blink-features=AutomationControlled",
                        "--disable-web-security",
                        "--disable-features=IsolateOrigins,site-per-process",
                        "--disable-site-isolation-trials",
                        "--disable-extensions",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--force-color-profile=srgb",
                        "--force-device-scale-factor=2",
                    ],
                }));
                
                if (!this.browser || this.browser.isConnected() === false) {
                    throw new Error('Browser failed to launch or is not connected');
                }
                
                const proxyConfig = await getDecryptedProxyConfig(this.userId);
                let proxyOptions: { server: string, username?: string, password?: string } = { server: '' };
                
                if (proxyConfig.proxy_url) {
                    proxyOptions = {
                        server: proxyConfig.proxy_url,
                        ...(proxyConfig.proxy_username && proxyConfig.proxy_password && {
                            username: proxyConfig.proxy_username,
                            password: proxyConfig.proxy_password,
                        }),
                    };
                }
                
                const contextOptions: any = {
                    reducedMotion: 'reduce',
                    javaScriptEnabled: true,
                    timeout: 50000,
                    forcedColors: 'none',
                    isMobile: false,
                    hasTouch: false,
                    userAgent: this.getUserAgent(),
                    deviceScaleFactor: 2,
                };
    
                if (proxyOptions.server) {
                    contextOptions.proxy = {
                        server: proxyOptions.server,
                        username: proxyOptions.username ? proxyOptions.username : undefined,
                        password: proxyOptions.password ? proxyOptions.password : undefined,
                    };
                }
    
                await new Promise(resolve => setTimeout(resolve, 500));
                
                const contextPromise = this.browser.newContext(contextOptions);
                this.context = await Promise.race([
                    contextPromise,
                    new Promise<never>((_, reject) => {
                        setTimeout(() => reject(new Error('Context creation timed out after 15s')), 15000);
                    })
                ]) as BrowserContext;
                
                await this.context.addInitScript(
                    `const defaultGetter = Object.getOwnPropertyDescriptor(
                      Navigator.prototype,
                      "webdriver"
                    ).get;
                    defaultGetter.apply(navigator);
                    defaultGetter.toString();
                    Object.defineProperty(Navigator.prototype, "webdriver", {
                      set: undefined,
                      enumerable: true,
                      configurable: true,
                      get: new Proxy(defaultGetter, {
                        apply: (target, thisArg, args) => {
                          Reflect.apply(target, thisArg, args);
                          return false;
                        },
                      }),
                    });
                    const patchedGetter = Object.getOwnPropertyDescriptor(
                      Navigator.prototype,
                      "webdriver"
                    ).get;
                    patchedGetter.apply(navigator);
                    patchedGetter.toString();`
                );
                
                this.currentPage = await this.context.newPage();
                await this.setupPageEventListeners(this.currentPage);
    
                const viewportSize = await this.currentPage.viewportSize();
                if (viewportSize) {
                    this.socket.emit('viewportInfo', {
                        width: viewportSize.width,
                        height: viewportSize.height,
                        userId: this.userId,
                        browserId: this.browserId
                    });
                }
    
                try {
                    const blocker = await PlaywrightBlocker.fromLists(fetch, ['https://easylist.to/easylist/easylist.txt']);
                    await blocker.enableBlockingInPage(this.currentPage);
                    this.client = await this.currentPage.context().newCDPSession(this.currentPage);
                    await blocker.disableBlockingInPage(this.currentPage);
                    console.log('Adblocker initialized');
                } catch (error: any) {
                    console.warn('Failed to initialize adblocker, continuing without it:', error.message);
                    // Still need to set up the CDP session even if blocker fails
                    this.client = await this.currentPage.context().newCDPSession(this.currentPage);
                }
                
                // Set up session management
                await this.setupSessionManagement();
                
                success = true;
                logger.log('debug', `Browser initialized successfully for user ${this.userId}, browser ${this.browserId}`);
            } catch (error: any) {
                retryCount++;
                logger.log('error', `Browser initialization failed (attempt ${retryCount}/${MAX_RETRIES}): ${error.message}`);
                
                if (this.browser) {
                    try {
                        await this.browser.close();
                    } catch (closeError) {
                        logger.log('warn', `Failed to close browser during cleanup: ${closeError}`);
                    }
                    this.browser = null;
                }
                
                if (retryCount >= MAX_RETRIES) {
                    throw new Error(`Failed to initialize browser after ${MAX_RETRIES} attempts: ${error.message}`);
                }
                
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    };

    /**
     * Gets the remaining session time
     * @returns Remaining time in milliseconds
     */
    public async getRemainingSessionTime(): Promise<number> {
        const sessionData = await redisClient.hgetall(REDIS_KEYS.BROWSER_SESSION(this.browserId));
        if (!sessionData || !sessionData.startTime || !sessionData.maxDuration) {
            return 0;
        }
        
        const startTime = parseInt(sessionData.startTime, 10);
        const maxDuration = parseInt(sessionData.maxDuration, 10);
        const elapsedTime = Date.now() - startTime;
        
        return Math.max(0, maxDuration - elapsedTime);
    }

    public updateViewportInfo = async (): Promise<void> => {
        if (this.currentPage) {
            const viewportSize = await this.currentPage.viewportSize();
            if (viewportSize) {
                this.socket.emit('viewportInfo', {
                    width: viewportSize.width,
                    height: viewportSize.height,
                    userId: this.userId,
                    browserId: this.browserId
                });
            }
        }
    };

    /**
     * Registers all event listeners needed for the recording editor session.
     * Should be called only once after the full initialization of the remote browser.
     * @returns void
     */
    public registerEditorEvents = (): void => {
        // For each event, include userId and browserId for proper routing
        logger.log('debug', `Registering editor events for user: ${this.userId}, browser: ${this.browserId}`);
        
        // Listen for specific events for this browser
        this.socket.on(`rerender:${this.browserId}`, async () => {
            logger.debug(`Rerender event received for browser ${this.browserId}`);
            await this.makeAndEmitScreenshot();
            await this.updateActivityInRedis();
        });
        
        // User-specific events
        this.socket.on(`rerender:${this.userId}`, async () => {
            logger.debug(`Rerender event received for user ${this.userId}`);
            await this.makeAndEmitScreenshot();
            await this.updateActivityInRedis();
        });
        
        // For backward compatibility, also listen to the general event
        this.socket.on('rerender', async () => {
            logger.debug(`General rerender event received, checking if for browser ${this.browserId}`);
            await this.makeAndEmitScreenshot();
            await this.updateActivityInRedis();
        });
        
        this.socket.on(`settings:${this.browserId}`, (settings) => {
            this.interpreterSettings = settings;
            logger.debug(`Settings updated for browser ${this.browserId}`);
            this.updateActivityInRedis();
        });
        
        this.socket.on(`changeTab:${this.browserId}`, async (tabIndex) => {
            logger.debug(`Tab change to ${tabIndex} requested for browser ${this.browserId}`);
            await this.changeTab(tabIndex);
            await this.updateActivityInRedis();
        });
        
        this.socket.on(`addTab:${this.browserId}`, async () => {
            logger.debug(`New tab requested for browser ${this.browserId}`);
            await this.currentPage?.context().newPage();
            const lastTabIndex = this.currentPage ? this.currentPage.context().pages().length - 1 : 0;
            await this.changeTab(lastTabIndex);
            await this.updateActivityInRedis();
        });
        
        this.socket.on(`closeTab:${this.browserId}`, async (tabInfo) => {
            logger.debug(`Close tab ${tabInfo.index} requested for browser ${this.browserId}`);
            const page = this.currentPage?.context().pages()[tabInfo.index];
            if (page) {
                if (tabInfo.isCurrent) {
                    if (this.currentPage?.context().pages()[tabInfo.index + 1]) {
                        // next tab
                        await this.changeTab(tabInfo.index + 1);
                    } else {
                        //previous tab
                        await this.changeTab(tabInfo.index - 1);
                    }
                }
                await page.close();
                logger.log(
                    'debug',
                    `Tab ${tabInfo.index} was closed for browser ${this.browserId}, new tab count: ${this.currentPage?.context().pages().length}`
                );
            } else {
                logger.log('error', `Tab index ${tabInfo.index} out of range for browser ${this.browserId}`);
            }
            await this.updateActivityInRedis();
        });
        
        this.socket.on(`setViewportSize:${this.browserId}`, async (data: { width: number, height: number }) => {
            const { width, height } = data;
            logger.log('debug', `Viewport size change to width=${width}, height=${height} requested for browser ${this.browserId}`);

            // Update the browser context's viewport dynamically
            if (this.context && this.browser) {
                this.context = await this.browser.newContext({ viewport: { width, height } });
                logger.log('debug', `Viewport size updated to width=${width}, height=${height} for browser ${this.browserId}`);
            }
            await this.updateActivityInRedis();
        });
        
        // For backward compatibility, also register the standard events
        this.socket.on('settings', (settings) => {
            this.interpreterSettings = settings;
            this.updateActivityInRedis();
        });
        
        this.socket.on('changeTab', async (tabIndex) => {
            await this.changeTab(tabIndex);
            await this.updateActivityInRedis();
        });
        
        this.socket.on('addTab', async () => {
            await this.currentPage?.context().newPage();
            const lastTabIndex = this.currentPage ? this.currentPage.context().pages().length - 1 : 0;
            await this.changeTab(lastTabIndex);
            await this.updateActivityInRedis();
        });
        
        this.socket.on('closeTab', async (tabInfo) => {
            const page = this.currentPage?.context().pages()[tabInfo.index];
            if (page) {
                if (tabInfo.isCurrent) {
                    if (this.currentPage?.context().pages()[tabInfo.index + 1]) {
                        await this.changeTab(tabInfo.index + 1);
                    } else {
                        await this.changeTab(tabInfo.index - 1);
                    }
                }
                await page.close();
            }
            await this.updateActivityInRedis();
        });
        
        this.socket.on('setViewportSize', async (data: { width: number, height: number }) => {
            const { width, height } = data;
            if (this.context && this.browser) {
                this.context = await this.browser.newContext({ viewport: { width, height } });
            }
            await this.updateActivityInRedis();
        });
    };
    
    /**
     * Subscribes the remote browser for a screencast session
     * on [CDP](https://chromedevtools.github.io/devtools-protocol/) level,
     * where screenshot is being sent through the socket
     * every time the browser's active page updates.
     * @returns {Promise<void>}
     */
    public subscribeToScreencast = async (): Promise<void> => {
        logger.log('debug', `Starting screencast for browser: ${this.browserId}`);
        await this.startScreencast();
        if (!this.client) {
            logger.log('warn', 'client is not initialized');
            return;
        }
        // Set flag to indicate screencast is active
        this.isScreencastActive = true;

        await this.updateViewportInfo();

        this.client.on('Page.screencastFrame', ({ data: base64, sessionId }) => {
            // Only process if screencast is still active for this browser
            if (!this.isScreencastActive) {
                return;
            }
            this.emitScreenshot(Buffer.from(base64, 'base64'))
            setTimeout(async () => {
                try {
                    if (!this.client || !this.isScreencastActive) {
                        logger.log('warn', 'client is not initialized');
                        return;
                    }
                    await this.client.send('Page.screencastFrameAck', { sessionId: sessionId });
                } catch (e: any) {
                    logger.log('error', `Screencast error: ${e}`);
                }
            }, 100);
        });
    };

    /**
     * Terminates the screencast session and closes the remote browser.
     * If an interpretation was running it will be stopped.
     * Cleans up all resources including Redis entries.
     * @returns {Promise<void>}
     */
    public async switchOff(): Promise<void> {
        try {
            this.isScreencastActive = false;

            // Clear all timers
            if (this.sessionWarningTimer) {
                clearTimeout(this.sessionWarningTimer);
                this.sessionWarningTimer = null;
            }
            
            if (this.sessionExpiryTimer) {
                clearTimeout(this.sessionExpiryTimer);
                this.sessionExpiryTimer = null;
            }
            
            if (this.screencastInterval) {
                clearInterval(this.screencastInterval);
            }

            await this.interpreter.stopInterpretation();

            if (this.client) {
                await this.stopScreencast();
            }

            if (this.browser) {
                await this.browser.close();
            }

            this.screenshotQueue = [];
            
            // Clean up Redis keys
            await Promise.all([
                redisClient.del(REDIS_KEYS.BROWSER_SESSION(this.browserId)),
                redisClient.del(REDIS_KEYS.BROWSER_ACTIVITY(this.browserId)),
                redisClient.del(REDIS_KEYS.SESSION_EXPIRY(this.browserId))
            ]);
            
            logger.log('debug', `Browser ${this.browserId} switched off and Redis data cleaned up`);
        } catch (error) {
            logger.error(`Error during browser ${this.browserId} shutdown:`, error);
        }
    }

    private async optimizeScreenshot(screenshot: Buffer): Promise<Buffer> {
        try {
            return await sharp(screenshot)
                .png({
                    quality: Math.round(SCREENCAST_CONFIG.compressionQuality * 100),                
                    compressionLevel: 6,        
                    adaptiveFiltering: true,    
                    force: true                 
                })
                .resize({
                    width: SCREENCAST_CONFIG.maxWidth,
                    height: SCREENCAST_CONFIG.maxHeight,
                    fit: 'inside',
                   withoutEnlargement: true,
                   kernel: 'lanczos3' 
               })
               .toBuffer();
       } catch (error) {
           logger.error('Screenshot optimization failed:', error);            
           return screenshot;
       }
   }
   
   /**
    * Makes and emits a single screenshot to the client side.
    * @returns {Promise<void>}
    */
   public makeAndEmitScreenshot = async (): Promise<void> => {
       try {
           const screenshot = await this.currentPage?.screenshot();
           if (screenshot) {
               this.emitScreenshot(screenshot);
           }
       } catch (e) {
           const { message } = e as Error;
           logger.log('error', `Screenshot error: ${message}`);
       }
   };

   /**
    * Updates the active socket instance.
    * This will update all registered events for the socket and
    * all the properties using the socket.
    * @param socket socket.io socket instance used to communicate with the client side
    * @returns void
    */
   public updateSocket = (socket: Socket): void => {
       this.socket = socket;
       this.registerEditorEvents();
       this.generator?.updateSocket(socket);
       this.interpreter?.updateSocket(socket);
   };

   /**
    * Starts the interpretation of the currently generated workflow.
    * @returns {Promise<void>}
    */
   public interpretCurrentRecording = async (): Promise<void> => {
       logger.log('debug', `Starting interpretation in the editor for browser ${this.browserId}`);
       if (this.generator) {
           const workflow = this.generator.AddGeneratedFlags(this.generator.getWorkflowFile());
           await this.initializeNewPage();
           if (this.currentPage) {
               const params = this.generator.getParams();
               if (params) {
                   this.interpreterSettings.params = params.reduce((acc, param) => {
                       if (this.interpreterSettings.params && Object.keys(this.interpreterSettings.params).includes(param)) {
                           return { ...acc, [param]: this.interpreterSettings.params[param] };
                       } else {
                           return { ...acc, [param]: '', }
                       }
                   }, {})
               }
               logger.log('debug', `Starting interpretation with settings: ${JSON.stringify(this.interpreterSettings, null, 2)}`);
               await this.interpreter.interpretRecordingInEditor(
                   workflow, this.currentPage,
                   (newPage: Page) => this.currentPage = newPage,
                   this.interpreterSettings
               );
               // clear the active index from generator
               this.generator.clearLastIndex();
               
               // Update activity timestamp
               await this.updateActivityInRedis();
           } else {
               logger.log('error', 'Could not get a new page, returned undefined');
           }
       } else {
           logger.log('error', 'Generator is not initialized');
       }
   };

   /**
    * Stops the workflow interpretation and initializes a new page.
    * @returns {Promise<void>}
    */
   public stopCurrentInterpretation = async (): Promise<void> => {
       await this.interpreter.stopInterpretation();
       await this.initializeNewPage();
       await this.updateActivityInRedis();
   };

   /**
    * Returns the current page instance.
    * @returns {Page | null | undefined}
    */
   public getCurrentPage = (): Page | null | undefined => {
       return this.currentPage;
   };

   /**
    * Changes the active page to the page instance on the given index
    * available in pages array on the {@link BrowserContext}.
    * Automatically stops the screencast session on the previous page and starts the new one.
    * @param tabIndex index of the page in the pages array on the {@link BrowserContext}
    * @returns {Promise<void>}
    */
   private changeTab = async (tabIndex: number): Promise<void> => {
       const page = this.currentPage?.context().pages()[tabIndex];
       if (page) {
           await this.stopScreencast();
           this.currentPage = page;

           await this.setupPageEventListeners(this.currentPage);

           this.client = await this.currentPage.context().newCDPSession(this.currentPage);
           // Include browserId in the URL change event
           this.socket.emit('urlChanged', { 
               url: this.currentPage.url(),
               userId: this.userId,
               browserId: this.browserId
           });
           await this.makeAndEmitScreenshot();
           await this.subscribeToScreencast();
           await this.updateActivityInRedis();
       } else {
           logger.log('error', `${tabIndex} index out of range of pages for browser ${this.browserId}`)
       }
   }

   /**
    * Internal method for a new page initialization. Subscribes this page to the screencast.
    * @param options optional page options to be used when creating a new page
    * @returns {Promise<void>}
    */
   private initializeNewPage = async (options?: Object): Promise<void> => {
       await this.stopScreencast();
       const newPage = options ? await this.browser?.newPage(options)
           : await this.browser?.newPage();
       await newPage?.setExtraHTTPHeaders({
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
       });

       await this.currentPage?.close();
       this.currentPage = newPage;
       if (this.currentPage) {
           await this.setupPageEventListeners(this.currentPage);

           this.client = await this.currentPage.context().newCDPSession(this.currentPage);
           await this.subscribeToScreencast();
           await this.updateActivityInRedis();
       } else {
           logger.log('error', `Could not get a new page for browser ${this.browserId}, returned undefined`);
       }
   };

   /**
    * Initiates screencast of the remote browser through socket,
    * registers listener for rerender event and emits the loaded event.
    * Should be called only once after the browser is fully initialized.
    * @returns {Promise<void>}
    */
   private async startScreencast(): Promise<void> {
       if (!this.client) {
           logger.warn(`Client is not initialized for browser ${this.browserId}`);
           return;
       }

       try {
           await this.client.send('Page.startScreencast', {
               format: SCREENCAST_CONFIG.format,
               quality: Math.round(SCREENCAST_CONFIG.compressionQuality * 100),
               maxWidth: SCREENCAST_CONFIG.maxWidth,
               maxHeight: SCREENCAST_CONFIG.maxHeight,
               everyNthFrame: 1 
           });
           
           this.isScreencastActive = true;
   
           this.client.on('Page.screencastFrame', async ({ data, sessionId }) => {
               try {
                   if (this.screenshotQueue.length >= SCREENCAST_CONFIG.maxQueueSize && this.isProcessingScreenshot) {
                       await this.client?.send('Page.screencastFrameAck', { sessionId });
                       return;
                   }
                   
                   const buffer = Buffer.from(data, 'base64');
                   this.emitScreenshot(buffer);
                   
                   setTimeout(async () => {
                       try {
                           if (this.client) {
                               await this.client.send('Page.screencastFrameAck', { sessionId });
                           }
                       } catch (e) {
                           logger.error(`Error acknowledging screencast frame for browser ${this.browserId}:`, e);
                       }
                   }, 10); 
               } catch (error) {
                   logger.error(`Screencast frame processing failed for browser ${this.browserId}:`, error);
                   
                   try {
                       await this.client?.send('Page.screencastFrameAck', { sessionId });
                   } catch (ackError) {
                       logger.error(`Failed to acknowledge screencast frame for browser ${this.browserId}:`, ackError);
                   }
               }
           });
           logger.info(`Screencast started successfully for browser ${this.browserId}`);
           await this.updateActivityInRedis();
       } catch (error) {
           logger.error(`Failed to start screencast for browser ${this.browserId}:`, error);
       }
   }

   private async stopScreencast(): Promise<void> {
       if (!this.client) {
           logger.error(`Client is not initialized for browser ${this.browserId}`);
           return;
       }

       try {
           // Set flag to indicate screencast is inactive
           this.isScreencastActive = false;
           await this.client.send('Page.stopScreencast');
           this.screenshotQueue = [];
           this.isProcessingScreenshot = false;
           logger.info(`Screencast stopped successfully for browser ${this.browserId}`);
       } catch (error) {
           logger.error(`Failed to stop screencast for browser ${this.browserId}:`, error);
       }
   }

   /**
    * Helper for emitting the screenshot of browser's active page through websocket.
    * @param payload the screenshot binary data
    * @returns void
    */
   private emitScreenshot = async (payload: Buffer, viewportSize?: { width: number, height: number }): Promise<void> => {
       if (this.screenshotQueue.length > SCREENCAST_CONFIG.maxQueueSize) {
           this.screenshotQueue = this.screenshotQueue.slice(-SCREENCAST_CONFIG.maxQueueSize);
       }
       
       if (this.isProcessingScreenshot) {
           if (this.screenshotQueue.length < SCREENCAST_CONFIG.maxQueueSize) {
               this.screenshotQueue.push(payload);
           }
           return;
       }
       
       this.isProcessingScreenshot = true;
       
       try {
           const optimizationPromise = this.optimizeScreenshot(payload);
           const timeoutPromise = new Promise<Buffer>((resolve) => {
               setTimeout(() => resolve(payload), 150);
           });
           
           const optimizedScreenshot = await Promise.race([optimizationPromise, timeoutPromise]);
           const base64Data = optimizedScreenshot.toString('base64');
           const dataWithMimeType = `data:image/${SCREENCAST_CONFIG.format};base64,${base64Data}`;
           
           payload = null as any;
           
           this.socket.emit('screencast', {
               image: dataWithMimeType,
               userId: this.userId,
               browserId: this.browserId,
               viewport: viewportSize || await this.currentPage?.viewportSize() || null,
               timestamp: Date.now()
           });
           
           // Update last activity time for session management
           await this.updateActivityInRedis();
       } catch (error) {
           logger.error(`Screenshot emission failed for browser ${this.browserId}:`, error);
           try {
               const base64Data = payload.toString('base64');
               const dataWithMimeType = `data:image/png;base64,${base64Data}`;
               
               this.socket.emit('screencast', {
                   image: dataWithMimeType,
                   userId: this.userId,
                   browserId: this.browserId,
                   viewport: viewportSize || await this.currentPage?.viewportSize() || null,
                   timestamp: Date.now()
               });
           } catch (e) {
               logger.error(`Fallback screenshot emission also failed for browser ${this.browserId}:`, e);
           }
       } finally {
           this.isProcessingScreenshot = false;
           
           if (this.screenshotQueue.length > 0) {
               const nextScreenshot = this.screenshotQueue.shift();  
               if (nextScreenshot) {
                   setTimeout(() => {
                       this.emitScreenshot(nextScreenshot);
                   }, 1000 / SCREENCAST_CONFIG.targetFPS);
               }
           }
       }
   };
}