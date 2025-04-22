import Interpreter, { WorkflowFile } from "maxun-core";
import logger from "../../logger";
import { Socket } from "socket.io";
import { Page } from "playwright";
import { InterpreterSettings } from "../../types";
import { decrypt } from "../../utils/auth";
import { v4 as uuidv4 } from 'uuid';
import { ScrapingStateService } from "../../storage/mino";

/**
 * Decrypts any encrypted inputs in the workflow. If checkLimit is true, it will also handle the limit validation for scrapeList action.
 * @param workflow The workflow to decrypt.
 * @param checkLimit If true, it will handle the limit validation for scrapeList action.
 */
function processWorkflow(workflow: WorkflowFile, checkLimit: boolean = false): WorkflowFile {
  const processedWorkflow = JSON.parse(JSON.stringify(workflow)) as WorkflowFile;

  processedWorkflow.workflow.forEach((pair) => {
    pair.what.forEach((action) => {
      // Handle limit validation for scrapeList action
      if (action.action === 'scrapeList' && checkLimit && Array.isArray(action.args) && action.args.length > 0) {
        const scrapeConfig = action.args[0];
        if (scrapeConfig && typeof scrapeConfig === 'object' && 'limit' in scrapeConfig) {
          if (typeof scrapeConfig.limit === 'number' && scrapeConfig.limit > 5) {
            scrapeConfig.limit = 5;
          }
        }
      }

      // Handle decryption for type and press actions
      if ((action.action === 'type' || action.action === 'press') && Array.isArray(action.args) && action.args.length > 1) {
        try {
          const encryptedValue = action.args[1];
          if (typeof encryptedValue === 'string') {
            const decryptedValue = decrypt(encryptedValue);
            action.args[1] = decryptedValue;
          } else {
            logger.log('error', 'Encrypted value is not a string');
            action.args[1] = '';
          }
        } catch (error: unknown) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          logger.log('error', `Failed to decrypt input value: ${errorMessage}`);
          action.args[1] = '';
        }
      }
    });
  });

  return processedWorkflow;
}

/**
 * This class implements the main interpretation functions.
 * It holds some information about the current interpretation process and
 * registers to some events to allow the client (frontend) to interact with the interpreter.
 * It uses the [maxun-core](https://www.npmjs.com/package/maxun-core)
 * library to interpret the workflow.
 * @category WorkflowManagement
 */
export class WorkflowInterpreter {
  /**
   * Socket.io socket instance enabling communication with the client (frontend) side.
   * @private
   */
  private socket: Socket;

  /**
   * True if the interpretation is paused.
   */
  public interpretationIsPaused: boolean = false;

  /**
   * The instance of the {@link Interpreter} class used to interpret the workflow.
   * From maxun-core.
   * @private
   */
  private interpreter: Interpreter | null = null;

  /**
   * An id of the currently interpreted pair in the workflow.
   * @private
   */
  private activeId: number | null = null;

  /**
   * An array of debug messages emitted by the {@link Interpreter}.
   */
  public debugMessages: string[] = [];

  /**
   * An array of all the serializable data extracted from the run.
   */
  public serializableData: string[] = [];

  /**
   * An array of all the binary data extracted from the run.
   */
  public binaryData: { mimetype: string, data: string }[] = [];

  /**
   * An array of id's of the pairs from the workflow that are about to be paused.
   * As "breakpoints".
   * @private
   */
  private breakpoints: boolean[] = [];

  /**
   * Service for handling scraping state and checkpoints
   */
  private scrapingStateService: ScrapingStateService;

  /**
   * Callback to resume the interpretation after a pause.
   * @private
   */
  private interpretationResume: (() => void) | null = null;
  private scrapingCompleted: boolean = false;
  private currentScrapingState: any | null = null;
  private currentRunId: string | null = null;

  /**
   * A public constructor taking a socket instance for communication with the client.
   * @param socket Socket.io socket instance enabling communication with the client (frontend) side.
   * @constructor
   */
  constructor(socket: Socket) {
    this.socket = socket;
    this.scrapingStateService = new ScrapingStateService();
  }

  /**
   * Gets the current run ID that's being processed
   * @returns The current run ID or null if not available
   */
  public getCurrentRunId = (): string | null => {
    return this.currentRunId;
  };

  /**
   * Subscribes to the events that are used to control the interpretation.
   * The events are pause, resume, step and breakpoints.
   * Step is used to interpret a single pair and pause on the other matched pair.
   * @returns void
   */
  public subscribeToPausing = () => {
    this.socket.on('pause', () => {
      this.interpretationIsPaused = true;
    });
    this.socket.on('resume', () => {
      this.interpretationIsPaused = false;
      if (this.interpretationResume) {
        this.interpretationResume();
        this.socket.emit('log', '----- The interpretation has been resumed -----', false);
      } else {
        logger.log('debug', "Resume called but no resume function is set");
      }
    });
    this.socket.on('step', () => {
      if (this.interpretationResume) {
        this.interpretationResume();
      } else {
        logger.log('debug', "Step called but no resume function is set");
      }
    });
    this.socket.on('breakpoints', (data: boolean[]) => {
      logger.log('debug', "Setting breakpoints: " + data);
      this.breakpoints = data
    });
  }

  /**
   * Sets up the instance of {@link Interpreter} and interprets
   * the workflow inside the recording editor.
   * Cleans up this interpreter instance after the interpretation is finished.
   * @param workflow The workflow to interpret.
   * @param page The page instance used to interact with the browser.
   * @param updatePageOnPause A callback to update the page after a pause.
   * @returns {Promise<void>}
   */
  public interpretRecordingInEditor = async (
    workflow: WorkflowFile,
    page: Page,
    updatePageOnPause: (page: Page) => void,
    settings: InterpreterSettings,
  ) => {
    const params = settings.params ? settings.params : null;
    delete settings.params;

    const processedWorkflow = processWorkflow(workflow, true);

    const options = {
      ...settings,
      debugChannel: {
        activeId: (id: any) => {
          this.activeId = id;
          this.socket.emit('activePairId', id);
        },
        debugMessage: (msg: any) => {
          this.debugMessages.push(`[${new Date().toLocaleString()}] ` + msg);
          this.socket.emit('log', msg)
        },
      },
      serializableCallback: (data: any) => {
        this.socket.emit('serializableCallback', data);
      },
      binaryCallback: (data: string, mimetype: string) => {
        this.socket.emit('binaryCallback', { data, mimetype });
      }
    }

    const interpreter = new Interpreter(processedWorkflow, options);
    this.interpreter = interpreter;

    interpreter.on('flag', async (page, resume) => {
      if (this.activeId !== null && this.breakpoints[this.activeId]) {
        logger.log('debug', `breakpoint hit id: ${this.activeId}`);
        this.socket.emit('breakpointHit');
        this.interpretationIsPaused = true;
      }

      if (this.interpretationIsPaused) {
        this.interpretationResume = resume;
        logger.log('debug', `Paused inside of flag: ${page.url()}`);
        updatePageOnPause(page);
        this.socket.emit('log', '----- The interpretation has been paused -----', false);
      } else {
        resume();
      }
    });

    this.socket.emit('log', '----- Starting the interpretation -----', false);

    const status = await interpreter.run(page, params);

    this.socket.emit('log', `----- The interpretation finished with status: ${status} -----`, false);

    logger.log('debug', `Interpretation finished`);
    this.interpreter = null;
    this.socket.emit('activePairId', -1);
    this.interpretationIsPaused = false;
    this.interpretationResume = null;
    this.socket.emit('finished');
  };

  /**
   * Stops the current process of the interpretation of the workflow.
   * @returns {Promise<void>}
   */
  public stopInterpretation = async () => {
    if (this.interpreter) {
      logger.log('info', 'Stopping the interpretation.');
      await this.interpreter.stop();
      this.socket.emit('log', '----- The interpretation has been stopped -----', false);
      this.clearState();
    } else {
      logger.log('error', 'Cannot stop: No active interpretation.');
    }
  };

  private clearState = () => {
    this.debugMessages = [];
    this.interpretationIsPaused = false;
    this.activeId = null;
    this.interpreter = null;
    this.breakpoints = [];
    this.interpretationResume = null;
    this.serializableData = [];
    this.binaryData = [];
    this.currentRunId = null;
  }

/**
   * Executes a scrapeList action in a checkpoint-based manner utilizing the full browser session duration
   * @param page Current browser page
   * @param action The scrapeList action configuration
   * @param runId ID of the current run
   * @returns Promise resolving to the merged scraping results and completion status
   */
private async executeCheckpointedScraping(
  page: Page,
  action: any,
  runId: string
): Promise<{results: any[], completed: boolean}> {
  if (!action || !action.args || !action.args[0]) {
    logger.log('error', 'Invalid scraping action configuration');
    return { results: [], completed: true };
  }

  const scrapingId = `scraping-${runId}`;
  logger.log('info', `Starting checkpointed scraping with ID: ${scrapingId}`);
  
  // Get the latest checkpoint for this scraping operation
  let checkpoint = await this.scrapingStateService.getLatestScrapingCheckpoint(runId, scrapingId);
  let isComplete = false;

  try {
    if (!this.interpreter) {
      throw new Error('Interpreter is not initialized.');
    }
    
    logger.log('info', `Executing scraping with${checkpoint ? '' : 'out'} checkpoint`);
    
    // Execute a single scraping operation using the full browser session duration
    // No explicit timeout - the browser's session timeout will naturally interrupt this
    const { results: batchResults, checkpoint: newCheckpoint, completed } = 
      await this.interpreter.handlePagination(
        page, 
        action.args[0], 
        scrapingId,
        checkpoint,
      );
    
    // Store the results for this batch
    await this.scrapingStateService.storeScrapingResults(runId, scrapingId, batchResults);
    
    // Store the checkpoint for resuming in a new browser if needed
    await this.scrapingStateService.storeScrapingCheckpoint(runId, scrapingId, newCheckpoint);
    
    isComplete = completed;
    
    logger.log('info', `Scraping batch completed. Completed: ${isComplete}. Results: ${batchResults.length}`);
    
    // Save the current scraping state so it can be accessed by the parent process
    this.currentScrapingState = {
      scrapingId,
      checkpoint: newCheckpoint,
      completed: isComplete,
      results: batchResults
    };

    return { 
      results: batchResults,
      completed: isComplete
    };
    
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.log('error', `Error in scraping: ${errorMessage}`);
    
    // Even on error, return the current state so we can resume
    return { 
      results: [],
      completed: false
    };
  }
}

/**
 * Executes a scrapeSchema action in a checkpoint-based manner utilizing the full browser session duration
 * @param page Current browser page
 * @param action The scrapeSchema action configuration
 * @param runId ID of the current run
 * @returns Promise resolving to the merged scraping results and completion status
 */
private async executeCheckpointedScrapingSchema(
  page: Page,
  action: any,
  runId: string
): Promise<{results: any[], completed: boolean}> {
  if (!action || !action.args || !action.args[0]) {
    logger.log('error', 'Invalid scraping schema action configuration');
    return { results: [], completed: true };
  }

  const schemaConfig = action.args[0];
  const scrapingId = `schema-scraping-${runId}`;
  logger.log('info', `Starting checkpointed schema scraping with ID: ${scrapingId}`);
  
  // Get the latest checkpoint for this scraping operation
  let checkpoint = await this.scrapingStateService.getLatestScrapingCheckpoint(runId, scrapingId);
  let isComplete = false;
  
  try {
    if (!this.interpreter) {
      throw new Error('Interpreter is not initialized.');
    }
    
    logger.log('info', `Executing schema scraping with${checkpoint ? '' : 'out'} checkpoint`);
    
    // Create a schema scraping checkpoint structure if one doesn't exist
    if (!checkpoint) {
      checkpoint = {
        scrapingId,
        pageUrl: page.url(),
        accumulatedResults: {},
        visitedUrls: [],
        isComplete: false
      };
    }
    
    // Track the current URL to avoid re-scraping the same page
    const currentUrl = page.url();
    
    // If we've already scraped this URL, return the existing results
    if (checkpoint.visitedUrls.includes(currentUrl)) {
      logger.log('info', `URL ${currentUrl} already scraped, skipping`);
      return { 
        results: [checkpoint.accumulatedResults],
        completed: checkpoint.isComplete
      };
    }
    
    // Execute the schema scraping
    let schemaResults = {};
    
    try {
      // Ensure required scripts are loaded
      await this.interpreter.ensureScriptsLoaded(page);
      
      // Execute the scrapeSchema action
      schemaResults = await page.evaluate((schema) => window.scrapeSchema(schema), schemaConfig);
      
      // Handle if the result is an array (unlikely for schema scraping, but possible)
      if (Array.isArray(schemaResults)) {
        schemaResults = schemaResults.reduce((merged, item) => ({ ...merged, ...item }), {});
      }
      
      logger.log('info', `Schema scraping successful, merging results`);
      
      // Merge with existing results
      const mergedResults = {
        ...checkpoint.accumulatedResults,
        ...schemaResults
      };
      
      // Update the checkpoint
      const newCheckpoint = {
        ...checkpoint,
        accumulatedResults: mergedResults,
        visitedUrls: [...checkpoint.visitedUrls, currentUrl],
        pageUrl: currentUrl,
        lastUpdated: new Date().toISOString()
      };
      
      // Store the updated checkpoint
      await this.scrapingStateService.storeScrapingCheckpoint(runId, scrapingId, newCheckpoint);
      
      // Save the current scraping state so it can be accessed by the parent process
      this.currentScrapingState = {
        scrapingId,
        checkpoint: newCheckpoint,
        completed: true,
        results: [mergedResults]
      };
      
      // For schema scraping, consider it complete after successful execution
      isComplete = true;
      
      logger.log('info', `Schema scraping completed. Results merged.`);
      
      return { 
        results: [mergedResults],
        completed: true
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.log('error', `Error in schema scraping: ${errorMessage}`);
      
      // Even on error, return the current state so we can resume
      return { 
        results: [checkpoint.accumulatedResults],
        completed: false
      };
    }
  } catch (outerError) {
    const errorMessage = outerError instanceof Error ? outerError.message : String(outerError);
    logger.log('error', `Outer error in schema scraping: ${errorMessage}`);
    
    return { 
      results: checkpoint ? [checkpoint.accumulatedResults] : [],
      completed: false
    };
  }
}

/**
   * Modified InterpretRecording to properly track checkpoint state between browser recreations
   */
public InterpretRecording = async (
  workflow: WorkflowFile, 
  page: Page, 
  updatePageOnPause: (page: Page) => void,
  settings: InterpreterSettings
) => {
  const params = settings.params ? settings.params : null;
  delete settings.params;

  const runId = settings.runId || uuidv4();
  // Store the runId as a class property for later access
  this.currentRunId = runId;
  
  logger.log('info', `Starting interpretation for run: ${runId}`);

  const processedWorkflow = processWorkflow(workflow);

  let originalScrapeList: Function | null = null;
  let originalScrapeSchema: Function | null = null;

  this.scrapingCompleted = true;

  const options = {
    ...settings,
    debugChannel: {
      activeId: (id: any) => {
        this.activeId = id;
        this.socket.emit('activePairId', id);
      },
      debugMessage: (msg: any) => {
        this.debugMessages.push(`[${new Date().toLocaleString()}] ` + msg);
        this.socket.emit('debugMessage', msg)
      },
    },
    serializableCallback: (data: any) => {
      this.serializableData.push(data);
      this.socket.emit('serializableCallback', data);
    },
    binaryCallback: async (data: string, mimetype: string) => {
      this.binaryData.push({ mimetype, data: JSON.stringify(data) });
      this.socket.emit('binaryCallback', { data, mimetype });
    }
  }

  const interpreter = new Interpreter(processedWorkflow, options);
  this.interpreter = interpreter;

  if (interpreter.carryOutSteps) {
    originalScrapeList = interpreter.carryOutSteps;
    
    interpreter.carryOutSteps = async (page, steps) => {
      for (let i = 0; i < steps.length; i++) {
        const step = steps[i];
        
        if (step.action === 'scrapeList') {
          logger.log('info', 'Intercepting scrapeList action for checkpoint-based execution');
          
          try {
            const { results, completed } = await this.executeCheckpointedScraping(page, step, runId);
            
            if (options.serializableCallback) {
              options.serializableCallback(results);
            }
            
            // Update the overall completion status
            if (!completed) {
              this.scrapingCompleted = false;
              logger.log('info', 'Scraping not completed, will need to resume in a new browser');
            }
            
          } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            logger.log('error', `Error in checkpoint-based scraping: ${errorMessage}`);
            // Even if we get an error, we'll mark as incomplete to retry
            this.scrapingCompleted = false;
          }
        } 
        else if (step.action === 'scrapeSchema') {
          logger.log('info', 'Intercepting scrapeSchema action for checkpoint-based execution');
          
          try {
            const { results, completed } = await this.executeCheckpointedScrapingSchema(page, step, runId);
            
            if (options.serializableCallback) {
              options.serializableCallback(results);
            }
            
            // Update the overall completion status
            if (!completed) {
              this.scrapingCompleted = false;
              logger.log('info', 'Schema scraping not completed, will need to resume in a new browser');
            }
            
          } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            logger.log('error', `Error in checkpoint-based schema scraping: ${errorMessage}`);
            // Even if we get an error, we'll mark as incomplete to retry
            this.scrapingCompleted = false;
          }
        }
        else if (originalScrapeList) {
          await originalScrapeList.call(interpreter, page, [step]);
        }
      }
    };
  }

  interpreter.on('flag', async (page, resume) => {
    if (this.activeId !== null && this.breakpoints[this.activeId]) {
      logger.log('debug', `breakpoint hit id: ${this.activeId}`);
      this.socket.emit('breakpointHit');
      this.interpretationIsPaused = true;
    }

    if (this.interpretationIsPaused) {
      this.interpretationResume = resume;
      logger.log('debug', `Paused inside of flag: ${page.url()}`);
      updatePageOnPause(page);
      this.socket.emit('log', '----- The interpretation has been paused -----', false);
    } else {
      resume();
    }
  });

  const status = await interpreter.run(page, params);

  const lastArray = this.serializableData.length > 1
    ? [this.serializableData[this.serializableData.length - 1]]
    : this.serializableData;

  const result = {
    log: this.debugMessages,
    result: status,
    serializableOutput: lastArray.reduce((reducedObject, item, index) => {
      return {
        [`item-${index}`]: item,
        ...reducedObject,
      }
    }, {}),
    binaryOutput: this.binaryData.reduce((reducedObject, item, index) => {
      return {
        [`item-${index}`]: item,
        ...reducedObject,
      }
    }, {}),
    scrapingCompleted: this.scrapingCompleted,
    currentScrapingState: this.currentScrapingState
  }

  logger.log('debug', `Interpretation finished for run: ${runId}, all scraping completed: ${this.scrapingCompleted}`);
  
  return result;
}


/**
 * Gets the current results of the interpretation process.
 * This is useful when a browser times out and we need to save the current state.
 */
public getCurrentResults = async (): Promise<{
  log: string[];
  serializableOutput: any;
  binaryOutput: any;
  checkpoint: any;
  runId: string | null;
}> => {
  const lastArray = this.serializableData.length > 1
    ? [this.serializableData[this.serializableData.length - 1]]
    : this.serializableData;

  return {
    log: this.debugMessages,
    serializableOutput: lastArray.reduce((reducedObject, item, index) => {
      return {
        [`item-${index}`]: item,
        ...reducedObject,
      }
    }, {}),
    binaryOutput: this.binaryData.reduce((reducedObject, item, index) => {
      return {
        [`item-${index}`]: item,
        ...reducedObject,
      }
    }, {}),
    checkpoint: this.currentScrapingState,
    runId: this.currentRunId
  };
}

  /**
   * Returns true if an interpretation is currently running.
   * @returns {boolean}
   */
  public interpretationInProgress = () => {
    return this.interpreter !== null;
  };

  /**
   * Updates the socket used for communication with the client (frontend).
   * @param socket Socket.io socket instance enabling communication with the client (frontend) side.
   * @returns void
   */
  public updateSocket = (socket: Socket): void => {
    this.socket = socket;
    this.subscribeToPausing();
  };
}