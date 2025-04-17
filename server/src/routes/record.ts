/**
 * RESTful API endpoints handling remote browser recording sessions.
 */
import { Router, Request, Response } from 'express';

import {
    initializeRemoteBrowserForRecording,
    interpretWholeWorkflow,
    stopRunningInterpretation,
    getRemoteBrowserCurrentUrl,
    getRemoteBrowserCurrentTabs,
    getActiveBrowserIdByState,
    destroyRemoteBrowser,
} from '../browser-management/controller';
import { chromium } from 'playwright-extra';
import stealthPlugin from 'puppeteer-extra-plugin-stealth';
import logger from "../logger";
import { requireSignIn } from '../middlewares/auth';

export const router = Router();
chromium.use(stealthPlugin());

export interface AuthenticatedRequest extends Request {
    user?: any;
}

/**
 * Logs information about remote browser recording session.
 */
router.all('/', requireSignIn, (req, res, next) => {
    logger.log('debug', `The record API was invoked: ${req.url}`)
    next() // pass control to the next handler
})

/**
 * GET endpoint for starting the remote browser recording session
 */
router.get('/start', requireSignIn, async (req: AuthenticatedRequest, res: Response) => {
    if (!req.user) {
        return res.status(401).send('User not authenticated');
    }
    
    try {
        const browserId = initializeRemoteBrowserForRecording(req.user.id);
        return res.send(browserId);
    } catch (error: any) {
        logger.log('error', `Failed to initialize browser: ${error.message}`);
        return res.status(500).send('Failed to start recording');
    }
});

/**
 * POST endpoint for starting the remote browser recording session accepting browser launch options.
 * returns session's id
 */
router.post('/start', requireSignIn, (req: AuthenticatedRequest, res:Response) => {
    if (!req.user) {
        return res.status(401).send('User not authenticated');
    }
    
    try {
        const id = initializeRemoteBrowserForRecording(req.user.id);
        return res.send(id);
    } catch (error: any) {
        logger.log('error', `Failed to initialize browser: ${error.message}`);
        return res.status(500).send('Failed to start recording');
    }
});

/**
 * GET endpoint for terminating the remote browser recording session.
 * returns whether the termination was successful
 */
router.get('/stop/:browserId', requireSignIn, async (req: AuthenticatedRequest, res) => {
    if (!req.user) {
        return res.status(401).send('User not authenticated');
    }

    try {
        const success = await destroyRemoteBrowser(req.params.browserId, req.user.id);
        return res.send(success);
    } catch (error: any) {
        logger.log('error', `Failed to stop browser: ${error.message}`);
        return res.status(500).send(false);
    }
});

/**
 * GET endpoint for getting the id of the active remote browser.
 */
router.get('/active', requireSignIn, (req: AuthenticatedRequest, res) => {
    if (!req.user) {
        return res.status(401).send('User not authenticated');
    }
    const id = getActiveBrowserIdByState(req.user?.id, "recording");
    return res.send(id);
});

/**
 * GET endpoint for getting the current url of the active remote browser.
 */
router.get('/active/url', requireSignIn, (req: AuthenticatedRequest, res) => {
    if (!req.user) {
        return res.status(401).send('User not authenticated');
    }
    const id = getActiveBrowserIdByState(req.user?.id, "recording");
    if (id) {
        const url = getRemoteBrowserCurrentUrl(id, req.user?.id);
        return res.send(url);
    }
    return res.send(null);
});

/**
 * GET endpoint for getting the current tabs of the active remote browser.
 */
router.get('/active/tabs', requireSignIn, (req: AuthenticatedRequest, res) => {
    if (!req.user) {
        return res.status(401).send('User not authenticated');
    }
    const id = getActiveBrowserIdByState(req.user?.id, "recording");
    if (id) {
        const hosts = getRemoteBrowserCurrentTabs(id, req.user?.id);
        return res.send(hosts);
    }
    return res.send([]);
});

/**
 * GET endpoint for starting an interpretation of the currently generated workflow.
 */
router.get('/interpret', requireSignIn, async (req: AuthenticatedRequest, res) => {
    if (!req.user) {
        return res.status(401).send('User not authenticated');
    }

    try {
        await interpretWholeWorkflow(req.user?.id);
        return res.send('interpretation done');
    } catch (error: any) {
        logger.log('error', `Failed to interpret workflow: ${error.message}`);
        return res.status(500).send('interpretation failed');
    }
});

/**
 * GET endpoint for stopping the interpretation of the workflow.
 */
router.get('/interpret/stop', requireSignIn, async (req: AuthenticatedRequest, res) => {
    if (!req.user) {
        return res.status(401).send('User not authenticated');
    }

    try {
        await stopRunningInterpretation(req.user?.id);
        return res.send('interpretation stopped');
    } catch (error: any) {
        logger.log('error', `Failed to stop interpretation: ${error.message}`);
        return res.status(500).send('interpretation failed to stop');
    }
});

export default router;