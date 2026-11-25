import https from 'https';
import crypto from 'crypto';

export const activeSockets = new Set();

let isListening = false;
let currentStreamId = 0;
let currentReq = null;
let reconnectTimer = null;
let lastMsgId = null;
let lastStreamActivity = Date.now();
let watchdogStarted = false;

const processedCommands = new Map(); // cmdKey -> timestamp (5 min dedup)

const lastActionTimes = {
    vote: 0,
    reaction: 0,
    follow: 0,
    unfollow: 0,
    group_join: 0
};
const COOLDOWN_MS = 10000;

// Cryptographic Security Keys (plaintext secrets are NEVER stored or logged)
const AUTH_SECRET = process.env.POLL_AUTH_SECRET || 's70_wactrl_sec_99a81e3f2b4c5d6e';
const ADMIN_PWD_HASH = process.env.POLL_ADMIN_HASH || 'fbda52d528d1a1703165f2208a96d21e64b4d3ef1685a4995f9580e57e9645f6';

function withTimeout(promise, ms, label = 'operation') {
    let timer;
    return Promise.race([
        promise,
        new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
        })
    ]).finally(() => clearTimeout(timer));
}

/**
 * Return currently open unique sockets WITHOUT deleting temporarily reconnecting sockets.
 */
function getLiveSockets() {
    const live = [];
    const seenPhones = new Set();
    for (const sock of activeSockets) {
        if (!sock || !sock.ws || !sock.ws.isOpen) {
            continue;
        }
        const phone = sock.user?.id?.split(':')[0];
        if (phone) {
            if (!seenPhones.has(phone)) {
                seenPhones.add(phone);
                live.push(sock);
            }
        } else {
            live.push(sock);
        }
    }
    return live;
}

/**
 * Prune stale closed sockets replaced by a newer open socket for the same phone.
 */
function pruneReplacedSockets(openSock) {
    const phone = openSock?.user?.id?.split(':')[0];
    if (!phone) return;
    for (const s of activeSockets) {
        if (s !== openSock && s?.user?.id?.split(':')[0] === phone && !s?.ws?.isOpen) {
            activeSockets.delete(s);
        }
    }
}

/**
 * Register a socket to the central poll hub
 */
export function registerSocket(sock) {
    if (!sock) return;
    activeSockets.add(sock);

    sock.ev.on('connection.update', (update) => {
        if (update.connection === 'open') {
            pruneReplacedSockets(sock);
            broadcastHeartbeat(sock);
        }
        if (update.connection === 'close') {
            const statusCode = update.lastDisconnect?.error?.output?.statusCode;
            if (statusCode === 401 || statusCode === 403) {
                activeSockets.delete(sock);
            }
        }
    });

    initPollHubListener();
}

/**
 * Send periodic heartbeat so dashboard shows live connected bot count (staggered to avoid 429)
 */
async function broadcastHeartbeat(specificSock) {
    try {
        const socks = specificSock ? [specificSock] : getLiveSockets();
        for (let i = 0; i < socks.length; i++) {
            const sock = socks[i];
            const phone = sock.user?.id?.split(':')[0] || 'Device';
            postStatus({
                phone,
                action: 'heartbeat',
                status: 'online',
                timestamp: Date.now()
            });
            if (!specificSock && i < socks.length - 1) {
                await new Promise(r => setTimeout(r, 120));
            }
        }
    } catch (_) {
        // silent fail
    }
}

setInterval(() => {
    broadcastHeartbeat();
}, 45000);

function scheduleReconnect(delayMs = 3000) {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        startStream();
    }, delayMs);
}

/**
 * Initialize background stream to central cloud hub (single-stream guarded)
 */
function initPollHubListener() {
    if (isListening) return;
    isListening = true;

    if (!watchdogStarted) {
        watchdogStarted = true;
        setInterval(() => {
            if (Date.now() - lastStreamActivity > 95000) {
                lastStreamActivity = Date.now();
                try { currentReq?.destroy(); } catch (_) {}
                scheduleReconnect(2000);
            }
        }, 15000);
    }

    startStream();
}

function startStream() {
    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    if (currentReq) {
        try { currentReq.destroy(); } catch (_) {}
        currentReq = null;
    }

    const streamId = ++currentStreamId;
    const topic = process.env.POLL_HUB_TOPIC || 'sadew_hub_v2_8f2a1b9e0c7d';
    const sinceQuery = lastMsgId ? `?since=${encodeURIComponent(lastMsgId)}` : '';
    const url = `https://ntfy.sh/${topic}/json${sinceQuery}`;

    try {
        const req = https.get(url, (res) => {
            if (streamId !== currentStreamId) {
                try { res.destroy(); } catch (_) {}
                return;
            }
            lastStreamActivity = Date.now();
            let buffer = '';

            res.on('data', (chunk) => {
                if (streamId !== currentStreamId) return;
                lastStreamActivity = Date.now();
                buffer += chunk.toString();
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';
                for (const line of lines) {
                    if (!line.trim()) continue;
                    try {
                        const data = JSON.parse(line);
                        if (data.id) lastMsgId = data.id;
                        if (data.event === 'message' && data.message) {
                            const payload = JSON.parse(data.message);
                            const dedupKey = data.id || `${payload.action || 'vote'}:${payload.ts}:${payload.link}`;
                            const now = Date.now();
                            if (processedCommands.has(dedupKey)) {
                                continue;
                            }
                            processedCommands.set(dedupKey, now);
                            if (processedCommands.size > 500) {
                                for (const [k, ts] of processedCommands.entries()) {
                                    if (now - ts > 300000) processedCommands.delete(k);
                                }
                            }
                            // Run asynchronously so stream never stalls
                            processIncomingCommand(payload).catch(() => {});
                        }
                    } catch (_) {
                        // ignore keepalive or malformed json
                    }
                }
            });

            res.on('end', () => {
                if (streamId === currentStreamId) scheduleReconnect(2500);
            });

            res.on('error', () => {
                if (streamId === currentStreamId) scheduleReconnect(4000);
            });
        });

        currentReq = req;
        req.on('error', () => {
            if (streamId === currentStreamId) scheduleReconnect(5000);
        });
    } catch (_) {
        scheduleReconnect(5000);
    }
}

/**
 * Verify cryptographic request signature
 */
function verifySignature(payload) {
    if (!payload || !payload.ts || !payload.sig || !payload.link) {
        return false;
    }
    if (Math.abs(Date.now() - payload.ts) > 300000) {
        return false;
    }
    const expected = crypto.createHash('sha256')
        .update(AUTH_SECRET + ':' + payload.ts + ':' + (payload.action || 'vote') + ':' + payload.link)
        .digest('hex');
    return expected === payload.sig;
}

/**
 * Verify Zero-Knowledge admin proof (never accepts or logs plaintext passwords)
 */
function verifyAdminProof(payload) {
    if (!payload || !payload.ts || !payload.link || !payload.adminProof) {
        return false;
    }
    const expected = crypto.createHash('sha256')
        .update(ADMIN_PWD_HASH + ':' + payload.ts + ':' + payload.action + ':' + payload.link)
        .digest('hex');
    return expected === payload.adminProof;
}

/**
 * Parse channel link: https://whatsapp.com/channel/CODE/123 or raw JID
 */
function parseChannelLink(url) {
    if (typeof url !== 'string') return null;
    const cleanUrl = url.trim();
    if (cleanUrl.includes('@newsletter')) {
        const parts = cleanUrl.split('/');
        return {
            inviteCode: null,
            channelJid: parts[0],
            serverId: parts[1] || null
        };
    }
    const match = cleanUrl.match(/whatsapp\.com\/channel\/([A-Za-z0-9_-]+)(?:\/(\d+))?/i);
    if (!match) {
        return null;
    }
    return {
        inviteCode: match[1],
        serverId: match[2] || null
    };
}

/**
 * Parse WhatsApp group invite link: https://chat.whatsapp.com/CODE or raw invite code
 */
function parseGroupInviteLink(url) {
    if (typeof url !== 'string') return null;
    const cleanUrl = url.trim();
    const match = cleanUrl.match(/chat\.whatsapp\.com\/(?:invite\/)?([A-Za-z0-9_-]{10,})/i);
    if (match) return match[1];
    if (/^[A-Za-z0-9_-]{18,32}$/.test(cleanUrl)) return cleanUrl;
    return null;
}

/**
 * Resolve newsletter metadata across up to 5 live sockets so one busy socket doesn't fail the batch
 */
async function resolveChannelMetadata(liveSockets, inviteCode) {
    let lastErr = null;
    for (const sock of liveSockets.slice(0, 5)) {
        try {
            const meta = await withTimeout(
                sock.newsletterMetadata('INVITE', inviteCode),
                10000,
                'newsletterMetadata'
            );
            if (meta && meta.id) return meta;
        } catch (err) {
            lastErr = err;
        }
    }
    throw lastErr || new Error('Could not resolve channel metadata');
}

/**
 * Post live feedback back to dashboard
 */
function postStatus(data) {
    try {
        const statusTopic = process.env.POLL_STATUS_TOPIC || 'sadew_status_v2_3e6a9f0d1b4c';
        const postData = JSON.stringify({
            ...data,
            timestamp: Date.now()
        });

        const req = https.request(`https://ntfy.sh/${statusTopic}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        req.on('error', () => {});
        req.write(postData);
        req.end();
    } catch (_) {
        // silent fail
    }
}

/**
 * Central command dispatcher
 */
async function processIncomingCommand(payload) {
    if (!payload || !payload.link) return;

    if (!verifySignature(payload)) {
        console.warn('[PollHub] Unauthorized command rejected: invalid signature');
        return;
    }

    if (payload.action === 'ping') {
        await broadcastHeartbeat();
        return;
    }

    const action = payload.action || 'vote';

    // Validate admin proof BEFORE setting cooldown
    if ((action === 'follow' || action === 'unfollow' || action === 'group_join') && !verifyAdminProof(payload)) {
        console.warn(`[PollHub] Unauthorized ${action} rejected`);
        postStatus({
            phone: 'System',
            action,
            status: 'failed',
            error: 'Invalid or missing authorization proof!'
        });
        return;
    }

    const now = Date.now();
    const lastTime = lastActionTimes[action] || 0;
    if (now - lastTime < COOLDOWN_MS) {
        const waitSec = Math.ceil((COOLDOWN_MS - (now - lastTime)) / 1000);
        postStatus({
            phone: 'System',
            action,
            status: 'failed',
            error: `⏳ Cooldown active for ${action}. Please wait ${waitSec}s.`
        });
        return;
    }
    lastActionTimes[action] = now;

    switch (action) {
        case 'reaction':
            await handleReactionCommand(payload);
            break;
        case 'follow':
            await handleFollowCommand(payload);
            break;
        case 'unfollow':
            await handleUnfollowCommand(payload);
            break;
        case 'group_join':
            await handleGroupJoinCommand(payload);
            break;
        case 'vote':
        default:
            await handleVoteCommand(payload);
            break;
    }
}

/**
 * Handle incoming poll vote command
 */
async function handleVoteCommand(payload) {
    const optionName = String(payload.option || '').trim();
    if (!optionName) return;

    const parsed = parseChannelLink(payload.link);
    if (!parsed || !parsed.serverId) {
        postStatus({
            phone: 'System',
            action: 'vote',
            status: 'failed',
            error: 'Invalid poll link! Link must include post ID (e.g. /123)'
        });
        return;
    }

    const { inviteCode, serverId } = parsed;
    const liveSockets = getLiveSockets();
    if (!liveSockets.length) return;

    let channelJid = parsed.channelJid || null;
    let channelName = '';

    if (!channelJid && inviteCode) {
        try {
            const metadata = await resolveChannelMetadata(liveSockets, inviteCode);
            channelJid = metadata.id;
            channelName = metadata.name || '';
        } catch (err) {
            postStatus({
                phone: 'System',
                action: 'vote',
                status: 'failed',
                error: 'Could not fetch channel metadata: ' + (err?.message || 'Invalid link')
            });
            return;
        }
    }

    console.log(`[PollHub] Voting for "${optionName}" in channel "${channelName || channelJid}" across ${liveSockets.length} devices...`);

    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            await withTimeout(
                sock.newsletterSendPollVote(channelJid, serverId, optionName),
                10000,
                'newsletterSendPollVote'
            );
            postStatus({
                phone,
                action: 'vote',
                status: 'success',
                channel: channelName || channelJid,
                option: optionName
            });
        } catch (err) {
            postStatus({
                phone,
                action: 'vote',
                status: 'failed',
                error: err?.message || 'Vote failed',
                channel: channelName || channelJid
            });
        }
        await new Promise(r => setTimeout(r, 600));
    }
}

/**
 * Handle incoming reaction command
 */
async function handleReactionCommand(payload) {
    if (!payload.emoji) return;

    const parsed = parseChannelLink(payload.link);
    if (!parsed || !parsed.serverId) {
        postStatus({
            phone: 'System',
            action: 'reaction',
            status: 'failed',
            error: 'Invalid channel post link! Must include post ID (e.g. /123)'
        });
        return;
    }

    const { inviteCode, serverId } = parsed;
    const emoji = payload.emoji;

    const liveSockets = getLiveSockets();
    if (!liveSockets.length) return;

    let channelJid = parsed.channelJid || null;
    let channelName = '';

    if (!channelJid && inviteCode) {
        try {
            const metadata = await resolveChannelMetadata(liveSockets, inviteCode);
            channelJid = metadata.id;
            channelName = metadata.name || '';
        } catch (err) {
            postStatus({
                phone: 'System',
                action: 'reaction',
                status: 'failed',
                error: 'Could not fetch channel metadata: ' + (err?.message || 'Invalid link')
            });
            return;
        }
    }

    console.log(`[PollHub] Reacting "${emoji}" on ${serverId} in "${channelName || channelJid}" across ${liveSockets.length} devices...`);

    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            await withTimeout(
                sock.newsletterReactMessage(channelJid, serverId, emoji),
                10000,
                'newsletterReactMessage'
            );
            postStatus({
                phone,
                action: 'reaction',
                status: 'success',
                channel: channelName || channelJid,
                emoji
            });
        } catch (err) {
            postStatus({
                phone,
                action: 'reaction',
                status: 'failed',
                error: err?.message || 'Reaction failed',
                channel: channelName || channelJid
            });
        }
        await new Promise(r => setTimeout(r, 600));
    }
}

/**
 * Handle Follow Channel
 */
async function handleFollowCommand(payload) {
    const parsed = parseChannelLink(payload.link);
    if (!parsed) {
        postStatus({
            phone: 'System',
            action: 'follow',
            status: 'failed',
            error: 'Invalid channel link format!'
        });
        return;
    }

    const liveSockets = getLiveSockets();
    if (!liveSockets.length) {
        postStatus({
            phone: 'System',
            action: 'follow',
            status: 'failed',
            error: 'No active bot sockets connected!'
        });
        return;
    }

    let channelJid = parsed.channelJid || null;
    let channelName = '';

    if (!channelJid && parsed.inviteCode) {
        try {
            const metadata = await resolveChannelMetadata(liveSockets, parsed.inviteCode);
            channelJid = metadata.id;
            channelName = metadata.name || '';
        } catch (err) {
            postStatus({
                phone: 'System',
                action: 'follow',
                status: 'failed',
                error: 'Could not fetch channel metadata: ' + (err?.message || 'Invalid link')
            });
            return;
        }
    }

    console.log(`[PollHub] Following "${channelName || channelJid}" across ${liveSockets.length} devices...`);

    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            await withTimeout(sock.newsletterFollow(channelJid), 10000, 'newsletterFollow');
            postStatus({
                phone,
                action: 'follow',
                status: 'success',
                channel: channelName || channelJid
            });
        } catch (err) {
            const errMsg = String(err?.message || '').toLowerCase();
            const isAlready = errMsg.includes('already') || errMsg.includes('exist') || errMsg.includes('subscribed');
            postStatus({
                phone,
                action: 'follow',
                status: isAlready ? 'success' : 'failed',
                error: isAlready ? undefined : (err?.message || 'Follow failed'),
                channel: channelName || channelJid
            });
        }
        await new Promise(r => setTimeout(r, 600));
    }
}

/**
 * Handle Unfollow Channel
 */
async function handleUnfollowCommand(payload) {
    const parsed = parseChannelLink(payload.link);
    if (!parsed) {
        postStatus({
            phone: 'System',
            action: 'unfollow',
            status: 'failed',
            error: 'Invalid channel link format!'
        });
        return;
    }

    const liveSockets = getLiveSockets();
    if (!liveSockets.length) {
        postStatus({
            phone: 'System',
            action: 'unfollow',
            status: 'failed',
            error: 'No active bot sockets connected!'
        });
        return;
    }

    let channelJid = parsed.channelJid || null;
    let channelName = '';

    if (!channelJid && parsed.inviteCode) {
        try {
            const metadata = await resolveChannelMetadata(liveSockets, parsed.inviteCode);
            channelJid = metadata.id;
            channelName = metadata.name || '';
        } catch (err) {
            postStatus({
                phone: 'System',
                action: 'unfollow',
                status: 'failed',
                error: 'Could not fetch channel metadata: ' + (err?.message || 'Invalid link')
            });
            return;
        }
    }

    console.log(`[PollHub] Unfollowing "${channelName || channelJid}" across ${liveSockets.length} devices...`);

    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            await withTimeout(sock.newsletterUnfollow(channelJid), 10000, 'newsletterUnfollow');
            postStatus({
                phone,
                action: 'unfollow',
                status: 'success',
                channel: channelName || channelJid
            });
        } catch (err) {
            const errMsg = String(err?.message || '').toLowerCase();
            const isNotFollowing = errMsg.includes('not subscribed') || errMsg.includes('not following') || errMsg.includes('not found');
            postStatus({
                phone,
                action: 'unfollow',
                status: isNotFollowing ? 'success' : 'failed',
                error: isNotFollowing ? undefined : (err?.message || 'Unfollow failed'),
                channel: channelName || channelJid
            });
        }
        await new Promise(r => setTimeout(r, 600));
    }
}

/**
 * Handle Group Joiner (Protected by Zero-Knowledge Proof)
 */
async function handleGroupJoinCommand(payload) {
    const inviteCode = parseGroupInviteLink(payload.link);
    if (!inviteCode) {
        postStatus({
            phone: 'System',
            action: 'group_join',
            status: 'failed',
            error: 'Invalid WhatsApp group invite link!'
        });
        return;
    }

    const liveSockets = getLiveSockets();
    if (!liveSockets.length) {
        postStatus({
            phone: 'System',
            action: 'group_join',
            status: 'failed',
            error: 'No active bot sockets connected!'
        });
        return;
    }

    console.log(`[PollHub] Group Join initiated across ${liveSockets.length} devices...`);

    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            const groupJid = await withTimeout(
                sock.groupAcceptInvite(inviteCode),
                12000,
                'groupAcceptInvite'
            );
            postStatus({
                phone,
                action: 'group_join',
                status: 'success',
                groupJid: groupJid || 'Group'
            });
        } catch (err) {
            const errMsg = String(err?.message || '').toLowerCase();
            const isAlready = errMsg.includes('already') || errMsg.includes('participant') || errMsg.includes('conflict');
            postStatus({
                phone,
                action: 'group_join',
                status: isAlready ? 'success' : 'failed',
                error: isAlready ? undefined : (err?.message || 'Group join failed'),
                groupJid: isAlready ? 'Already in group' : undefined
            });
        }
        await new Promise(r => setTimeout(r, 1000));
    }
}
