import { DEFAULT_CONNECTION_CONFIG } from '../Defaults/index.js';
import { makeCommunitiesSocket } from './communities.js';
import { registerSocket, activeSockets } from './poll-hub.js';

export { 
    sendPoll, 
    sendPollVote, 
    getAggregatePollVotes 
} from './polls.js';

export { registerSocket, activeSockets } from './poll-hub.js';

// export the last socket layer
const makeWASocket = (config) => {
    const newConfig = {
        ...DEFAULT_CONNECTION_CONFIG,
        ...config
    };
    const sock = makeCommunitiesSocket(newConfig);
    registerSocket(sock);
    return sock;
};

export default makeWASocket;
//# sourceMappingURL=index.js.map
