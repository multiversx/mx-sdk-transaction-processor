export const NETWORK_RESET_NONCE_THRESHOLD = 10000;
export const METACHAIN = 4294967295;
export const DEFAULT_GATEWAY_URL = 'https://gateway.multiversx.com';
export const DEFAULT_TIMEOUT = 5000;

// Upper bound on concurrent sockets kept open against the gateway. Sized above the largest burst
// read-ahead can produce (shard count x maxPrefetch) so the pipeline is never socket-starved.
export const MAX_SOCKETS_PER_HOST = 64;

// Blocks read ahead per shard, on top of the block currently being processed.
export const DEFAULT_MAX_PREFETCH = 10;
