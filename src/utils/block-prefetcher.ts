/**
 * Read-ahead cache for block reads.
 *
 * The processor hands blocks to the consumer strictly one nonce at a time, in order, which means
 * the gateway sits idle for the whole duration of the consumer callback and every nonce pays a
 * full round-trip. This keeps a bounded number of reads per shard in flight ahead of the nonce
 * being processed, so `take` almost always resolves from an already-settled promise.
 *
 * Delivery order is unchanged: the caller still asks for one explicit nonce at a time. Only the
 * reads move off the critical path.
 */
export class BlockPrefetcher<T> {
  private readonly inFlight: Map<number, Map<number, Promise<T | undefined>>> = new Map();

  constructor(
    private readonly fetchBlock: (shardId: number, nonce: number) => Promise<T | undefined>,
    private readonly onFetchError: (shardId: number, nonce: number, error: unknown) => void,
  ) { }

  /**
   * Starts reads for [fromNonce, toNonce] without waiting for them. Nonces already in flight are
   * left alone, so repeated calls across passes top the pipeline up instead of duplicating reads.
   */
  prime(shardId: number, fromNonce: number, toNonce: number): void {
    const entries = this.getShardEntries(shardId);

    for (let nonce = fromNonce; nonce <= toNonce; nonce++) {
      if (entries.has(nonce)) {
        continue;
      }

      // The catch is attached synchronously, so a read-ahead failure can never surface as an
      // unhandled rejection and can never reject the caller's Promise.all. It resolves to
      // undefined instead - the value the processor already treats as 'block not available' -
      // which leaves the nonce uncommitted to be retried on a later pass.
      entries.set(nonce, this.fetchBlock(shardId, nonce).catch(error => {
        this.onFetchError(shardId, nonce, error);
        return undefined;
      }));
    }
  }

  /**
   * Returns the block for a single nonce, waiting for its read only if it has not settled yet.
   */
  async take(shardId: number, nonce: number): Promise<T | undefined> {
    const entries = this.getShardEntries(shardId);

    let pending = entries.get(nonce);
    if (!pending) {
      this.prime(shardId, nonce, nonce);
      pending = entries.get(nonce);
    }

    try {
      return await pending;
    } finally {
      // Consumed either way: a block that was missing or failed to read must be re-read on the
      // next attempt rather than served again from the cache.
      entries.delete(nonce);
    }
  }

  /**
   * Drops reads for nonces below `belowNonce`, i.e. blocks that were read ahead but then skipped
   * (a maxLookBehind jump).
   */
  prune(shardId: number, belowNonce: number): void {
    const entries = this.inFlight.get(shardId);
    if (!entries) {
      return;
    }

    for (const nonce of entries.keys()) {
      if (nonce < belowNonce) {
        entries.delete(nonce);
      }
    }
  }

  /**
   * Drops every read for a shard, for cases where the whole read-ahead window became meaningless
   * (a network reset restarting nonces from zero).
   */
  clearShard(shardId: number): void {
    this.inFlight.delete(shardId);
  }

  get pendingCount(): number {
    let total = 0;
    for (const entries of this.inFlight.values()) {
      total += entries.size;
    }

    return total;
  }

  private getShardEntries(shardId: number): Map<number, Promise<T | undefined>> {
    let entries = this.inFlight.get(shardId);
    if (!entries) {
      entries = new Map();
      this.inFlight.set(shardId, entries);
    }

    return entries;
  }
}
