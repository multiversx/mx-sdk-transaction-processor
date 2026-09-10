import { GatewayBlockResponse } from './types/gateway/block-response';
import { GatewayMiniblockProcessingType } from './types/gateway/miniblock-processing-type.enum';
import { DEFAULT_MAX_PREFETCH, METACHAIN, NETWORK_RESET_NONCE_THRESHOLD } from './utils/constants';
import { TransactionProcessorMode } from './types/transaction-processor-mode.enum';
import { LogTopic } from './types/log-topic';
import { TransactionStatistics } from './types/transaction-statistics';
import { CrossShardTransaction } from './types/cross-shard-transaction';
import { TransactionProcessorOptions } from './types/options';
import { base64Decode } from './utils/decoders';
import { selectMany } from './utils/utils';
import { ShardTransaction } from './types/shard-transaction';
import { GatewayMiniblock } from './types/gateway/miniblock';
import { GatewayTransaction } from './types/gateway/transaction';
import { ShardsMaintainerService } from './shards-maintainer.service';
import { HttpService } from './utils/http.service';
import { BlockPrefetcher } from './utils/block-prefetcher';

type BlockTransactions = { blockHash: string, transactions: ShardTransaction[] };

export class TransactionProcessor {
  private startDate: Date = new Date();
  private shardIds: number[] = [];
  private options: TransactionProcessorOptions = new TransactionProcessorOptions();
  private readonly lastProcessedNoncesInternal: { [key: number]: number } = {};
  private isRunning: boolean = false;
  private crossShardDictionary: { [key: string]: CrossShardTransaction } = {};
  private httpService: HttpService | undefined;
  private readonly shardsMaintainerService: ShardsMaintainerService = new ShardsMaintainerService();

  // Kept on the instance rather than per pass: start() is typically re-entered by a sub-second
  // cron, so blocks read ahead near the end of one pass are still warm at the start of the next.
  private readonly shardBlockPrefetcher = new BlockPrefetcher<BlockTransactions>(
    (shardId, nonce) => this.getShardTransactions(shardId, nonce),
    (shardId, nonce, error) => this.logMessage(LogTopic.Debug, `Could not read block for shardId ${shardId} and nonce ${nonce}: ${error}`),
  );

  private readonly hyperblockPrefetcher = new BlockPrefetcher<BlockTransactions>(
    (_shardId, nonce) => this.getHyperblockTransactions(nonce),
    (_shardId, nonce, error) => this.logMessage(LogTopic.Debug, `Could not read hyperblock for nonce ${nonce}: ${error}`),
  );

  async start(options: TransactionProcessorOptions): Promise<void> {
    this.options = options;
    this.httpService = new HttpService(this.options.gatewayUrl, this.options.timeout);

    switch (options.mode) {
      case TransactionProcessorMode.Hyperblock:
        await this.startProcessByHyperblock(options);
        break;
      default:
        await this.startProcessByShardblock(options);
    }
  }

  async startProcessByShardblock(options: TransactionProcessorOptions) {
    if (this.isRunning) {
      this.logMessage(LogTopic.Debug, 'Transaction processor is already running');
      return;
    }

    this.isRunning = true;

    const crossShardHashes = Object.keys(this.crossShardDictionary);
    for (const crossShardHash of crossShardHashes) {
      const crossShardItem = this.crossShardDictionary[crossShardHash];
      const elapsedSeconds = (new Date().getTime() - crossShardItem.created.getTime()) / 1000;
      if (elapsedSeconds > 600) {
        this.logMessage(LogTopic.CrossShardSmartContractResult, `Pruning transaction with hash ${crossShardHash} since its elapsed time is ${elapsedSeconds} seconds`);
        delete this.crossShardDictionary[crossShardHash];
      }
    }

    try {
      this.startDate = new Date();
      this.shardIds = await this.getShards();

      this.logMessage(LogTopic.Debug, `shardIds: ${this.shardIds}`);

      const startLastProcessedNonces: { [key: number]: number } = {};

      let reachedTip: boolean;

      const currentNonces = await this.getCurrentNonces();

      do {
        reachedTip = true;

        for (const shardId of this.shardIds) {
          const currentNonce = currentNonces[shardId];
          let lastProcessedNonce = await this.getLastProcessedNonceOrCurrent(shardId, currentNonce);

          this.logMessage(LogTopic.Debug, `shardId: ${shardId}, currentNonce: ${currentNonce}, lastProcessedNonce: ${lastProcessedNonce}`);

          if (lastProcessedNonce === currentNonce) {
            this.logMessage(LogTopic.Debug, 'lastProcessedNonce === currentNonce');
            continue;
          }

          // this is to handle the situation where the current nonce is reset
          // (e.g. devnet/testnet reset where the nonces start again from zero)
          if (lastProcessedNonce > currentNonce + NETWORK_RESET_NONCE_THRESHOLD) {
            this.logMessage(LogTopic.Debug, `Detected network reset. Setting last processed nonce to ${currentNonce} for shard ${shardId}`);
            lastProcessedNonce = currentNonce;
            // Everything read ahead belongs to the pre-reset chain and can never be delivered.
            this.shardBlockPrefetcher.clearShard(shardId);
          }

          if (lastProcessedNonce > currentNonce) {
            this.logMessage(LogTopic.Debug, 'lastProcessedNonce > currentNonce');
            continue;
          }

          if (options.maxLookBehind && currentNonce - lastProcessedNonce > options.maxLookBehind) {
            lastProcessedNonce = currentNonce - options.maxLookBehind;
          }

          if (!startLastProcessedNonces[shardId]) {
            startLastProcessedNonces[shardId] = lastProcessedNonce;
          }

          const nonce = lastProcessedNonce + 1;

          // Top the read-ahead window up before waiting on this nonce, so that while this block is
          // being handed to the consumer the reads for the following nonces are already in flight.
          // Only nonces up to the tip observed at the start of the pass are ever requested.
          this.shardBlockPrefetcher.prune(shardId, nonce);
          this.shardBlockPrefetcher.prime(shardId, nonce, nonce + this.getPrefetchSize(currentNonce - lastProcessedNonce) - 1);

          const transactionsResult = await this.shardBlockPrefetcher.take(shardId, nonce);
          if (transactionsResult === undefined) {
            this.logMessage(LogTopic.Debug, 'transactionsResult === undefined');
            continue;
          }

          const blockHash = transactionsResult.blockHash;
          const transactions = transactionsResult.transactions;

          reachedTip = false;

          const validTransactions = [];
          const crossShardTransactions = [];

          if (this.options.waitForFinalizedCrossShardSmartContractResults === true) {
            const crossShardTransactions = this.getFinalizedCrossShardScrTransactions(shardId, transactions);

            for (const crossShardTransaction of crossShardTransactions) {
              validTransactions.push(crossShardTransaction);
            }
          }

          for (const transaction of transactions) {
            // we only care about transactions that are finalized in the given shard
            if (transaction.destinationShard !== shardId && !options.includeCrossShardStartedTransactions) {
              this.logMessage(LogTopic.Debug, `transaction with hash '${transaction.hash}' not on destination shard. Skipping`);
              continue;
            }

            // we skip transactions that are cross shard and still pending for smart-contract results
            if (this.crossShardDictionary[transaction.hash]) {
              this.logMessage(LogTopic.Debug, `transaction with hash '${transaction.hash}' is still awaiting cross shard SCRs. Skipping`);
              crossShardTransactions.push(transaction);
              continue;
            }

            validTransactions.push(transaction);
          }

          if (validTransactions.length > 0 || options.notifyEmptyBlocks === true) {
            this.logMessage(LogTopic.CrossShardSmartContractResult, `crossShardTransactionsCounterDictionary items: ${Object.keys(this.crossShardDictionary).length}`);

            const statistics = new TransactionStatistics();

            statistics.secondsElapsed = (new Date().getTime() - this.startDate.getTime()) / 1000;
            statistics.processedNonces = lastProcessedNonce - startLastProcessedNonces[shardId];
            statistics.noncesPerSecond = statistics.processedNonces / statistics.secondsElapsed;
            statistics.noncesLeft = currentNonce - lastProcessedNonce;
            statistics.secondsLeft = statistics.noncesLeft / statistics.noncesPerSecond * 1.1;
            this.logMessage(LogTopic.Debug, `For shardId ${shardId} and nonce ${nonce}, notifying transactions with hashes ${validTransactions.map(x => x.hash)}`);

            await this.onTransactionsReceived(shardId, nonce, validTransactions, statistics, blockHash);
          }

          if (crossShardTransactions.length > 0) {
            await this.onTransactionsPending(shardId, nonce, crossShardTransactions);
          }

          this.logMessage(LogTopic.Debug, `Setting last processed nonce for shardId ${shardId} to ${nonce}`);
          await this.setLastProcessedNonce(shardId, nonce);
        }
      } while (!reachedTip);
    } finally {
      this.isRunning = false;
    }
  }

  async startProcessByHyperblock(options: TransactionProcessorOptions) {
    if (this.isRunning) {
      this.logMessage(LogTopic.Debug, 'Transaction processor is already running');
      return;
    }

    this.isRunning = true;


    try {
      this.shardIds = [METACHAIN];
      this.startDate = new Date();

      let startLastProcessedNonce = 0;

      let reachedTip: boolean;

      const currentNonces = await this.getCurrentNonces();
      const currentNonce = currentNonces[METACHAIN];

      do {
        reachedTip = true;

        let lastProcessedNonce = await this.getLastProcessedNonceOrCurrent(METACHAIN, currentNonce);

        this.logMessage(LogTopic.Debug, `currentNonce: ${currentNonce}, lastProcessedNonce: ${lastProcessedNonce}`);

        if (lastProcessedNonce === currentNonce) {
          continue;
        }

        // this is to handle the situation where the current nonce is reset
        // (e.g. devnet/testnet reset where the nonces start again from zero)
        if (lastProcessedNonce > currentNonce + NETWORK_RESET_NONCE_THRESHOLD) {
          this.logMessage(LogTopic.Debug, `Detected network reset. Setting last processed nonce to ${currentNonce}`);
          lastProcessedNonce = currentNonce;
          this.hyperblockPrefetcher.clearShard(METACHAIN);
        }

        if (lastProcessedNonce > currentNonce) {
          this.logMessage(LogTopic.Debug, 'lastProcessedNonce > currentNonce');
          continue;
        }

        if (options.maxLookBehind && currentNonce - lastProcessedNonce > options.maxLookBehind) {
          lastProcessedNonce = currentNonce - options.maxLookBehind;
        }

        if (!startLastProcessedNonce) {
          startLastProcessedNonce = lastProcessedNonce;
        }

        const nonce = lastProcessedNonce + 1;

        this.hyperblockPrefetcher.prune(METACHAIN, nonce);
        this.hyperblockPrefetcher.prime(METACHAIN, nonce, nonce + this.getPrefetchSize(currentNonce - lastProcessedNonce) - 1);

        const transactionsResult = await this.hyperblockPrefetcher.take(METACHAIN, nonce);
        if (transactionsResult === undefined) {
          this.logMessage(LogTopic.Debug, 'transactionsResult === undefined');
          continue;
        }

        const { blockHash, transactions } = transactionsResult;

        reachedTip = false;

        const transactionsByShard = new Map<number, ShardTransaction[]>();
        for (const transaction of transactions) {
          const shardId = transaction.destinationShard;
          const shardTransactions = transactionsByShard.get(shardId) ?? [];
          shardTransactions.push(transaction);
          transactionsByShard.set(shardId, shardTransactions);
        }

        for (const shardId of transactionsByShard.keys()) {
          const transactions = transactionsByShard.get(shardId) ?? [];
          if (transactions.length > 0 || options.notifyEmptyBlocks === true) {
            const statistics = new TransactionStatistics();

            statistics.secondsElapsed = (new Date().getTime() - this.startDate.getTime()) / 1000;
            statistics.processedNonces = lastProcessedNonce - startLastProcessedNonce;
            statistics.noncesPerSecond = statistics.processedNonces / statistics.secondsElapsed;
            statistics.noncesLeft = currentNonce - lastProcessedNonce;
            statistics.secondsLeft = statistics.noncesLeft / statistics.noncesPerSecond * 1.1;

            this.logMessage(LogTopic.Debug, `For shardId ${shardId} and nonce ${nonce}, notifying transactions with hashes ${transactions.map(x => x.hash)}`);
            await this.onTransactionsReceived(shardId, nonce, transactions, statistics, blockHash);
          }
        }

        this.logMessage(LogTopic.Debug, `Setting last processed nonce to ${nonce}`);
        await this.setLastProcessedNonce(METACHAIN, nonce);
      } while (!reachedTip);
    } finally {
      this.isRunning = false;
    }
  }

  private getFinalizedCrossShardScrTransactions(shardId: number, transactions: ShardTransaction[]): ShardTransaction[] {
    const crossShardTransactions: ShardTransaction[] = [];

    // pass 1: we add pending transactions in the dictionary from current shard to another one
    for (const transaction of transactions) {
      if (transaction.originalTransactionHash && transaction.sourceShard === shardId && transaction.destinationShard !== shardId) {
        let crossShardItem = this.crossShardDictionary[transaction.originalTransactionHash];
        if (!crossShardItem) {
          this.logMessage(LogTopic.CrossShardSmartContractResult, `Creating dictionary for original tx hash ${transaction.originalTransactionHash}`);
          const originalTransaction = transactions.find(x => x.hash === transaction.originalTransactionHash);
          if (originalTransaction) {
            crossShardItem = new CrossShardTransaction(originalTransaction);
            this.crossShardDictionary[transaction.originalTransactionHash] = crossShardItem;
          } else {
            this.logMessage(LogTopic.CrossShardSmartContractResult, `Could not identify transaction with hash ${transaction.originalTransactionHash} in transaction list`);
            continue;
          }
        }

        // if '@ok', ignore
        if (transaction.data) {
          const data = base64Decode(transaction.data);
          if (data === '@6f6b') {
            this.logMessage(LogTopic.CrossShardSmartContractResult, `Not incrementing counter for cross-shard SCR, original tx hash ${transaction.originalTransactionHash}, tx hash ${transaction.hash} since the data is @ok (${data})`);
            continue;
          }
        }

        crossShardItem.counter++;
        this.logMessage(LogTopic.CrossShardSmartContractResult, `Detected new cross-shard SCR for original tx hash ${transaction.originalTransactionHash}, tx hash ${transaction.hash}, counter = ${crossShardItem.counter}`);

        this.crossShardDictionary[transaction.originalTransactionHash] = crossShardItem;
      }
    }

    // pass 2: we delete pending transactions in the dictionary from another shard to current shard
    for (const transaction of transactions) {
      if (transaction.originalTransactionHash && transaction.sourceShard !== shardId && transaction.destinationShard === shardId) {
        const crossShardItem = this.crossShardDictionary[transaction.originalTransactionHash];
        if (!crossShardItem) {
          this.logMessage(LogTopic.CrossShardSmartContractResult, `No counter available for cross-shard SCR, original tx hash ${transaction.originalTransactionHash}, tx hash ${transaction.hash}`);
          continue;
        }

        // if '@ok', ignore
        if (transaction.data) {
          const data = base64Decode(transaction.data);
          if (data === '@6f6b') {
            this.logMessage(LogTopic.CrossShardSmartContractResult, `Not decrementing counter for cross-shard SCR, original tx hash ${transaction.originalTransactionHash}, tx hash ${transaction.hash} since the data is @ok (${data})`);
            continue;
          }
        }

        crossShardItem.counter--;
        this.logMessage(LogTopic.CrossShardSmartContractResult, `Finalized cross-shard SCR for original tx hash ${transaction.originalTransactionHash}, tx hash ${transaction.hash}, counter = ${crossShardItem.counter}`);

        this.crossShardDictionary[transaction.originalTransactionHash] = crossShardItem;
      }
    }

    // step 3. If the counter reached zero, we take the value out
    const crossShardDictionaryHashes = Object.keys(this.crossShardDictionary);
    for (const transactionHash of crossShardDictionaryHashes) {
      const crossShardItem = this.crossShardDictionary[transactionHash];
      if (crossShardItem.counter === 0) {
        this.logMessage(LogTopic.CrossShardSmartContractResult, `Completed cross-shard transaction for original tx hash ${transactionHash}`);
        // we only add it to the cross shard transactions if it isn't already in the list of completed transactions
        if (!transactions.some(transaction => transaction.hash === transactionHash)) {
          crossShardTransactions.push(crossShardItem.transaction);
        }

        delete this.crossShardDictionary[transactionHash];
      }
    }

    return crossShardTransactions;
  }

  /**
   * Size of the read-ahead window for a shard that is `noncesBehind` blocks behind the tip.
   * Never exceeds the distance to the tip: at the tip there is nothing to speculate on, and
   * reading past it would only ask the gateway for blocks that do not exist yet.
   */
  private getPrefetchSize(noncesBehind: number): number {
    const maxPrefetch = this.options.maxPrefetch ?? DEFAULT_MAX_PREFETCH;

    return Math.max(1, Math.min(maxPrefetch, noncesBehind));
  }

  private async getShardTransactions(shardId: number, nonce: number): Promise<BlockTransactions | undefined> {
    const result = await this.gatewayGet<GatewayBlockResponse>(`block/${shardId}/by-nonce/${nonce}?withTxs=true`);

    if (!result || !result.block) {
      this.logMessage(LogTopic.Debug, `Block for shardId ${shardId} and nonce ${nonce} is undefined or block not available`);
      return undefined;
    }

    if (result.block.miniBlocks === undefined) {
      this.logMessage(LogTopic.Debug, `Block for shardId ${shardId} and nonce ${nonce} does not contain any miniBlocks`);
      return { blockHash: result.block.hash, transactions: [] };
    }

    const filteredMiniBlocks = result.block.miniBlocks.filter(q => q.processingType !== GatewayMiniblockProcessingType.Scheduled);

    const transactions: ShardTransaction[] = this.computeShardTransactionsFromMiniblocks(filteredMiniBlocks);

    return { blockHash: result.block.hash, transactions };
  }

  private computeShardTransactionsFromMiniblocks(miniblocks: GatewayMiniblock[]): ShardTransaction[] {
    const predicate = (item: GatewayMiniblock): GatewayTransaction[] => { return item.transactions ?? []; };
    return selectMany(miniblocks, predicate)
      .map(ShardTransaction.build);
  }

  private async getHyperblockTransactions(nonce: number): Promise<BlockTransactions | undefined> {
    const result = await this.gatewayGet(`hyperblock/by-nonce/${nonce}`);
    if (!result) {
      return undefined;
    }
    const { hyperblock: { hash, transactions } } = result;
    if (transactions === undefined) {
      return { blockHash: hash, transactions: [] };
    }

    return {
      blockHash: hash,
      transactions: transactions.map(ShardTransaction.build),
    };
  }

  private async getShards(): Promise<number[]> {
    return this.shardsMaintainerService.get(this.options.gatewayUrl, this.options.timeout);
  }

  private async getCurrentNonce(shardId: number): Promise<number> {
    const shardInfo = await this.gatewayGet(`network/status/${shardId}`);
    return shardInfo.status.erd_nonce;
  }

  private async gatewayGet<T = any>(path: string): Promise<T> {
    if (this.httpService == null) {
      throw new Error("Http Service not initialized.");
    }

    return this.httpService.get<T>(path);
  }

  private async getCurrentNonces(): Promise<{ [key: number]: number }> {
    const currentNonces = await Promise.all(
      this.shardIds.map(shardId => this.getCurrentNonce(shardId))
    );

    const result: { [key: number]: number } = {};
    for (const [index, shardId] of this.shardIds.entries()) {
      result[shardId] = currentNonces[index];
    }

    return result;
  }

  private async getLastProcessedNonceOrCurrent(shardId: number, currentNonce: number): Promise<number> {
    let lastProcessedNonce = await this.getLastProcessedNonce(shardId, currentNonce);
    if (lastProcessedNonce === null || lastProcessedNonce === undefined) {
      lastProcessedNonce = currentNonce - 1;
      await this.setLastProcessedNonce(shardId, lastProcessedNonce);
    }

    return lastProcessedNonce;
  }

  private async getLastProcessedNonce(shardId: number, currentNonce: number): Promise<number | undefined> {
    const getLastProcessedNonceFunc = this.options.getLastProcessedNonce;
    if (!getLastProcessedNonceFunc) {
      return this.lastProcessedNoncesInternal[shardId];
    }

    return await getLastProcessedNonceFunc(shardId, currentNonce);
  }

  private async setLastProcessedNonce(shardId: number, nonce: number): Promise<void> {
    const setLastProcessedNonceFunc = this.options.setLastProcessedNonce;
    if (!setLastProcessedNonceFunc) {
      this.lastProcessedNoncesInternal[shardId] = nonce;
      return;
    }

    await setLastProcessedNonceFunc(shardId, nonce);
  }

  private async onTransactionsReceived(
    shardId: number,
    nonce: number,
    transactions: ShardTransaction[],
    statistics: TransactionStatistics,
    blockHash: string,
  ): Promise<void> {
    const onTransactionsReceivedFunc = this.options.onTransactionsReceived;
    if (onTransactionsReceivedFunc) {
      await onTransactionsReceivedFunc(shardId, nonce, transactions, statistics, blockHash);
    }
  }

  private async onTransactionsPending(
    shardId: number,
    nonce: number,
    transactions: ShardTransaction[],
  ): Promise<void> {
    const onTransactionsPendingFunc = this.options.onTransactionsPending;
    if (onTransactionsPendingFunc) {
      await onTransactionsPendingFunc(shardId, nonce, transactions);
    }
  }

  private logMessage(topic: LogTopic, message: string): void {
    const onMessageLogged = this.options.onMessageLogged;
    if (onMessageLogged) {
      onMessageLogged(topic, message);
    }
  }
}

export { ShardTransaction };
export { TransactionProcessorMode };
