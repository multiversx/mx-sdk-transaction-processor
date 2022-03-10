import axios from 'axios';

export class TransactionProcessor {
  private readonly METACHAIN = 4294967295;
  private startDate: Date = new Date();
  private shardIds: number[] = [];
  private options: TransactionProcessorOptions = new TransactionProcessorOptions();
  private readonly lastProcessedNoncesInternal: { [key: number]: number } = {};
  private isRunning: boolean = false;

  private NETWORK_RESET_NONCE_THRESHOLD = 10000;

  private crossShardDictionary: { [key: string]: CrossShardTransaction } = {};

  async start(options: TransactionProcessorOptions) {
    this.options = options;

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
          if (lastProcessedNonce > currentNonce + this.NETWORK_RESET_NONCE_THRESHOLD) {
            this.logMessage(LogTopic.Debug, `Detected network reset. Setting last processed nonce to ${currentNonce} for shard ${shardId}`);
            lastProcessedNonce = currentNonce;
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

          const transactionsResult = await this.getShardTransactions(shardId, nonce);
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
      this.shardIds = [this.METACHAIN];
      this.startDate = new Date();

      let startLastProcessedNonce = 0;

      let reachedTip: boolean;

      const currentNonces = await this.getCurrentNonces();
      const currentNonce = currentNonces[this.METACHAIN];

      do {
        reachedTip = true;

        let lastProcessedNonce = await this.getLastProcessedNonceOrCurrent(this.METACHAIN, currentNonce);

        this.logMessage(LogTopic.Debug, `currentNonce: ${currentNonce}, lastProcessedNonce: ${lastProcessedNonce}`);

        if (lastProcessedNonce === currentNonce) {
          continue;
        }

        // this is to handle the situation where the current nonce is reset
        // (e.g. devnet/testnet reset where the nonces start again from zero)
        if (lastProcessedNonce > currentNonce + this.NETWORK_RESET_NONCE_THRESHOLD) {
          this.logMessage(LogTopic.Debug, `Detected network reset. Setting last processed nonce to ${currentNonce}`);
          lastProcessedNonce = currentNonce;
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

        const transactionsResult = await this.getHyperblockTransactions(nonce);
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
        await this.setLastProcessedNonce(this.METACHAIN, nonce);
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
          const data = TransactionProcessor.base64Decode(transaction.data);
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
          const data = TransactionProcessor.base64Decode(transaction.data);
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

  static base64Decode(str: string): string {
    return Buffer.from(str, 'base64').toString('binary');
  }

  private selectMany<TIN, TOUT>(array: TIN[], predicate: Function): TOUT[] {
    const result = [];

    for (const item of array) {
      result.push(...predicate(item));
    }

    return result;
  }

  private async getShardTransactions(shardId: number, nonce: number): Promise<{ blockHash: string, transactions: ShardTransaction[] } | undefined> {
    const result = await this.gatewayGet(`block/${shardId}/by-nonce/${nonce}?withTxs=true`);

    if (!result || !result.block) {
      this.logMessage(LogTopic.Debug, `Block for shardId ${shardId} and nonce ${nonce} is undefined or block not available`);
      return undefined;
    }

    if (result.block.miniBlocks === undefined) {
      this.logMessage(LogTopic.Debug, `Block for shardId ${shardId} and nonce ${nonce} does not contain any miniBlocks`);
      return { blockHash: result.block.hash, transactions: [] };
    }

    const transactions: ShardTransaction[] = this.selectMany(result.block.miniBlocks, (item: any) => item.transactions)
      .map((item: any) => TransactionProcessor.itemToShardTransaction(item));

    return { blockHash: result.block.hash, transactions };
  }

  private async getHyperblockTransactions(nonce: number): Promise<{ blockHash: string, transactions: ShardTransaction[] } | undefined> {
    const result = await this.gatewayGet(`hyperblock/by-nonce/${nonce}`);
    if (!result) {
      return undefined;
    }
    const { hyperblock: { hash, transactions } } = result;

    const shardTransactions: ShardTransaction[] = transactions
      .map((item: any) => TransactionProcessor.itemToShardTransaction(item));

    return { blockHash: hash, transactions: shardTransactions };
  }

  static itemToShardTransaction(item: any): ShardTransaction {
    const transaction = new ShardTransaction();
    transaction.data = item.data;
    transaction.sender = item.sender;
    transaction.receiver = item.receiver;
    transaction.sourceShard = item.sourceShard;
    transaction.destinationShard = item.destinationShard;
    transaction.hash = item.hash;
    transaction.nonce = item.nonce;
    transaction.status = item.status;
    transaction.value = item.value;
    transaction.originalTransactionHash = item.originalTransactionHash;
    transaction.gasPrice = item.gasPrice;
    transaction.gasLimit = item.gasLimit;
    return transaction;
  }

  private async getShards(): Promise<number[]> {
    const networkConfig = await this.gatewayGet('network/config');
    const shardCount = networkConfig.config.erd_num_shards_without_meta;

    const result = [];
    for (let i = 0; i < shardCount; i++) {
      result.push(i);
    }

    result.push(this.METACHAIN);
    return result;
  }

  private async getCurrentNonce(shardId: number): Promise<number> {
    const shardInfo = await this.gatewayGet(`network/status/${shardId}`);
    return shardInfo.status.erd_nonce;
  }

  private async gatewayGet(path: string): Promise<any> {
    const gatewayUrl = this.options.gatewayUrl ?? 'https://gateway.elrond.com';
    const fullUrl = `${gatewayUrl}/${path}`;

    try {
      const result = await axios.get(fullUrl);
      return result.data.data;
    } catch (error) {
      this.logMessage(LogTopic.Error, `Error when getting from gateway url ${fullUrl}: ${error}`);
    }
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

  private async setLastProcessedNonce(shardId: number, nonce: number) {
    const setLastProcessedNonceFunc = this.options.setLastProcessedNonce;
    if (!setLastProcessedNonceFunc) {
      this.lastProcessedNoncesInternal[shardId] = nonce;
      return;
    }

    await setLastProcessedNonceFunc(shardId, nonce);
  }

  private async onTransactionsReceived(shardId: number, nonce: number, transactions: ShardTransaction[], statistics: TransactionStatistics, blockHash: string) {
    const onTransactionsReceivedFunc = this.options.onTransactionsReceived;
    if (onTransactionsReceivedFunc) {
      await onTransactionsReceivedFunc(shardId, nonce, transactions, statistics, blockHash);
    }
  }

  private async onTransactionsPending(shardId: number, nonce: number, transactions: ShardTransaction[]) {
    const onTransactionsPendingFunc = this.options.onTransactionsPending;
    if (onTransactionsPendingFunc) {
      await onTransactionsPendingFunc(shardId, nonce, transactions);
    }
  }

  private logMessage(topic: LogTopic, message: string) {
    const onMessageLogged = this.options.onMessageLogged;
    if (onMessageLogged) {
      onMessageLogged(topic, message);
    }
  }
}

export enum LogTopic {
  CrossShardSmartContractResult = 'CrossShardSmartContractResult',
  Debug = 'Debug',
  Error = 'Error',
}

export class ShardTransaction {
  value: string = '';
  data?: string;
  hash: string = '';
  sender: string = '';
  receiver: string = '';
  status: string = '';
  sourceShard: number = 0;
  destinationShard: number = 0;
  nonce: number = 0;
  previousTransactionHash?: string;
  originalTransactionHash?: string;
  gasPrice?: number;
  gasLimit?: number;

  private dataDecoded: string | undefined;
  private getDataDecoded(): string | undefined {
    if (!this.dataDecoded) {
      if (this.data) {
        this.dataDecoded = TransactionProcessor.base64Decode(this.data);
      }
    }

    return this.dataDecoded;
  }

  private dataFunctionName: string | undefined;
  public getDataFunctionName(): string | undefined {
    if (!this.dataFunctionName) {
      const decoded = this.getDataDecoded();
      if (decoded) {
        this.dataFunctionName = decoded.split('@')[0];
      }
    }

    return this.dataFunctionName;
  }

  private dataArgs: string[] | undefined;
  public getDataArgs(): string[] | undefined {
    if (!this.dataArgs) {
      const decoded = this.getDataDecoded();
      if (decoded) {
        this.dataArgs = decoded.split('@').splice(1);
      }
    }

    return this.dataArgs;
  }
}

export enum TransactionProcessorMode {
  Shardblock = 'Shardblock',
  Hyperblock = 'Hyperblock',
}

export class TransactionProcessorOptions {
  gatewayUrl?: string;
  maxLookBehind?: number;
  waitForFinalizedCrossShardSmartContractResults?: boolean;
  notifyEmptyBlocks?: boolean;
  includeCrossShardStartedTransactions?: boolean;
  mode?: TransactionProcessorMode;
  onTransactionsReceived?: (shardId: number, nonce: number, transactions: ShardTransaction[], statistics: TransactionStatistics, blockHash: string) => Promise<void>;
  onTransactionsPending?: (shardId: number, nonce: number, transactions: ShardTransaction[]) => Promise<void>;
  getLastProcessedNonce?: (shardId: number, currentNonce: number) => Promise<number | undefined>;
  setLastProcessedNonce?: (shardId: number, nonce: number) => Promise<void>;
  onMessageLogged?: (topic: LogTopic, message: string) => void;
}

export class TransactionStatistics {
  secondsElapsed: number = 0;
  processedNonces: number = 0;
  noncesPerSecond: number = 0;
  noncesLeft: number = 0;
  secondsLeft: number = 0;
}

export class CrossShardTransaction {
  transaction: ShardTransaction;
  counter: number = 0;
  created: Date = new Date();

  constructor(transaction: ShardTransaction) {
    this.transaction = transaction;
  }
}
