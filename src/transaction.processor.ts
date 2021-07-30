import axios from "axios";

export class TransactionProcessor {
  private startCurrentNonces: { [ key: number ]: number } = {};
  private startDate: Date = new Date();
  private shardIds: number[] = [];
  private options: TransactionProcessorOptions = new TransactionProcessorOptions();
  private readonly lastProcessedNoncesInternal: { [key: number]: number } = {};
  private isRunning: boolean = false;

  async start(options: TransactionProcessorOptions) {
    if (this.isRunning) {
      throw new Error('Transaction processor is already running');
    }

    this.isRunning = true;

    try {
      this.options = options;
      this.startDate = new Date();
      this.shardIds = await this.getShards();
      this.startCurrentNonces = await this.getCurrentNonces();

      let startLastProcessedNonces: { [ key: number ]: number } = {};

      let reachedTip: boolean;

      do {
        reachedTip = true;

        for (let shardId of this.shardIds) {
          let currentNonce = await this.estimateCurrentNonce(shardId);
          let lastProcessedNonce = await this.getLastProcessedNonceOrCurrent(shardId, currentNonce);

          if (lastProcessedNonce === currentNonce) {
            continue;
          }

          // this is to handle the situation where the current nonce is reset
          // (e.g. devnet/testnet reset where the nonces start again from zero)
          if (lastProcessedNonce > currentNonce) {
            lastProcessedNonce = currentNonce;
          }

          if (options.maxLookBehind && currentNonce - lastProcessedNonce > options.maxLookBehind) {
            lastProcessedNonce = currentNonce - options.maxLookBehind;
          }

          if (!startLastProcessedNonces[shardId]) {
            startLastProcessedNonces[shardId] = lastProcessedNonce;
          }

          let nonce = lastProcessedNonce + 1;

          let transactions = await this.getShardTransactions(shardId, nonce);
          if (transactions === undefined) {
            continue;
          }

          reachedTip = false;

          if (transactions.length > 0) {
            let statistics = new TransactionStatistics();

            statistics.secondsElapsed = (new Date().getTime() - this.startDate.getTime()) / 1000;
            statistics.processedNonces = lastProcessedNonce - startLastProcessedNonces[shardId];
            statistics.noncesPerSecond = statistics.processedNonces / statistics.secondsElapsed;
            statistics.noncesLeft = currentNonce - lastProcessedNonce;
            statistics.secondsLeft = statistics.noncesLeft / statistics.noncesPerSecond * 1.1;

            await this.onTransactionsReceived(shardId, nonce, transactions, statistics);
          }

          this.setLastProcessedNonce(shardId, nonce);
        }
      } while (reachedTip === false);
    } finally {
      this.isRunning = false;
    }
  }

  private selectMany<TIN, TOUT>(array: TIN[], predicate: Function): TOUT[] {
    let result = [];
  
    for (let item of array) {
        result.push(...predicate(item));
    }
  
    return result;
  };

  private async getShardTransactions(shardId: number, nonce: number): Promise<ShardTransaction[] | undefined> {
    let result = await this.gatewayGet(`block/${shardId}/by-nonce/${nonce}?withTxs=true`);

    if (!result || !result.block) {
      return undefined;
    }

    if (result.block.miniBlocks === undefined) {
      return [];
    }

    let transactions: ShardTransaction[] = this.selectMany(result.block.miniBlocks, (x: any) => x.transactions)
      .map((item: any) => {
        let transaction = new ShardTransaction();
        transaction.data = item.data;
        transaction.sender = item.sender;
        transaction.receiver = item.receiver;
        transaction.sourceShard = item.sourceShard;
        transaction.destinationShard = item.destinationShard;
        transaction.hash = item.hash;
        transaction.nonce = item.nonce;
        transaction.status = item.status;
        transaction.value = item.value;

        return transaction;
      });

    // we only care about transactions that are finalized on the destinationShard
    return transactions.filter(x => x.destinationShard === shardId);
  }

  private async getShards(): Promise<number[]> {
    let networkConfig = await this.gatewayGet('network/config');
    let shardCount = networkConfig.config.erd_num_shards_without_meta;

    let result = [];
    for (let i = 0; i < shardCount; i++) {
      result.push(i);
    }

    result.push(4294967295);
    return result;
  }

  private async getCurrentNonce(shardId: number): Promise<number> {
    let shardInfo = await this.gatewayGet(`network/status/${shardId}`);
    return shardInfo.status.erd_nonce;
  }

  private async gatewayGet(path: string): Promise<any> {
    let gatewayUrl = this.options.gatewayUrl ?? 'https://gateway.elrond.com';
    let fullUrl = `${gatewayUrl}/${path}`;

    try {
      let result = await axios.get(fullUrl);
      return result.data.data;
    } catch (error) {
      // console.error(`Error when getting from gateway url ${fullUrl}`, error);
    }
  }

  private async getCurrentNonces(): Promise<{ [ key: number ]: number }> {
    let currentNonces = await Promise.all(
      this.shardIds.map(shardId => this.getCurrentNonce(shardId))
    );

    let result: { [ key: number ]: number } = {};
    for (let [index, shardId] of this.shardIds.entries()) {
      result[shardId] = currentNonces[index];
    }

    return result;
  }

  private async estimateCurrentNonce(shardId: number): Promise<number> {
    let startCurrentNonce = this.startCurrentNonces[shardId];

    let secondsElapsedSinceStart = (new Date().getTime() - this.startDate.getTime()) / 1000;
    let roundsElapsedSinceStart = Math.floor(secondsElapsedSinceStart / 6);

    return startCurrentNonce + roundsElapsedSinceStart;
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
    let getLastProcessedNonceFunc = this.options.getLastProcessedNonce;
    if (!getLastProcessedNonceFunc) {
      return this.lastProcessedNoncesInternal[shardId];
    }

    return await getLastProcessedNonceFunc(shardId, currentNonce);
  }

  private async setLastProcessedNonce(shardId: number, nonce: number) {
    let setLastProcessedNonceFunc = this.options.setLastProcessedNonce;
    if (!setLastProcessedNonceFunc) {
      this.lastProcessedNoncesInternal[shardId] = nonce;
      return;
    }

    await setLastProcessedNonceFunc(shardId, nonce);
  }
  
  private async onTransactionsReceived(shardId: number, nonce: number, transactions: ShardTransaction[], statistics: TransactionStatistics) {
    let onTransactionsReceivedFunc = this.options.onTransactionsReceived;
    if (onTransactionsReceivedFunc) {
      await onTransactionsReceivedFunc(shardId, nonce, transactions, statistics);
    }
  }
}

export class ShardTransaction {
  value: string = '';
  data: string | undefined;
  hash: string = '';
  sender: string = '';
  receiver: string = '';
  status: string = '';
  sourceShard: number = 0;
  destinationShard: number = 0;
  nonce: number = 0;
}

export class TransactionStatistics {
  secondsElapsed: number = 0;
  processedNonces: number = 0;
  noncesPerSecond: number = 0;
  noncesLeft: number = 0;
  secondsLeft: number = 0;
}

export class TransactionProcessorOptions {
  gatewayUrl?: string;
  maxLookBehind?: number;
  onTransactionsReceived?: (shardId: number, nonce: number, transactions: ShardTransaction[], statistics: TransactionStatistics) => Promise<void>;
  getLastProcessedNonce?: (shardId: number, currentNonce: number) => Promise<number | undefined>;
  setLastProcessedNonce?: (shardId: number, nonce: number) => Promise<void>;
}
