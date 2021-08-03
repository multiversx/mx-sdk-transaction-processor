import axios from "axios";
import { LogTopic } from "./log.topic";
import { ShardTransaction } from "./shard.transaction";
import { TransactionProcessorOptions } from "./transaction.processor.options";
import { TransactionStatistics } from "./transaction.statistics";

export class TransactionProcessor {
  private startCurrentNonces: { [ key: number ]: number } = {};
  private startDate: Date = new Date();
  private shardIds: number[] = [];
  private options: TransactionProcessorOptions = new TransactionProcessorOptions();
  private readonly lastProcessedNoncesInternal: { [key: number]: number } = {};
  private isRunning: boolean = false;

  private crossShardTransactionsCounterDictionary: { [ key: string ]: number } = {};
  private crossShardTransactionsDictionary: { [ key: string ]: ShardTransaction } = {};

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

          let crossShardTransactions = this.handleCrossShardTransactions(shardId, transactions);

          let validTransactions = [];
          for (let transaction of transactions) {
            // we only care about transactions that are finalized in the given shard
            if (transaction.destinationShard !== shardId) {
              this.logMessage(LogTopic.CrossShardSmartContractResult, `Transaction with hash ${transaction.hash} skipped since its destination shard (${transaction.destinationShard}) is different than current shard ${shardId}`);
              continue;
            }

            // we skip transactions that are cross shard and still pending for smart-contract results
            if (this.crossShardTransactionsDictionary[transaction.hash]) {
              this.logMessage(LogTopic.CrossShardSmartContractResult, `Transaction with hash ${transaction.hash} skipped since it is present in the cross shard dictionary`);
              continue;
            }

            validTransactions.push(transaction);
          }

          for (let crossShardTransaction of crossShardTransactions) {
            validTransactions.push(crossShardTransaction);
          }

          if (validTransactions.length > 0) {
            let statistics = new TransactionStatistics();

            statistics.secondsElapsed = (new Date().getTime() - this.startDate.getTime()) / 1000;
            statistics.processedNonces = lastProcessedNonce - startLastProcessedNonces[shardId];
            statistics.noncesPerSecond = statistics.processedNonces / statistics.secondsElapsed;
            statistics.noncesLeft = currentNonce - lastProcessedNonce;
            statistics.secondsLeft = statistics.noncesLeft / statistics.noncesPerSecond * 1.1;

            await this.onTransactionsReceived(shardId, nonce, validTransactions, statistics);
          }

          this.setLastProcessedNonce(shardId, nonce);
        }
      } while (reachedTip === false);
    } finally {
      this.isRunning = false;
    }
  }

  private handleCrossShardTransactions(shardId: number, transactions: ShardTransaction[]): ShardTransaction[] {
    let crossShardTransactions: ShardTransaction[] = [];

    // pass 1: we add pending transactions in the dictionary from current shard to another one
    for (let transaction of transactions) {
      if (transaction.originalTransactionHash && transaction.sourceShard === shardId && transaction.destinationShard !== shardId) {
        let counter = this.crossShardTransactionsCounterDictionary[transaction.originalTransactionHash];
        if (!counter) {
          this.logMessage(LogTopic.CrossShardSmartContractResult, `Creating dictionary for original tx hash ${transaction.originalTransactionHash}`);
          let originalTransaction = transactions.find(x => x.hash === transaction.originalTransactionHash);
          if (originalTransaction) {
            this.crossShardTransactionsDictionary[transaction.originalTransactionHash] = originalTransaction;
          } else {
            this.logMessage(LogTopic.CrossShardSmartContractResult, `Could not identify transaction with hash ${transaction.originalTransactionHash} in transaction list`);
          }

          counter = 0;
        }

        counter++;
        this.logMessage(LogTopic.CrossShardSmartContractResult, `Detected new cross-shard SCR for original tx hash ${transaction.originalTransactionHash}, tx hash ${transaction.hash}, counter = ${counter}`);

        this.crossShardTransactionsCounterDictionary[transaction.originalTransactionHash] = counter;
      }
    }

    // pass 2: we delete pending transactions in the dictionary from another shard to current shard
    for (let transaction of transactions) {
      if (transaction.originalTransactionHash && transaction.sourceShard !== shardId && transaction.destinationShard === shardId) {
        let counter = this.crossShardTransactionsCounterDictionary[transaction.originalTransactionHash];
        if (!counter) {
          this.logMessage(LogTopic.CrossShardSmartContractResult, `No counter available for cross-shard SCR, original tx hash ${transaction.originalTransactionHash}, tx hash ${transaction.hash}`);
          continue;
        }

        counter--;
        this.logMessage(LogTopic.CrossShardSmartContractResult, `Finalized cross-shard SCR for original tx hash ${transaction.originalTransactionHash}, tx hash ${transaction.hash}, counter = ${counter}`);

        this.crossShardTransactionsCounterDictionary[transaction.originalTransactionHash] = counter;

        if (counter === 0) {
          this.logMessage(LogTopic.CrossShardSmartContractResult, `Completed cross-shard transaction for original tx hash ${transaction.originalTransactionHash}, tx hash ${transaction.hash}`);
          let originalTransaction = this.crossShardTransactionsDictionary[transaction.originalTransactionHash];
          if (originalTransaction) {
            this.logMessage(LogTopic.CrossShardSmartContractResult, `Pushing transaction with hash ${transaction.originalTransactionHash}, contents: ${JSON.stringify(originalTransaction)}`);
            crossShardTransactions.push(originalTransaction);
            delete this.crossShardTransactionsDictionary[transaction.originalTransactionHash];
          } else {
            this.logMessage(LogTopic.CrossShardSmartContractResult, `Could not identify transaction with hash ${transaction.originalTransactionHash} in cross shard transaction dictionary`);
          }

          delete this.crossShardTransactionsCounterDictionary[transaction.originalTransactionHash];
        }
      }
    }

    return crossShardTransactions;
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
        transaction.originalTransactionHash = item.originalTransactionHash;

        return transaction;
      });

    return transactions;
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

  private logMessage(topic: LogTopic, message: string) {
    let onMessageLogged = this.options.onMessageLogged;
    if (onMessageLogged) {
      onMessageLogged(topic, message);
    }
  }
}







