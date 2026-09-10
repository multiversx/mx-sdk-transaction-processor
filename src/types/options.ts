import { ShardTransaction } from "./shard-transaction";
import { LogTopic } from "./log-topic";
import { TransactionProcessorMode } from "./transaction-processor-mode.enum";
import { TransactionStatistics } from "./transaction-statistics";

export class TransactionProcessorOptions {
  gatewayUrl?: string;
  maxLookBehind?: number;
  /**
   * How many blocks per shard to read ahead of the block currently being processed, so that
   * gateway latency is paid concurrently instead of once per nonce. Read-ahead never targets a
   * nonce above the tip observed when the pass started, so it cannot request a block the network
   * has not produced yet. Delivery stays strictly one nonce at a time, in order.
   *
   * Defaults to 10. Set to 1 to restore reading exactly one block at a time.
   */
  maxPrefetch?: number;
  waitForFinalizedCrossShardSmartContractResults?: boolean;
  notifyEmptyBlocks?: boolean;
  includeCrossShardStartedTransactions?: boolean;
  mode?: TransactionProcessorMode;
  onTransactionsReceived?: (shardId: number, nonce: number, transactions: ShardTransaction[], statistics: TransactionStatistics, blockHash: string) => Promise<void>;
  onTransactionsPending?: (shardId: number, nonce: number, transactions: ShardTransaction[]) => Promise<void>;
  getLastProcessedNonce?: (shardId: number, currentNonce: number) => Promise<number | undefined>;
  setLastProcessedNonce?: (shardId: number, nonce: number) => Promise<void>;
  onMessageLogged?: (topic: LogTopic, message: string) => void;
  timeout?: number | undefined;
}
