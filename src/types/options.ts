import { ShardTransaction } from "./shard-transaction";
import { LogTopic } from "./log-topic";
import { TransactionProcessorMode } from "./transaction-processor-mode.enum";
import { TransactionStatistics } from "./transaction-statistics";

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
  timeout?: number | undefined;
}
