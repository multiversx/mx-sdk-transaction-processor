import { LogTopic } from "./log.topic";
import { ShardTransaction } from "./shard.transaction";
import { TransactionStatistics } from "./transaction.statistics";

export class TransactionProcessorOptions {
  gatewayUrl?: string;
  maxLookBehind?: number;
  waitForFinalizedCrossShardSmartContractResults?: boolean;
  onTransactionsReceived?: (shardId: number, nonce: number, transactions: ShardTransaction[], statistics: TransactionStatistics) => Promise<void>;
  getLastProcessedNonce?: (shardId: number, currentNonce: number) => Promise<number | undefined>;
  setLastProcessedNonce?: (shardId: number, nonce: number) => Promise<void>;
  onMessageLogged?: (topic: LogTopic, message: string) => void;
}
