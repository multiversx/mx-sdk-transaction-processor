import { ShardTransaction } from "./shard-transaction";

export class CrossShardTransaction {
  transaction: ShardTransaction;
  counter: number = 0;
  created: Date = new Date();

  constructor(transaction: ShardTransaction) {
    this.transaction = transaction;
  }
}
