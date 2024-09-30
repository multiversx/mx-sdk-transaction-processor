import { base64Decode } from "../utils/decoders";
import { GatewayTransaction } from "./gateway/transaction";

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
  epoch: number = 0;

  private dataDecoded: string | undefined;
  private getDataDecoded(): string | undefined {
    if (!this.dataDecoded) {
      if (this.data) {
        this.dataDecoded = base64Decode(this.data);
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

  static build(item: GatewayTransaction): ShardTransaction {
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
    transaction.epoch = item.epoch;
    return transaction;
  }
}
