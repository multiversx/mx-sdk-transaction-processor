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
  previousTransactionHash: string | undefined;
  originalTransactionHash: string | undefined;
}