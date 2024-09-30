import { base64Decode } from "../../utils/decoders";

export class GatewayTransaction {
  type: string = '';
  processingTypeOnSource: string = '';
  processingTypeOnDestination: string = '';
  round: number = 0;
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
  signature: string = '';
  miniblockType: string = '';
  miniblockHash: string = '';
  operation: string = '';
  function: string = '';
  initiallyPaidFee: string = '';
  chainID: string = '';
  version: number = 0;
  options: number = 0;

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
}
