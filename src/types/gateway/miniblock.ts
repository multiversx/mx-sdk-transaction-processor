
import { GatewayMiniblockProcessingType } from "./miniblock-processing-type.enum";
import { GatewayTransaction } from "./transaction";

export interface GatewayMiniblock {
  hash: string;
  type: string;
  processingType: GatewayMiniblockProcessingType;
  constructionState: string;
  sourceShard: number;
  destinationShard: number;
  transactions: GatewayTransaction[];
  indexOfFirstTxProcessed: number;
  indexOfLastTxProcessed: number;
}
