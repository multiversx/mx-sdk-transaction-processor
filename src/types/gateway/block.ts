import { GatewayMiniblock } from "./miniblock";

export interface GatewayBlock {
  nonce: number;
  round: number;
  epoch: number;
  shard: number;
  numTxs: number;
  hash: string;
  prevBlockHash: string;
  stateRootHash: string;
  accumulatedFees: string;
  developerFees: string;
  status: string;
  randSeed: string;
  prevRandSeed: string;
  pubKeyBitmap: string;
  signature: string;
  leaderSignature: string;
  chainID: string;
  softwareVersion: string;
  receiptsHash: string;
  timestamp: number;
  miniBlocks: GatewayMiniblock[];
}
