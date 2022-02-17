import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';

import { Mode, TransactionProcessor, ShardTransaction } from '../../src/transaction.processor';
import { Locker } from '../utils/locker';

@Injectable()
export class TransactionProcessorService {
  private readonly logger: Logger;
  private lastNonce: number | undefined;
  private readonly transactionProcessor = new TransactionProcessor();
  constructor(
  ) {
    this.logger = new Logger(TransactionProcessorService.name);
  }

  @Cron('*/1 * * * * *')
  async handleNewElrondTransactions() {
    Locker.lock('newElrondTransactions', async () => {
        await this.transactionProcessor.start({
          mode: Mode.ProcessByHyperblockTransactions,
          gatewayUrl: 'https://gateway.elrond.com', // mainnet
          getLastProcessedNonce: async (_shardId: number, _currentNonce: number) => {
            // In ProcessByHyperblockTransactions shardId will always be METACHAIN
            return this.lastNonce;
          },
          setLastProcessedNonce: async(_shardId: number, nonce: number) => {
            // In ProcessByHyperblockTransactions shardId will always be METACHAIN
            this.lastNonce = nonce;
          },
          onTransactionsReceived: async (shardId: number, nonce: number, transactions: ShardTransaction[]) => {
            console.log(`Received ${transactions.length} transactions on shard ${shardId} and nonce ${nonce}`);
          }
        });
    });
  }

}
