import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';

import { TransactionProcessorService } from './transaction.processor.service';

@Module({
  imports: [
    ScheduleModule.forRoot(),
  ],
  providers: [
    TransactionProcessorService,
  ],
})
export class TransactionProcessorModule { }
