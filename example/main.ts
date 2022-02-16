import { NestFactory } from '@nestjs/core';
import { TransactionProcessorModule } from "./crons/transaction.processor.module";

async function start() {
    const transactionProcessorApp = await NestFactory.create(TransactionProcessorModule);
    await transactionProcessorApp.listen(4242);
}

start().then()
