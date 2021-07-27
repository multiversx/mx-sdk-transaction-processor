# Transaction Processor for JavaScript

Transaction processor for JavaScript and TypeScript (written in TypeScript).

## Distribution

[npm](https://www.npmjs.com/package/@elrondnetwork/transaction-processor)

## Usage

```js
let transactionProcessor = new TransactionProcessor();
await transactionProcessor.start({
  onTransactionsReceived: (shardId, nonce, transactions) => {
    console.log(`Received ${transactions.length} transactions on shard ${shardId} and nonce ${nonce}`);
  }
});
```