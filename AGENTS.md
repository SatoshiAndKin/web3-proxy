# Block relay

- Keep the relay focused on transport. Destination execution and consensus nodes own Ethereum validation.
- Forward source blocks, blobs, and proofs. Do not add local KZG verification or proof generation, Beacon Merkle trees, or execution block reconstruction to the relay.
- Parse only what request formatting, routing, and delivery require. Preserve signed block data and supplied proof bytes.
- Keep transport size limits, bounded work, private Engine authentication, and independent target delivery.
