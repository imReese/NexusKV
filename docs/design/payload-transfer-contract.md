# Payload Transfer Contract

## Goal

Define the minimal execution-time payload and transfer model needed to stop treating materialization as payload-less control flow.

PR 10 does not add real transport. It adds the explicit types that future transport implementations will consume.

## Core types

### `PayloadDescriptor`

Describes what a handle refers to:

- descriptor id
- engine family
- semantic type
- state slice descriptor
- byte-size hint

### `StateSliceDescriptor`

Describes the portion of state represented by a payload:

- granularity
- token count
- token start
- optional block id
- optional page id

### `PayloadLocation`

Describes where a payload currently lives or should live:

- tier
- buffer kind
- device class
- locator
- handle kind

### `PayloadHandle`

Opaque execution-time handle for a materialized or materializable payload:

- handle id
- location
- descriptor
- ownership hint
- opaque reference

### `TransferRequest`

Describes a desired movement:

- action kind
- source handle
- desired target location
- selected backend
- degraded-from backend
- required materialization capability
- fallback reason

### `TransferResult`

Describes the result of a movement:

- status
- result handle
- optional intermediate handle
- detail string

### `TransferSession`

Groups a transfer request and result into a stable session record:

- session id
- request
- result

## Ownership and lifetime hints

Current ownership hints:

- `owned`
- `borrowed`
- `cached`
- `ephemeral`

These are descriptive for now. They are not yet hard memory-management contracts.

## Current fake/stubbed behavior

PR 10 intentionally keeps transport fake:

- baseline backend can store and retrieve payload handles from the in-memory store
- staged-copy backend returns target and intermediate host-staging handles
- remote shared-store backend returns a remote locator payload handle
- recompute and skip return fallback-oriented transfer sessions instead of real data movement

There is no real:

- GPU memory allocation
- host-pinned allocation
- tensor serialization
- DMA
- RDMA
- remote networking
- async completion engine

## Why this contract matters

Future work can now add real transport without changing connector APIs again.

The connector-visible result already exposes:

- selected backend
- payload handle
- transfer session
- fallback state

That is the stable seam needed for future:

- host-staged copy
- device-to-host-to-store paths
- store-to-host-to-device paths
- RDMA-capable backends
- lower-copy or zero-copy candidates
- remote shared-store implementations
