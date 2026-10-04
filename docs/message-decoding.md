# Message decoding

Kaflux preserves message value and key bytes as Base64 alongside text previews. The JSON, Raw, Text, Hex and Base64 views remain available independently of a schema registry.

## Avro values

Configure a Confluent-compatible Schema Registry integration for the selected cluster. In Messages, choose **Avro · registry-ID** under **Value decoder**, select **Value**, **Key**, or **Key and value** under **Decode field**, then open the corresponding inspector's **Schema** view. This is opt-in; ordinary reads never query the registry. Schema-aware producing is not supported yet.

The backend reads the Confluent magic byte and big-endian schema ID, checks the caller's `schema-read` permission against the schema's registered subjects, and resolves `/schemas/ids/{id}`. A missing subject permission, failed registry request or invalid payload produces a per-record `decodeError`; it does not hide the raw record or make Kafka administration unavailable. Registry credentials and addresses stay on the backend.

Example API request:

```text
GET /api/v1/clusters/prod/messages?topic=orders&partition=0&offset=42&limit=25&decoder=avro&registry=primary
```

Successful records include `decodedValue`, `decodedFormat` and `schemaId`; original `valueBase64` remains unchanged. `consume` authorization for the topic is still required. JSON uses the decoder's native representation, including tagged Avro unions, Base64 byte fields and logical type conversions.

Set `decoderTarget=key` or `decoderTarget=both` to inspect schema-encoded keys. Key results use `decodedKey`, `keyDecodedFormat`, `keySchemaId` and `keyDecodeError`; `keyBase64` remains unchanged. Keys and values share the selected format and registry, eight-schema lookup limit, five-second deadline and 2 MiB decoded response budget. An invalid key does not suppress a valid value. The default target is `value`, preserving existing API behavior. For mixed serialization formats, inspect each field separately using its appropriate decoder.

## Protobuf values

Choose **Protobuf · registry-ID** to decode a `.proto` schema registered with type `PROTOBUF`, including bounded external references. The API accepts `decoder=protobuf` with the same registry and topic permission checks. The decoder handles the Confluent magic-byte-zero/schema-ID prefix and zigzag message-index array, including its `[0]` shortcut, to select top-level or nested message declarations. See the [Confluent wire-format specification](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html).

Schemas compile in memory once per schema ID per request, without filesystem or network import resolution by the compiler. Explicit registry references resolve through `/subjects/{subject}/versions/{version}` only after checking `schema-read` for each referenced subject. Positive pinned versions and unambiguous relative `.proto` import names are required; cycles, conflicting names, missing imports and non-Protobuf dependencies fail per record. Identical references share a request-local cache. Built-in Google imports require explicit registered references; no implicit standard-library fetch occurs. Protobuf messages, enums, oneofs, maps and repeated fields use Google's protobuf JSON mapping: 64-bit integers are strings, bytes are Base64 and absent fields are omitted. Unknown fields remain in the original bytes but do not appear in decoded JSON. Legacy wire groups and header/GUID-based schema identification are not supported.

Keys and values share at most eight referenced subject/version fetch attempts and 256 KiB of referenced source text per request, in addition to the existing eight root schema-ID limit and five-second deadline. Each compiled graph has at most eight distinct import names, eight import levels and 256 KiB total source text including its root; each source is at most 64 KiB. Registry documents may declare at most sixteen direct references. Imported message and field declarations share the same complexity budget as the root.

Before unmarshalling, a schema-aware wire scan limits nesting to 24 and total field/value occurrences to 10,000, including packed repeated values. Schemas share the 64 KiB input bound, with at most 2,048 message/field nodes, 256 fields per message and 24 nested declaration levels. Message indexes are limited to 16 levels. These limits apply before dynamic message allocation.

## Resource bounds and limitations

- Four decoding requests may run simultaneously per API process, with a five-second request deadline.
- Each read resolves at most eight distinct schema IDs; registry GET caching, four upstream slots, timeouts and circuit breaking apply.
- Encoded and decoded values are limited to 512 KiB; decoded additions to a response total at most 2 MiB.
- JSON tree branches render at most 50 entries per page and stop expanding at depth 24. Previous/Next controls preserve field names and array indexes without accumulating rendered rows.
- Schemas are limited to 64 KiB, 24 nesting levels and 2,048 visited type nodes. Records have at most 256 fields and unions 64 branches.
- Inline records, primitives, enums, fixed values and unions are supported. Collections, named references and external schema references are explicitly rejected to prevent allocations or recursion disproportionate to encoded size.
- Truncated previews, malformed envelopes and mismatched schema types are rejected. Avro additionally rejects trailing binary bytes. Glue and custom SerDe support remain unfinished.

Tests cover raw-byte preservation, subject authorization, cancellation, registry failures, malformed data, message indexes, repeated-value/nesting limits and unsupported structures. Real local Kafka integrations produce and consume Avro and Protobuf records through the API using HTTP registry fixtures; this does not certify an external registry deployment. Browser tests exercise selection, decoded inspection, raw bytes and failure states with explicit API fixtures.
