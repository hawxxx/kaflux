# Changelog

## [0.9.0](https://github.com/hawxxx/kaflux/compare/v0.8.0...v0.9.0) (2026-11-20)


### Features

* **auth:** support Google and Azure broker IAM ([#94](https://github.com/hawxxx/kaflux/issues/94)) ([01552d2](https://github.com/hawxxx/kaflux/commit/01552d23b137b90d09c5c0db06e12ce4fa516141))
* **cleanup:** filter topic names with keywords ([#91](https://github.com/hawxxx/kaflux/issues/91)) ([4097a13](https://github.com/hawxxx/kaflux/commit/4097a13c3823a926e7c90cfbe7a5c3c44bfa2037))
* **mcp:** add MCP server with scoped tokens and confirmed write tools ([#92](https://github.com/hawxxx/kaflux/issues/92)) ([df9e34c](https://github.com/hawxxx/kaflux/commit/df9e34c31902d88f8569d51e23dfbd2e5e220ee4))
* **mcp:** add OAuth sign-in for MCP clients ([#93](https://github.com/hawxxx/kaflux/issues/93)) ([d9611a8](https://github.com/hawxxx/kaflux/commit/d9611a83e45e1d00cf84a2c1803a9a1c6d8fe6ca))
* **ui:** rename clusters in place and paginate names from ten clusters ([#88](https://github.com/hawxxx/kaflux/issues/88)) ([51007ea](https://github.com/hawxxx/kaflux/commit/51007ea0dbf1a7c608721a800a846dd08dffc64b))


### Bug Fixes

* **messages:** skip empty keys and document raw decoders ([#87](https://github.com/hawxxx/kaflux/issues/87)) ([d8a4831](https://github.com/hawxxx/kaflux/commit/d8a4831a6750a588c791308ad09af1a3ac28da84))
* **sessions:** align revoke and current session tag ([#90](https://github.com/hawxxx/kaflux/issues/90)) ([35a3c16](https://github.com/hawxxx/kaflux/commit/35a3c16f23189b30e5e94884afd9359410e2f061))

## [0.8.0](https://github.com/hawxxx/kaflux/compare/v0.7.2...v0.8.0) (2026-11-19)


### Features

* **messages:** decode Protobuf values without a registry ([#84](https://github.com/hawxxx/kaflux/issues/84)) ([888e784](https://github.com/hawxxx/kaflux/commit/888e7847c675b9486163d47496cf20634f801eea))
* **unused:** evaluate topics concurrently with live progress ([#86](https://github.com/hawxxx/kaflux/issues/86)) ([8c86f1b](https://github.com/hawxxx/kaflux/commit/8c86f1bd78562c5edec4361e61f4c31266e9b730))

## [0.7.2](https://github.com/hawxxx/kaflux/compare/v0.7.1...v0.7.2) (2026-11-19)


### Bug Fixes

* **metrics:** bind a Prometheus URL to each cluster ([946695a](https://github.com/hawxxx/kaflux/commit/946695ae9204722b0083247893f68edec295cb56))

## [0.7.1](https://github.com/hawxxx/kaflux/compare/v0.7.0...v0.7.1) (2026-11-19)


### Bug Fixes

* **console:** wait for the cluster list before loading pages ([#80](https://github.com/hawxxx/kaflux/issues/80)) ([95b2964](https://github.com/hawxxx/kaflux/commit/95b29640311165daa19265fcea4bd77a5ff8484a))

## [0.7.0](https://github.com/hawxxx/kaflux/compare/v0.6.2...v0.7.0) (2026-11-19)


### Features

* **login:** Drift sign-in page with running version and provider logos ([#78](https://github.com/hawxxx/kaflux/issues/78)) ([098ff66](https://github.com/hawxxx/kaflux/commit/098ff66899d5514384ad904661c12224a0e3a4a1))

## [0.6.2](https://github.com/hawxxx/kaflux/compare/v0.6.1...v0.6.2) (2026-11-19)


### Bug Fixes

* **deps:** bump golang.org/x/net to 0.62.0 ([#76](https://github.com/hawxxx/kaflux/issues/76)) ([60c776a](https://github.com/hawxxx/kaflux/commit/60c776a073b80eeb0a99421b1ac21659b2eb9536))

## [0.6.1](https://github.com/hawxxx/kaflux/compare/v0.6.0...v0.6.1) (2026-11-19)


### Bug Fixes

* **deps:** update go-jose and OpenTelemetry to patched releases ([#74](https://github.com/hawxxx/kaflux/issues/74)) ([911c7ab](https://github.com/hawxxx/kaflux/commit/911c7ab905449d4a2e51293356ade0cd098cb1ca))
* **jobs:** leave skipped topics out of rebalance progress and ETA ([#73](https://github.com/hawxxx/kaflux/issues/73)) ([23d03dd](https://github.com/hawxxx/kaflux/commit/23d03dd1048ec92800ca23cee605108712d02fad))

## [0.6.0](https://github.com/hawxxx/kaflux/compare/v0.5.0...v0.6.0) (2026-11-14)


### Features

* **helm:** add scheduling and storage options for spot and EBS deployments ([#71](https://github.com/hawxxx/kaflux/issues/71)) ([21b92c0](https://github.com/hawxxx/kaflux/commit/21b92c00f4a4418d23eeb68a50b02debdf0593bb))
* **reassign:** bulk topic selection, switch and stepper controls ([#67](https://github.com/hawxxx/kaflux/issues/67)) ([7484c91](https://github.com/hawxxx/kaflux/commit/7484c917e7bd4b72ced4344137665ed8d9c1785d))
* **topics:** topic cleanup for unused topics ([#72](https://github.com/hawxxx/kaflux/issues/72)) ([ab2a2e0](https://github.com/hawxxx/kaflux/commit/ab2a2e08acaf146e8091a78723c65927e46ea2ea))
* **ui:** add restrained glass surfaces to console ([#65](https://github.com/hawxxx/kaflux/issues/65)) ([f69ef2d](https://github.com/hawxxx/kaflux/commit/f69ef2dc1d2f8d886960bf5a7d5305d9e819d0da))


### Bug Fixes

* **ui:** harden bulk topic add, throttle stepper and glass polish ([#70](https://github.com/hawxxx/kaflux/issues/70)) ([69c7efa](https://github.com/hawxxx/kaflux/commit/69c7efadd30c8dcbf20789c2aa940e9f90dbafcf))
* **ui:** stop Empty consumer group badge rendering as an empty-state panel ([#66](https://github.com/hawxxx/kaflux/issues/66)) ([325c447](https://github.com/hawxxx/kaflux/commit/325c447a7718a0f3e865638575c271eff09af679))

## [0.5.0](https://github.com/hawxxx/kaflux/compare/v0.4.1...v0.5.0) (2026-10-18)


### Features

* **cluster:** show Kaflux version, Kafka version and KRaft/ZooKeeper mode ([#54](https://github.com/hawxxx/kaflux/issues/54)) ([464f0bc](https://github.com/hawxxx/kaflux/commit/464f0bce45bdca9503e222e821b8aaaab4727a74))
* **cmd:** add a healthcheck subcommand for the distroless image ([4d41398](https://github.com/hawxxx/kaflux/commit/4d41398765676e8033814dbd16807f43bd21c8c2))
* **messages:** search a topic on the server ([#46](https://github.com/hawxxx/kaflux/issues/46)) ([abd91cf](https://github.com/hawxxx/kaflux/commit/abd91cf71ee4dabcf8de1d6aad782671784e27ea))


### Bug Fixes

* **api:** report capability lookup failures as 503 instead of a blank 409 ([950bed9](https://github.com/hawxxx/kaflux/commit/950bed9383996ac55745849be91e2ef9364d0fed))
* **deploy:** harden Compose services and the Helm chart ([4d41398](https://github.com/hawxxx/kaflux/commit/4d41398765676e8033814dbd16807f43bd21c8c2))
* **frontend:** confine API requests to an allow-listed same-origin base ([4d41398](https://github.com/hawxxx/kaflux/commit/4d41398765676e8033814dbd16807f43bd21c8c2))
* **frontend:** remove dynamic regular expressions and Math.random ([4d41398](https://github.com/hawxxx/kaflux/commit/4d41398765676e8033814dbd16807f43bd21c8c2))
* **search:** finish a partition only when its position reaches the end offset ([950bed9](https://github.com/hawxxx/kaflux/commit/950bed9383996ac55745849be91e2ef9364d0fed))
* **store:** bind active job states as query parameters ([4d41398](https://github.com/hawxxx/kaflux/commit/4d41398765676e8033814dbd16807f43bd21c8c2))

## [0.4.1](https://github.com/hawxxx/kaflux/compare/v0.4.0...v0.4.1) (2026-10-10)


### Bug Fixes

* **frontend:** hide app shell until the session is known ([#41](https://github.com/hawxxx/kaflux/issues/41)) ([11a5511](https://github.com/hawxxx/kaflux/commit/11a551175c10e82226baa9a6bab76c690b1f200a))
* **messages:** keep all-partition reads complete and fair ([#43](https://github.com/hawxxx/kaflux/issues/43)) ([70f8d3a](https://github.com/hawxxx/kaflux/commit/70f8d3afd4d116f602549329d323136731a0960b))
* **ui:** polish topic toggles, degraded health color and cluster menu ([#40](https://github.com/hawxxx/kaflux/issues/40)) ([d9774d0](https://github.com/hawxxx/kaflux/commit/d9774d034404785119aad86d9af488957680599c))

## [0.4.0](https://github.com/hawxxx/kaflux/compare/v0.3.0...v0.4.0) (2026-10-09)


### Features

* **auth:** make OIDC, LDAP and role grants work as configured ([#36](https://github.com/hawxxx/kaflux/issues/36)) ([043977d](https://github.com/hawxxx/kaflux/commit/043977d5011b9cb485d8955ff1c715a1f6fb9c52))
* **messages:** read all partitions at once ([#38](https://github.com/hawxxx/kaflux/issues/38)) ([26cf5e3](https://github.com/hawxxx/kaflux/commit/26cf5e363c82b9cb4d6261c521e95c0e98dc6571))
* **rebalance:** cluster health and run rebalances topic by topic with progress, pause and rollback ([#34](https://github.com/hawxxx/kaflux/issues/34)) ([4885122](https://github.com/hawxxx/kaflux/commit/48851223530e3065c6ded9e0b6663656e33d01dd))

## [0.3.0](https://github.com/hawxxx/kaflux/compare/v0.2.1...v0.3.0) (2026-10-09)


### Features

* **clusters:** rename any cluster from settings with unique names ([#31](https://github.com/hawxxx/kaflux/issues/31)) ([0910282](https://github.com/hawxxx/kaflux/commit/0910282d437e309b1f942c5abdcf5b7cc77d399c))
* **reassignments:** lead plan review with the changes table ([#29](https://github.com/hawxxx/kaflux/issues/29)) ([4935fb2](https://github.com/hawxxx/kaflux/commit/4935fb21d41294ca0b2b0700e2824d30bc79efd0))
* **ui:** add 404 and error pages ([#33](https://github.com/hawxxx/kaflux/issues/33)) ([1e6963a](https://github.com/hawxxx/kaflux/commit/1e6963afff200ab7ee05f04de13929cea5def2e5))

## [0.2.1](https://github.com/hawxxx/kaflux/compare/v0.2.0...v0.2.1) (2026-10-09)


### Bug Fixes

* show generic reasons for blocked reassignments on non-MSK clusters ([#25](https://github.com/hawxxx/kaflux/issues/25)) ([cf9b232](https://github.com/hawxxx/kaflux/commit/cf9b2327282847c99c6c6f73aa4c72fb41b7ed8a))

## [0.2.0](https://github.com/hawxxx/kaflux/compare/v0.1.0...v0.2.0) (2026-10-09)


### Features

* **console:** time zone selector for displayed and entered times ([b3bf7ec](https://github.com/hawxxx/kaflux/commit/b3bf7ecf1b072358e57e45c06d80e8ab3ba5b1ad))
* **console:** time zone selector for displayed and entered times ([f7d78be](https://github.com/hawxxx/kaflux/commit/f7d78be4b5966e992226869beb6e0677b15aab16))
* **frontend:** color consumer group state tags ([c1adcf7](https://github.com/hawxxx/kaflux/commit/c1adcf7f0326473d53170f3f7a1a8d0ca121e781))
* **frontend:** color consumer group state tags ([954511b](https://github.com/hawxxx/kaflux/commit/954511b04b87098e0a7b628169a10db32937ffff))
* **messages:** expandable records and a robust produce dialog ([49ab77a](https://github.com/hawxxx/kaflux/commit/49ab77a34cf21436a9ec1ff8f4e4487846b85159))
* **messages:** expandable records and a robust produce dialog ([4c8c552](https://github.com/hawxxx/kaflux/commit/4c8c5520eb41156a9839dfab321272940b1493e7))
* **topics:** internal tag, message counts, bulk delete and row actions ([4b6c5be](https://github.com/hawxxx/kaflux/commit/4b6c5be9febf9b4d3cc5541783400e4e69b325fa))
* **topics:** internal tag, message counts, bulk delete and row actions ([8206aad](https://github.com/hawxxx/kaflux/commit/8206aad33d97516f588e6cb7642f972e7d404e7a))
* **topics:** show consumers and under-replicated count on topic overview ([e4ad2af](https://github.com/hawxxx/kaflux/commit/e4ad2af8207049eccfc07eb4c11857026ab543ad))
* **topics:** show consumers and URP on topic overview ([bfac2aa](https://github.com/hawxxx/kaflux/commit/bfac2aa2326c77d277705924ab931cd8d7978d08))


### Bug Fixes

* **deps:** bump Go to 1.26.9 and golang.org/x/net to 0.60.0 ([87a5055](https://github.com/hawxxx/kaflux/commit/87a5055d2c876f12ad47547ea295a7cf8c1526aa))
* **deps:** bump Go to 1.26.9 and golang.org/x/net to 0.60.0 ([e6ab541](https://github.com/hawxxx/kaflux/commit/e6ab541a6527553a89e1e3af618b60ff675b01c9))
