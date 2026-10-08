# Changelog

## [0.2.0](https://github.com/hawxxx/kaflux/compare/kaflux-v0.1.0...kaflux-v0.2.0) (2026-10-08)


### ⚠ BREAKING CHANGES

* **api:** API login requests must send Content-Type application/json; reverse proxies must preserve the public Host header.

### Features

* **balance:** add topic-scoped distribution analysis ([2921423](https://github.com/hawxxx/kaflux/commit/2921423cef9ca9de1b7a0da5bde65518f2dacf3a))
* **balance:** plan and validate while intelligent rebalancing is active ([59f74ba](https://github.com/hawxxx/kaflux/commit/59f74ba80456a2e9fb19d1a0ee2d025d0c858a57))
* **balance:** resolve broker capacity from config or MSK broker type ([c5174ce](https://github.com/hawxxx/kaflux/commit/c5174ce3dd893d5711db7b99e1c6a8efc70a3502))
* **balance:** show the intelligent rebalancing status as a colored badge ([8e192af](https://github.com/hawxxx/kaflux/commit/8e192afd98ce2ced868859bdb670b07c26ac9978))
* **frontend:** add accessible mobile navigation and metric formatting ([f0fcdd0](https://github.com/hawxxx/kaflux/commit/f0fcdd0cb1cceb7ffd56212fc9ffda15d3c1d3c8))
* **frontend:** add an auto refresh interval next to Refresh ([5409ebb](https://github.com/hawxxx/kaflux/commit/5409ebb19cbf5bdbee3a79ce94cbf7b63d05211f))
* **frontend:** animate stat-card sparklines ([bf5715e](https://github.com/hawxxx/kaflux/commit/bf5715e0fe5f4d6566b80e389a8d6bc6916094de))
* **frontend:** chart bytes in and bytes out on cluster throughput ([38b5545](https://github.com/hawxxx/kaflux/commit/38b5545977845de34845df76c4821fad7d403d23))
* **frontend:** health gauge, KPI ticker, live tail beam, panel fade-in ([b9e3a3b](https://github.com/hawxxx/kaflux/commit/b9e3a3bbcb99f536c3debe8c0ea71b50ea41d712))
* **frontend:** last-hour sparklines behind overview stat cards ([92d37b3](https://github.com/hawxxx/kaflux/commit/92d37b37075f3c7dded1060c175d827caa762454))
* **frontend:** readable multi-series metrics with range picker and table view ([477d36a](https://github.com/hawxxx/kaflux/commit/477d36a42cf056933fe7273edce779517c57b461))
* **frontend:** replace the refresh interval select with a styled menu ([2be259c](https://github.com/hawxxx/kaflux/commit/2be259c03d60a274d7764f6575032828f6853a3b))
* **frontend:** show progress for refresh, fetch and reassignments ([f7ffe10](https://github.com/hawxxx/kaflux/commit/f7ffe10d6e60f6e8da1abfa037f781e2b4b73f83))
* **frontend:** show progress while topics and consumer groups load ([0b91e5e](https://github.com/hawxxx/kaflux/commit/0b91e5e8af20d26439a59bd42189ed438ae64e85))
* **frontend:** sortable columns across tables ([cb9d887](https://github.com/hawxxx/kaflux/commit/cb9d887dad388cc3a133cfdb1d93c9aee20b2eb0))
* **messages:** searchable topic picker ([746e540](https://github.com/hawxxx/kaflux/commit/746e54051adca87636dfecfe80cc27e703aa9af1))
* **messages:** smooth live tail rendering ([e266253](https://github.com/hawxxx/kaflux/commit/e26625318aca2e7431c0325a400766a1f6e84f60))
* **metrics:** show every series at the cursor and a wrapping legend ([e5b1797](https://github.com/hawxxx/kaflux/commit/e5b179752b7b851aae788d2b7662d861a3d09718))
* **overview:** page the broker distribution list ([5b501e0](https://github.com/hawxxx/kaflux/commit/5b501e0a2e48a469e61205ca7e978cdd7c25d17d))
* **overview:** report replica balance skew and latest metric sample ([79f3124](https://github.com/hawxxx/kaflux/commit/79f3124b4dacff1555328f07759b6df3c22ad378))
* publish initial Kaflux operations console ([dc608d3](https://github.com/hawxxx/kaflux/commit/dc608d3e4a5b501fcc2a52c79e5ecc27117c2cbc))
* **reassignments:** deletable plans and multi-topic picker ([fc16a0c](https://github.com/hawxxx/kaflux/commit/fc16a0c0bb0c956907a0354a04ee9723acd4766e))
* **topics:** edit every topic config and readable topic views ([4225abd](https://github.com/hawxxx/kaflux/commit/4225abd53364acbc202dda1bd34d59ef3ff714e2))
* **topics:** per-partition consumer lag on topic Consumers tab ([21d30a5](https://github.com/hawxxx/kaflux/commit/21d30a5347793e5ceb58a30ed82e1eb9c64102dc))


### Bug Fixes

* **api:** harden login and upgrade vulnerable dependencies ([dc0e015](https://github.com/hawxxx/kaflux/commit/dc0e01564e364deac373fe50c0e9aab9c31d029e))
* **api:** serve the app shell for deep links with dotted names ([9c75a0c](https://github.com/hawxxx/kaflux/commit/9c75a0c86f47a7f043497fbc770a81e9f093d13e))
* **frontend:** add Kaflux favicon ([919062a](https://github.com/hawxxx/kaflux/commit/919062a10082e17ff9a09c07b5eabfa12885c5e9))
* **frontend:** distinct colors per series in metric charts ([3cc7ca8](https://github.com/hawxxx/kaflux/commit/3cc7ca873e71640c8f29d207561250ccf3b4e424))
* **frontend:** keep previous metric samples when a refresh has none ([f9b4988](https://github.com/hawxxx/kaflux/commit/f9b4988993bceee36d122cbe17e720d0615208b4))
* **frontend:** keep tablet header breadcrumb and logo intact ([1d4fc4e](https://github.com/hawxxx/kaflux/commit/1d4fc4e5052d30b59952402dee7f504b8775ec9f))
* **frontend:** keep the refresh control inside narrow screens ([949b91a](https://github.com/hawxxx/kaflux/commit/949b91a04dc3bbf77036dbead30a2f953cb3cad5))
* **frontend:** make the auto refresh label show what really refreshes ([085c3b1](https://github.com/hawxxx/kaflux/commit/085c3b1b58896981ad3ecda78fcdd7244c45f440))
* **frontend:** readable light theme and cleaner overview cards ([33da940](https://github.com/hawxxx/kaflux/commit/33da940df4ccc3ac9ce6a3e944a6b5eeb88638eb))
* **kafka:** derive replication factor from the lowest partition ([9d667f2](https://github.com/hawxxx/kaflux/commit/9d667f23673b8414f8f6509138819d77d050c3a2))
* **kafka:** keep cluster view usable while brokers restart ([c06a1ef](https://github.com/hawxxx/kaflux/commit/c06a1ef23dca814d653d3af60ed6217f8118e755))
* **kafka:** read cleanup policy and retention for the topic list ([cbae8e9](https://github.com/hawxxx/kaflux/commit/cbae8e9da2128bd292373e32091ad6ed71032d8a))
* **kafka:** report broker and topic sizes and consumer group lag ([4710a40](https://github.com/hawxxx/kaflux/commit/4710a40afeac8bb14423f370613ed8989e1a8691))
* **metrics:** bound per-topic queries and isolate query-shape errors ([ef75ff3](https://github.com/hawxxx/kaflux/commit/ef75ff3f9f43fc29726ab6efa6a802c30447d283))
* **metrics:** keep charts on screen across refreshes ([54b400a](https://github.com/hawxxx/kaflux/commit/54b400aabaaa2f2627a877a97ad4d69fbd121cb0))
* **metrics:** leave single-value and per-broker charts unbounded ([2b4100f](https://github.com/hawxxx/kaflux/commit/2b4100f0ec35d82608619c2fb441c3c8aa55cc23))
* **sessions:** group admin session list by user and stop duplicate logins ([f810eed](https://github.com/hawxxx/kaflux/commit/f810eedda7595ba64479a29a33504257121b821d))


### Performance Improvements

* **api:** gzip JSON responses for clients that accept it ([1d42aef](https://github.com/hawxxx/kaflux/commit/1d42aef2725ca9cb6af02b13262bf04a17da4f2d))
