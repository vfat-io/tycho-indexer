## [0.66.4](https://github.com/propeller-heads/tycho-indexer/compare/0.66.3...0.66.4) (2025-04-18)


### Bug Fixes

* **tycho-client:** relax default timeout values ([eba302f](https://github.com/propeller-heads/tycho-indexer/commit/eba302f11606798acc589f0ae3a57407a7b16da3))
* **tycho-client:** relax default timeout values ([#555](https://github.com/propeller-heads/tycho-indexer/issues/555)) ([3852e6a](https://github.com/propeller-heads/tycho-indexer/commit/3852e6a5db5932083feb37d9a0817e54ad8d3c49))

## [0.66.3](https://github.com/propeller-heads/tycho-indexer/compare/0.66.2...0.66.3) (2025-04-18)


### Bug Fixes

* cleanly close ws connections on timeouts ([319d988](https://github.com/propeller-heads/tycho-indexer/commit/319d988d3d095326f191907ab5e2a28e7fe7ca77))
* cleanly close ws connections on timeouts ([#556](https://github.com/propeller-heads/tycho-indexer/issues/556)) ([262d070](https://github.com/propeller-heads/tycho-indexer/commit/262d070b827730a594fef833484667d8825bdd78))

## [0.66.2](https://github.com/propeller-heads/tycho-indexer/compare/0.66.1...0.66.2) (2025-04-11)


### Bug Fixes

* Make Snapshot attributes pub ([#552](https://github.com/propeller-heads/tycho-indexer/issues/552)) ([55c668e](https://github.com/propeller-heads/tycho-indexer/commit/55c668e29a453809d820e362435093485d77d019))

## [0.66.1](https://github.com/propeller-heads/tycho-indexer/compare/0.66.0...0.66.1) (2025-04-10)

## [0.66.0](https://github.com/propeller-heads/tycho-indexer/compare/0.65.0...0.66.0) (2025-04-10)


### Features

* Implement retry logic for deadlock detection in database transactions ([5d63e49](https://github.com/propeller-heads/tycho-indexer/commit/5d63e497a0b8279a57f3809c84a1df91dd9cc261))
* Implement retry logic for deadlock detection in database transactions ([#549](https://github.com/propeller-heads/tycho-indexer/issues/549)) ([5a4b28e](https://github.com/propeller-heads/tycho-indexer/commit/5a4b28e49533ad3a99b2e7a08e905c18f9990a75))

## [0.65.0](https://github.com/propeller-heads/tycho-indexer/compare/0.64.2...0.65.0) (2025-04-02)


### Features

* Add link to Tycho.build telegram ([2ef1fe6](https://github.com/propeller-heads/tycho-indexer/commit/2ef1fe6687eb005b7f0b305abcb886136242a415))

## [0.64.2](https://github.com/propeller-heads/tycho-indexer/compare/0.64.1...0.64.2) (2025-04-01)


### Bug Fixes

* Set default value for account_balances in BlockChanges model ([4f69191](https://github.com/propeller-heads/tycho-indexer/commit/4f69191e2507a1e397ba930ce9719fec8bcd9366))
* Set default value for account_balances in BlockChanges model ([#544](https://github.com/propeller-heads/tycho-indexer/issues/544)) ([8ced9f9](https://github.com/propeller-heads/tycho-indexer/commit/8ced9f92a4c2d36feab51677f4fffea448dd5b60))

## [0.64.1](https://github.com/propeller-heads/tycho-indexer/compare/0.64.0...0.64.1) (2025-03-31)


### Bug Fixes

* include workspace cargo toml in release config ([2cecbd8](https://github.com/propeller-heads/tycho-indexer/commit/2cecbd8f050544b35e8bb82171dde7fa75781dd0))
* include workspace cargo toml in release config ([#543](https://github.com/propeller-heads/tycho-indexer/issues/543)) ([071af8a](https://github.com/propeller-heads/tycho-indexer/commit/071af8a0e40e51aa089d9b9d73da08d9ea736e6d))

## [0.64.0](https://github.com/propeller-heads/tycho-indexer/compare/0.63.1...0.64.0) (2025-03-31)


### Features

* add max_missed_blocks as staleness threshold ([d9c6846](https://github.com/propeller-heads/tycho-indexer/commit/d9c6846cb407a4a10928b3df6a1a7779530272ac))
* handle delayed exchanges on tycho-client ([d106b82](https://github.com/propeller-heads/tycho-indexer/commit/d106b82056a1db551031c3bf1911069fa19dd371))
* Handle delayed exchanges on tycho-client ([#539](https://github.com/propeller-heads/tycho-indexer/issues/539)) ([a5da6fd](https://github.com/propeller-heads/tycho-indexer/commit/a5da6fd1ebf7212430cff2b3d28d1efe48f211f6))
* only wait for one deltas message on client start up ([4fba42f](https://github.com/propeller-heads/tycho-indexer/commit/4fba42f168fac3a6d6c94a235a6aac9e39ffbdd5))
* use block_time + timeout as wait time for first deltas ([51e2313](https://github.com/propeller-heads/tycho-indexer/commit/51e23130d96f7b69fbe02eb439482fd7042bc693))


### Bug Fixes

* **tycho-client:** allow setting TYCHO_URL as an env var ([54fa5c4](https://github.com/propeller-heads/tycho-indexer/commit/54fa5c4a53f58e3562d1225f523c46831e8ae4ce))
* wait block time + timeout for first message ([751aae7](https://github.com/propeller-heads/tycho-indexer/commit/751aae7e62881412ca58840dd97085147b5a47df))

## [0.63.1](https://github.com/propeller-heads/tycho-indexer/compare/0.63.0...0.63.1) (2025-03-26)


### Bug Fixes

* add missing version to checkout action ([c025616](https://github.com/propeller-heads/tycho-indexer/commit/c025616b3c19106113c195bc95cb04c05848153f))
* add missing version to checkout action ([#537](https://github.com/propeller-heads/tycho-indexer/issues/537)) ([a803970](https://github.com/propeller-heads/tycho-indexer/commit/a803970ee129ef8d9ed8317ca0d2ecb5fe8c52c6))

## [0.63.0](https://github.com/propeller-heads/tycho-indexer/compare/0.62.0...0.63.0) (2025-03-26)


### Features

* Rename tycho-core to tycho-common. ([f5ed755](https://github.com/propeller-heads/tycho-indexer/commit/f5ed7559adf4fc64b4ac39ea0afdb9845e23bb1e))
* Rename tycho-core to tycho-common. ([#536](https://github.com/propeller-heads/tycho-indexer/issues/536)) ([476f3f3](https://github.com/propeller-heads/tycho-indexer/commit/476f3f34f58039877fa47863fd37e541ae4bb25f))

## [0.62.0](https://github.com/propeller-heads/tycho-indexer/compare/0.61.1...0.62.0) (2025-03-25)


### Features

* add Unichain support ([2422692](https://github.com/propeller-heads/tycho-indexer/commit/242269286d6020c1bd1489606783f2bf2e9ed6fd))
* add Unichain support ([#534](https://github.com/propeller-heads/tycho-indexer/issues/534)) ([e0c1b41](https://github.com/propeller-heads/tycho-indexer/commit/e0c1b41209b95b7408ef7a795ab99baea7b922d2))

## [0.61.1](https://github.com/propeller-heads/tycho-indexer/compare/0.61.0...0.61.1) (2025-03-11)


### Bug Fixes

* Add default to change attr to deprecate ([38efe73](https://github.com/propeller-heads/tycho-indexer/commit/38efe738f1620409f521c8ee5757c1b0f0598032))
* Revert skipping serialization on change field ([12e4482](https://github.com/propeller-heads/tycho-indexer/commit/12e4482f0b7e148223dbf550b3dd2223291d51fe))
* Revert skipping serialization on change field ([#529](https://github.com/propeller-heads/tycho-indexer/issues/529)) ([d53c7cf](https://github.com/propeller-heads/tycho-indexer/commit/d53c7cfe2607e30eaba517a49f0f3a444df7445c))

## [0.61.0](https://github.com/propeller-heads/tycho-indexer/compare/0.60.0...0.61.0) (2025-03-11)


### Features

* Add propellerheads docs domain to CORS allowance list ([7d87115](https://github.com/propeller-heads/tycho-indexer/commit/7d8711574bdf15c5e20fe9de1705a3dfd5bb85ab))
* add security requirement to paths ([55eec82](https://github.com/propeller-heads/tycho-indexer/commit/55eec823f39dc01d83407c63fad78a246e9da582))
* Allow CORS from Gitbook UI ([83c68b9](https://github.com/propeller-heads/tycho-indexer/commit/83c68b99a0fcd0ae26db230b2341e59d8fedbb65))
* Allow CORS from Gitbook UI ([#526](https://github.com/propeller-heads/tycho-indexer/issues/526)) ([98f8b41](https://github.com/propeller-heads/tycho-indexer/commit/98f8b4108e137b87bf5c98f00efc39d2c49021a8))
* Improve autogenerated openapi schema  ([#524](https://github.com/propeller-heads/tycho-indexer/issues/524)) ([53a4020](https://github.com/propeller-heads/tycho-indexer/commit/53a40202e9be65545e533a52592b86b57246de57))
* skip serializing change field on PC ([0317c8d](https://github.com/propeller-heads/tycho-indexer/commit/0317c8dc8a268ed78daa9d98fbfc74897d6cc5e9))

## [0.60.0](https://github.com/propeller-heads/tycho-indexer/compare/0.59.5...0.60.0) (2025-03-06)


### Features

* update codebase with latest substream message changes ([c5dca5e](https://github.com/propeller-heads/tycho-indexer/commit/c5dca5e5aac38b65f3fbd7e6472da28450263db9))
* update substreams proto files ([b72cb77](https://github.com/propeller-heads/tycho-indexer/commit/b72cb776e6ec88aaffd954c4374be39ee4c8b99c))
* update substreams proto messages ([#525](https://github.com/propeller-heads/tycho-indexer/issues/525)) ([3175a47](https://github.com/propeller-heads/tycho-indexer/commit/3175a470c552ca19495e1f7a532f07914b0a791b))

## [0.59.5](https://github.com/propeller-heads/tycho-indexer/compare/0.59.4...0.59.5) (2025-03-06)

## [0.59.4](https://github.com/propeller-heads/tycho-indexer/compare/0.59.3...0.59.4) (2025-03-06)

## [0.59.3](https://github.com/propeller-heads/tycho-indexer/compare/0.59.2...0.59.3) (2025-03-04)


### Bug Fixes

* update dockerfile ([114ad12](https://github.com/propeller-heads/tycho-indexer/commit/114ad127b494ec117f47dc30a21df141f9953739))
* update dockerfile ([dd4ffdc](https://github.com/propeller-heads/tycho-indexer/commit/dd4ffdc5e5a9bf177ce3504ab09f2285b9ad750f))
* update dockerfile ([be3c408](https://github.com/propeller-heads/tycho-indexer/commit/be3c40842e83a44e93244fd8c498e0da6e68b0df))
* update dockerfile ([#522](https://github.com/propeller-heads/tycho-indexer/issues/522)) ([488ac2e](https://github.com/propeller-heads/tycho-indexer/commit/488ac2edf3298085577b9b0eb506b4db37d27657))

## [0.59.2](https://github.com/propeller-heads/tycho-indexer/compare/0.59.1...0.59.2) (2025-03-04)

## [0.59.1](https://github.com/propeller-heads/tycho-indexer/compare/0.59.0...0.59.1) (2025-03-04)

## [0.59.0](https://github.com/propeller-heads/tycho-indexer/compare/0.58.3...0.59.0) (2025-02-28)


### Features

* fix conflicting timestamps, update latest block on reverts ([0bbd30e](https://github.com/propeller-heads/tycho-indexer/commit/0bbd30e64c7a5e74e173d5e3195449353c76efbd))
* get block by hash instead of number ([b38d97d](https://github.com/propeller-heads/tycho-indexer/commit/b38d97d1288bf528df6a9f21f68b8390479612e6))
* Handle blocks with same ts ([#494](https://github.com/propeller-heads/tycho-indexer/issues/494)) ([314de48](https://github.com/propeller-heads/tycho-indexer/commit/314de48e270ad8a749a72d3b254fc634a8994cc6))
* remove #[allow(dead_code)] ([f88c606](https://github.com/propeller-heads/tycho-indexer/commit/f88c606fe9040b316164727870645abf415c8632))
* return block_id together with cursor ([160757a](https://github.com/propeller-heads/tycho-indexer/commit/160757a1b662248dc0894074f342cb4e5250383b))
* simplify logic, remove previous block ts overwrite for Arbitrum ([e32cd2b](https://github.com/propeller-heads/tycho-indexer/commit/e32cd2bbb3778e10b5f25f7c8867b944ce63b8be))

## [0.58.3](https://github.com/propeller-heads/tycho-indexer/compare/0.58.2...0.58.3) (2025-02-28)


### Bug Fixes

* Fix token_balances typing on ResponseAccount ([e9d854b](https://github.com/propeller-heads/tycho-indexer/commit/e9d854bd9dedced448ffe6067a55738bf21cc689))
* Fix token_balances typing on ResponseAccount ([#514](https://github.com/propeller-heads/tycho-indexer/issues/514)) ([924e3d0](https://github.com/propeller-heads/tycho-indexer/commit/924e3d037d3def9dfb4804548de49e4095a547d7))

## [0.58.2](https://github.com/propeller-heads/tycho-indexer/compare/0.58.1...0.58.2) (2025-02-27)


### Bug Fixes

* propagate more meaningful RPC errors on the client ([#515](https://github.com/propeller-heads/tycho-indexer/issues/515)) ([6261432](https://github.com/propeller-heads/tycho-indexer/commit/6261432585ae1f2d4f8a99b6cfd629e4a9f1285f))
* propogate more meaningful RPC errors on the client ([bf92d73](https://github.com/propeller-heads/tycho-indexer/commit/bf92d73ae4a0535619b4d8d42f78c6c981403c34))
* return error with body on failed response parsing ([08b74cc](https://github.com/propeller-heads/tycho-indexer/commit/08b74cc0178dc0d69a3e461c271c401e3d23ee1e))

## [0.58.1](https://github.com/propeller-heads/tycho-indexer/compare/0.58.0...0.58.1) (2025-02-26)


### Bug Fixes

* index tokens table on quality for quicker quality based lookups ([7f38460](https://github.com/propeller-heads/tycho-indexer/commit/7f38460f1189deee46c934ec7ad535d93218fdec))

## [0.58.0](https://github.com/propeller-heads/tycho-indexer/compare/0.57.2...0.58.0) (2025-02-26)


### Features

* update PG gateway get_tokens quality filter to be ranged ([9648dd6](https://github.com/propeller-heads/tycho-indexer/commit/9648dd6b4b5d7021d8cc0057b644bbf3e5a656f2))
* update postgres gateway get_tokens quality filter to be ranged ([#516](https://github.com/propeller-heads/tycho-indexer/issues/516)) ([72fac95](https://github.com/propeller-heads/tycho-indexer/commit/72fac95de65110625d5fba154e675240866274f7))

## [0.57.2](https://github.com/propeller-heads/tycho-indexer/compare/0.57.1...0.57.2) (2025-02-14)

## [0.57.1](https://github.com/propeller-heads/tycho-indexer/compare/0.57.0...0.57.1) (2025-02-14)


### Bug Fixes

* add arbitrum to the native token migration ([657aa53](https://github.com/propeller-heads/tycho-indexer/commit/657aa53110416f5c54c8fc0ef6bba3293dcacf12))
* add arbitrum to the native token migration ([#512](https://github.com/propeller-heads/tycho-indexer/issues/512)) ([4a76b38](https://github.com/propeller-heads/tycho-indexer/commit/4a76b3802b1b1c9082f6bfc162d585c8173a2d6e))

## [0.57.0](https://github.com/propeller-heads/tycho-indexer/compare/0.56.5...0.57.0) (2025-02-14)


### Features

* add account balances to client-py ([e37f1c6](https://github.com/propeller-heads/tycho-indexer/commit/e37f1c6d96599aaa3e86e91643f4aebb226db575))
* add account balances to ResponseAccount ([145f270](https://github.com/propeller-heads/tycho-indexer/commit/145f2709aa4f6f27e30cc513553fa80f68c206f4))
* add account_balances to BlockChanges dto struct ([7cc3e3c](https://github.com/propeller-heads/tycho-indexer/commit/7cc3e3cd66d3d5a5a046e84d373a38e653185c26))
* add add_account_balances postgres gateway fn ([a38c0ae](https://github.com/propeller-heads/tycho-indexer/commit/a38c0aec5353992aca3b30c50066c0e9fb8351b9))
* add get_account_balances gateway fn ([83e87ef](https://github.com/propeller-heads/tycho-indexer/commit/83e87eff5bda2a373c64f123a25f5f92d0f917c9))
* add migration for token_id in account_balance table ([d3acab0](https://github.com/propeller-heads/tycho-indexer/commit/d3acab0c53ead8a902e43eb4db5d3055b602046c))
* add migration for token_id in account_balance table ([#495](https://github.com/propeller-heads/tycho-indexer/issues/495)) ([fa7e424](https://github.com/propeller-heads/tycho-indexer/commit/fa7e42442bed5cf7b840736fd042008f065317c1))
* also ensure native token when ensuring chain on start-up ([4cfd6c2](https://github.com/propeller-heads/tycho-indexer/commit/4cfd6c2387ad99ee5b40dd0a630e47b9c62ef72e))
* fetch account balances on get_contracts ([9ded485](https://github.com/propeller-heads/tycho-indexer/commit/9ded485ad33f5336f7f35f5a24a2ceceebc0d04f))
* handle account balance changes on reverts ([4cf80bb](https://github.com/propeller-heads/tycho-indexer/commit/4cf80bb4fe56cb8010935398067d2d3da0fb11f7))
* implement chain -> native token DB id cache ([b35fe77](https://github.com/propeller-heads/tycho-indexer/commit/b35fe77049697215ed3b530474c673aba9f53a52))
* implement hardcoded chain -> native token map ([531348c](https://github.com/propeller-heads/tycho-indexer/commit/531348c524ad5c265a5dd185c3082a027efa75e0))
* update existing postgres gateway methods with AccountBalance ([f67fb68](https://github.com/propeller-heads/tycho-indexer/commit/f67fb68fab649dea0ceb260e85c0b5b2568fb54a))
* Update Tycho Python client Account DTO ([#506](https://github.com/propeller-heads/tycho-indexer/issues/506)) ([a48bb82](https://github.com/propeller-heads/tycho-indexer/commit/a48bb822b5b2a54d71100ab309012a88633097e0))


### Bug Fixes

* do not error if ensure chains finds existing chain ([3874ac0](https://github.com/propeller-heads/tycho-indexer/commit/3874ac0528c65ec1437471bcd0b1785cb0fa97d4))
* filter balances by native token on account balance delta retrieval ([2d4655b](https://github.com/propeller-heads/tycho-indexer/commit/2d4655b3f4ff3b5b6561a639a9768f0cc730a832))
* only insert ETH native token if DB has 1 chain ([8a38bca](https://github.com/propeller-heads/tycho-indexer/commit/8a38bcabc85ed9b777017980b7adc919aa00f9f3))
* re-add mistakenly removed dead code clippy skip ([0c2de2b](https://github.com/propeller-heads/tycho-indexer/commit/0c2de2b92dc2cae3e35ae82641abf39eb54518e8))
* remove balance_float field ([2bb50f4](https://github.com/propeller-heads/tycho-indexer/commit/2bb50f4788b2227cad43c0e0df7f706e81fa40fc))
* set native token gas correctly ([58f1832](https://github.com/propeller-heads/tycho-indexer/commit/58f1832c584042f25944212b07ce8114ca8686e2))
* set native token gas value and improve cache name ([d3c3033](https://github.com/propeller-heads/tycho-indexer/commit/d3c303334f8496ac2239c75b44d2a9311af64302))
* set Starknet native token to ETH ([3f7ec77](https://github.com/propeller-heads/tycho-indexer/commit/3f7ec7733c9604924094d507558513ff5a79dac7))
* update accout balance table constraints ([7278897](https://github.com/propeller-heads/tycho-indexer/commit/727889779358bae778ee9f8863448439642f7c9f))
* update get_contract to filter balances by native token ([03dc2e0](https://github.com/propeller-heads/tycho-indexer/commit/03dc2e0fa616dc512dda14c3bb422a3f8f91d077))

## [0.56.5](https://github.com/propeller-heads/tycho-indexer/compare/0.56.4...0.56.5) (2025-02-12)


### Bug Fixes

* **rpc:** correctly apply TVL filtering on `/protocol_component` requests ([dd4dc20](https://github.com/propeller-heads/tycho-indexer/commit/dd4dc203ededcbe9c6de1eb73b1f99884966f3b2))
* **rpc:** correctly apply TVL filtering on `/protocol_component` requests ([#511](https://github.com/propeller-heads/tycho-indexer/issues/511)) ([588e368](https://github.com/propeller-heads/tycho-indexer/commit/588e36806046b1a779c791d6d9959b4fc76153a2))

## [0.56.4](https://github.com/propeller-heads/tycho-indexer/compare/0.56.3...0.56.4) (2025-02-11)


### Bug Fixes

* default RPC client and server to use HTTP/2 ([#508](https://github.com/propeller-heads/tycho-indexer/issues/508)) ([f611383](https://github.com/propeller-heads/tycho-indexer/commit/f6113838c889e5fd76feeaa85b3e41fc9a5bebe6))
* disable connection pooling on tycho-client rpc ([c6bb715](https://github.com/propeller-heads/tycho-indexer/commit/c6bb715d24f4122801f71d86791483ec41a07d8c))
* finetune connection timeouts on rpc ([b06ec9b](https://github.com/propeller-heads/tycho-indexer/commit/b06ec9b9a107892acb76bbe012abd510b6e495e4))
* set both client and server to use HTTP/2 ([1516abe](https://github.com/propeller-heads/tycho-indexer/commit/1516abea713ab747835941e7a95726e54d38dd29))

## [0.56.3](https://github.com/propeller-heads/tycho-indexer/compare/0.56.2...0.56.3) (2025-02-10)


### Bug Fixes

* increase Base block time default ([9bcb718](https://github.com/propeller-heads/tycho-indexer/commit/9bcb7188f281c39c36b20af860666605d7d3b82f))
* increase Base blocktime default ([#510](https://github.com/propeller-heads/tycho-indexer/issues/510)) ([82702cd](https://github.com/propeller-heads/tycho-indexer/commit/82702cdeee20c53e04e0c9e6e720909a4155cd00))

## [0.56.2](https://github.com/propeller-heads/tycho-indexer/compare/0.56.1...0.56.2) (2025-02-07)

## [0.56.1](https://github.com/propeller-heads/tycho-indexer/compare/0.56.0...0.56.1) (2025-02-06)


### Bug Fixes

* improve efficiency of activity filter on tokens query ([5649e46](https://github.com/propeller-heads/tycho-indexer/commit/5649e4659aed1a9f23e64a8d2812b72b412dc03e))
* improve efficiency of activity filter on tokens query ([#504](https://github.com/propeller-heads/tycho-indexer/issues/504)) ([de741e6](https://github.com/propeller-heads/tycho-indexer/commit/de741e64d9de08d50017872c27975113310dffd8))

## [0.56.0](https://github.com/propeller-heads/tycho-indexer/compare/0.55.2...0.56.0) (2025-02-05)


### Features

* **rpc:** sort tokens by address in `get_protocol_component` ([3014126](https://github.com/propeller-heads/tycho-indexer/commit/3014126a7deecdc56ed5cdf998968193ef9b3443))
* **rpc:** sort tokens by address in `get_protocol_component` ([#503](https://github.com/propeller-heads/tycho-indexer/issues/503)) ([f59b665](https://github.com/propeller-heads/tycho-indexer/commit/f59b6658d5bfe87e61776f199901dabf7b31c1f9))

## [0.55.2](https://github.com/propeller-heads/tycho-indexer/compare/0.55.1...0.55.2) (2025-02-05)


### Bug Fixes

* update ChangeType enum derived traits ([2e9032e](https://github.com/propeller-heads/tycho-indexer/commit/2e9032ea0afe219e897f87d4bcbf10c9360bb011))
* update ChangeType enum derived traits ([#502](https://github.com/propeller-heads/tycho-indexer/issues/502)) ([383e348](https://github.com/propeller-heads/tycho-indexer/commit/383e3482f45bb7960ad6e2593c3d54e964b6357d))

## [0.55.1](https://github.com/propeller-heads/tycho-indexer/compare/0.55.0...0.55.1) (2025-01-30)


### Bug Fixes

* clean up imports ([a32f8f9](https://github.com/propeller-heads/tycho-indexer/commit/a32f8f9e8ce1cc933c39ea1578aea77f4e181673))
* clean up imports ([#499](https://github.com/propeller-heads/tycho-indexer/issues/499)) ([0a8971b](https://github.com/propeller-heads/tycho-indexer/commit/0a8971ba9b7b566247431f8bd0c9c087ac2f4161))
* fix protocol system endpoint conversion to POST ([9f8d11b](https://github.com/propeller-heads/tycho-indexer/commit/9f8d11bdd2aa76ed4a7869d966142a110cb804f3))
* fix protocol system endpoint conversion to POST ([#498](https://github.com/propeller-heads/tycho-indexer/issues/498)) ([e559128](https://github.com/propeller-heads/tycho-indexer/commit/e5591282340b408c1d23a13b4d4cebf5b3b64115))

## [0.55.0](https://github.com/propeller-heads/tycho-indexer/compare/0.54.0...0.55.0) (2025-01-29)


### Features

* make protocol system a POST endpoint ([d85b2a4](https://github.com/propeller-heads/tycho-indexer/commit/d85b2a44166c376808e634c13839d2a95b4705fa))
* make protocol system a POST endpoint ([#496](https://github.com/propeller-heads/tycho-indexer/issues/496)) ([aad6b51](https://github.com/propeller-heads/tycho-indexer/commit/aad6b5153203a502920b05b1fcad67c80e307ae3))

## [0.54.0](https://github.com/propeller-heads/tycho-indexer/compare/0.53.0...0.54.0) (2025-01-27)


### Features

* add AccountBalance model to tx aggregated model ([ee96ba2](https://github.com/propeller-heads/tycho-indexer/commit/ee96ba27233b1e6a114f2e7326c79d7f938fb416))
* Add AccountBalance to core models ([#493](https://github.com/propeller-heads/tycho-indexer/issues/493)) ([cb86d4e](https://github.com/propeller-heads/tycho-indexer/commit/cb86d4e0ffb905c4d51a0a3514296305b8d19d3b))
* update parsing of new protobuf message changes ([1c424c0](https://github.com/propeller-heads/tycho-indexer/commit/1c424c05606bf9f056b7849a64bce559b17d008f))

## [0.53.0](https://github.com/propeller-heads/tycho-indexer/compare/0.52.0...0.53.0) (2025-01-23)


### Features

* add account balances to protobuf messages ([349aa2c](https://github.com/propeller-heads/tycho-indexer/commit/349aa2c61e0766db72b18c4920588c3969c12de8))
* add AccountBalances to the protobuf messages ([#492](https://github.com/propeller-heads/tycho-indexer/issues/492)) ([08cb0c6](https://github.com/propeller-heads/tycho-indexer/commit/08cb0c600447851c91493d1e58e81ce1362d47da))


### Bug Fixes

* remove unnecessary tx field in ProtocolComponent ([43e6573](https://github.com/propeller-heads/tycho-indexer/commit/43e65737f05fb48f06051670882ffc379616b1dd))

## [0.52.0](https://github.com/propeller-heads/tycho-indexer/compare/0.51.0...0.52.0) (2025-01-22)


### Features

* Add Base to supported chains ([c0afd1a](https://github.com/propeller-heads/tycho-indexer/commit/c0afd1ac5df5a3302e4a5153638d1c71889fa2be))
* Add Base to supported chains ([#491](https://github.com/propeller-heads/tycho-indexer/issues/491)) ([31447a8](https://github.com/propeller-heads/tycho-indexer/commit/31447a8ad3de358f330775e634bda326714cf2da))

## [0.51.0](https://github.com/propeller-heads/tycho-indexer/compare/0.50.0...0.51.0) (2025-01-20)


### Features

* update substreams client to accept compressed messages ([3a68674](https://github.com/propeller-heads/tycho-indexer/commit/3a6867418ba78a89568757a249dcd66fe2b758ff))
* update substreams client to accept compressed messages ([#489](https://github.com/propeller-heads/tycho-indexer/issues/489)) ([b441682](https://github.com/propeller-heads/tycho-indexer/commit/b4416824d66799d208812bd698ca12e16bba01cc))

## [0.50.0](https://github.com/propeller-heads/tycho-indexer/compare/0.49.2...0.50.0) (2025-01-17)


### Features

* add protocol system rpc ([#484](https://github.com/propeller-heads/tycho-indexer/issues/484)) ([9987d56](https://github.com/propeller-heads/tycho-indexer/commit/9987d5630c8be58173ca73a85549bf5f7c585644))
* add protocol_systems rpc ([9e44522](https://github.com/propeller-heads/tycho-indexer/commit/9e4452234a51a3ce83e76c82cfad0d430b0775ab))


### Bug Fixes

* add cache exist ([97f75a1](https://github.com/propeller-heads/tycho-indexer/commit/97f75a12cd1ff3b2a6cca8edf9b555d819192ac9))
* add sort before pagination ([c864f8f](https://github.com/propeller-heads/tycho-indexer/commit/c864f8f43669f778d01dca937d0e263918abf9ae))
* get method ([8540c47](https://github.com/propeller-heads/tycho-indexer/commit/8540c474e9b7a716cb4e2d61229d874d19fd51ad))

## [0.49.2](https://github.com/propeller-heads/tycho-indexer/compare/0.49.1...0.49.2) (2025-01-16)


### Bug Fixes

* add more db gateway 'get' tracing spans ([#487](https://github.com/propeller-heads/tycho-indexer/issues/487)) ([b11ee91](https://github.com/propeller-heads/tycho-indexer/commit/b11ee913628b0ae3053081b415e0c67472390df6))
* add more db gateway read spans ([958f357](https://github.com/propeller-heads/tycho-indexer/commit/958f357b7fd9ee8f4f59484133803c52ebbad3b5))

## [0.49.1](https://github.com/propeller-heads/tycho-indexer/compare/0.49.0...0.49.1) (2025-01-13)


### Bug Fixes

* map empty user identity to 'unknown' in metrics ([352016a](https://github.com/propeller-heads/tycho-indexer/commit/352016abbf6f9b8947af8a3a4baf1a0353a6ca33))
* map empty user identity to 'unknown' in metrics ([#485](https://github.com/propeller-heads/tycho-indexer/issues/485)) ([8d9fb62](https://github.com/propeller-heads/tycho-indexer/commit/8d9fb62c940f7b4d1c3c96e2d1dbb82242ef8f56))

## [0.49.0](https://github.com/propeller-heads/tycho-indexer/compare/0.48.0...0.49.0) (2025-01-10)


### Features

* add chain reorg metric ([b17b74f](https://github.com/propeller-heads/tycho-indexer/commit/b17b74f42b702d05d5283f010f7ebc0e5fe0766c))
* add substreams block message size metric ([5180275](https://github.com/propeller-heads/tycho-indexer/commit/5180275a876f29f06c9548d116cfc99980ac343a))
* extend substreams block message metrics ([#482](https://github.com/propeller-heads/tycho-indexer/issues/482)) ([b005e26](https://github.com/propeller-heads/tycho-indexer/commit/b005e26ec23b9ef318b324ac10301f04852d2be7))


### Bug Fixes

* add to and from block data to reorg metric ([44df826](https://github.com/propeller-heads/tycho-indexer/commit/44df826b5635864e1e6c067c7c8516e29fc94066))

## [0.48.0](https://github.com/propeller-heads/tycho-indexer/compare/0.47.0...0.48.0) (2025-01-10)


### Features

* bound end_idx to latest ([ea548ef](https://github.com/propeller-heads/tycho-indexer/commit/ea548ef21d78c3ace7c144223029f35b29f956ed))
* predefined behaviour for latest ts ([2ae6012](https://github.com/propeller-heads/tycho-indexer/commit/2ae60126cc71cd60a16d36e430e3b732a9b53542))
* revert changes and match current time ([81ad8e1](https://github.com/propeller-heads/tycho-indexer/commit/81ad8e19cfc654f8b21bd1817efaad34a1826fe7))
* revert previous changes && make version optional ([be15c0d](https://github.com/propeller-heads/tycho-indexer/commit/be15c0d8932219ca716644e1c4a685a02171401c))


### Bug Fixes

* delete all account balances for accounts to be deleted ([a3d9ffc](https://github.com/propeller-heads/tycho-indexer/commit/a3d9ffc0db659d8a074cb0e5c73f92fea9e1c72d))
* fix account balances bug ([c0b6678](https://github.com/propeller-heads/tycho-indexer/commit/c0b6678fb09786f0522a875cebae345e521ddf3c))
* fix protocol system deletion script account balances bug ([#477](https://github.com/propeller-heads/tycho-indexer/issues/477)) ([a749b53](https://github.com/propeller-heads/tycho-indexer/commit/a749b5305f23d4a66b177f6c89d6c269a62801e4))
* get_block_range ([6460d3d](https://github.com/propeller-heads/tycho-indexer/commit/6460d3d297a6667417f71f39fc066f897c0af762))
* test ([fe343d4](https://github.com/propeller-heads/tycho-indexer/commit/fe343d4e5e0396cb05c33a84de12fb3c1878a6ef))
* Update get_block_range to return full buffer when Ts is now or greater ([#470](https://github.com/propeller-heads/tycho-indexer/issues/470)) ([9507f32](https://github.com/propeller-heads/tycho-indexer/commit/9507f3280678be1b13f47feae7b90d523ef9d91e))

## [0.47.0](https://github.com/propeller-heads/tycho-indexer/compare/0.46.8...0.47.0) (2025-01-10)


### Features

* add user identity to ws connection metrics ([ac807b2](https://github.com/propeller-heads/tycho-indexer/commit/ac807b265f775aa27509cc3caa06274fddcda467))
* add user identity to ws connection metrics ([#480](https://github.com/propeller-heads/tycho-indexer/issues/480)) ([f2a3c8d](https://github.com/propeller-heads/tycho-indexer/commit/f2a3c8d1b0fbacdaea73277a60ca52a862b29d10))


### Bug Fixes

* default to 'unknown' if no user identity is present ([977f02e](https://github.com/propeller-heads/tycho-indexer/commit/977f02e6385ed283b0915ab020de6403a910f2df))

## [0.46.8](https://github.com/propeller-heads/tycho-indexer/compare/0.46.7...0.46.8) (2025-01-10)

## [0.46.7](https://github.com/propeller-heads/tycho-indexer/compare/0.46.6...0.46.7) (2025-01-09)


### Bug Fixes

* docker build ([68cb6a8](https://github.com/propeller-heads/tycho-indexer/commit/68cb6a8ef3abd146871ef5adaf8b91cfcf6e01bb)), closes [#474](https://github.com/propeller-heads/tycho-indexer/issues/474)
* docker build ([#475](https://github.com/propeller-heads/tycho-indexer/issues/475)) ([3569ee3](https://github.com/propeller-heads/tycho-indexer/commit/3569ee3ed22e25a8c78657ea09a87bf703451c58))

## [0.46.6](https://github.com/propeller-heads/tycho-indexer/compare/0.46.5...0.46.6) (2025-01-07)


### Bug Fixes

* changed validate pr condition ([c2913e6](https://github.com/propeller-heads/tycho-indexer/commit/c2913e618aba3000f4f5be0f1ae2b9a5f017b3a3))
* changed validate pr condition ([#476](https://github.com/propeller-heads/tycho-indexer/issues/476)) ([7bb6e84](https://github.com/propeller-heads/tycho-indexer/commit/7bb6e84bb31b63330d471265e6826eb9f8b5193f))
* try to run validate pr ([e646ed8](https://github.com/propeller-heads/tycho-indexer/commit/e646ed882662eb352c109f1391dda556591303ba))
* try to run validate pr ([4da002f](https://github.com/propeller-heads/tycho-indexer/commit/4da002f687cc1548f2cbf024e40c2270dc2bb4a5))
* try to run validate pr ([e85317d](https://github.com/propeller-heads/tycho-indexer/commit/e85317df16a7dcdf49847817ae7f89e9e250e2b5))

## [0.46.5](https://github.com/propeller-heads/tycho-indexer/compare/0.46.4...0.46.5) (2024-12-20)


### Bug Fixes

* switch block_processing_time metric to a gauge ([6ce8fd0](https://github.com/propeller-heads/tycho-indexer/commit/6ce8fd08f314862724a2a3aa805341cf02c5d88d))
* switch block_processing_time metric to a gauge ([#466](https://github.com/propeller-heads/tycho-indexer/issues/466)) ([8f2f584](https://github.com/propeller-heads/tycho-indexer/commit/8f2f5848d4dccf488557f9f55db0adb1baee9faa))

## [0.46.4](https://github.com/propeller-heads/tycho-indexer/compare/0.46.3...0.46.4) (2024-12-19)


### Bug Fixes

* calculate substream lag in millis ([5f3d503](https://github.com/propeller-heads/tycho-indexer/commit/5f3d503141f0b89c3c1ebae0f8c5f734b2b2770c))
* calculate substream lag in millis ([#465](https://github.com/propeller-heads/tycho-indexer/issues/465)) ([07916ff](https://github.com/propeller-heads/tycho-indexer/commit/07916fffbd61f6d68eaf3609762a41fa8b5c5057))

## [0.46.3](https://github.com/propeller-heads/tycho-indexer/compare/0.46.2...0.46.3) (2024-12-18)


### Bug Fixes

* rpc_requests metrics typo ([5cc490c](https://github.com/propeller-heads/tycho-indexer/commit/5cc490c6f6283c0c5b40bd54e17f8d5f290de3e2))
* split chain and extractor metric labels ([f15f2e2](https://github.com/propeller-heads/tycho-indexer/commit/f15f2e2ffa17cd4f34a4a8493bdb6e8892a7789e))
* split chain and extractor metric labels ([#463](https://github.com/propeller-heads/tycho-indexer/issues/463)) ([b98a5db](https://github.com/propeller-heads/tycho-indexer/commit/b98a5dbc74cbf76cfae8eae3d8b93a9c2c36d73a))

## [0.46.2](https://github.com/propeller-heads/tycho-indexer/compare/0.46.1...0.46.2) (2024-12-18)


### Bug Fixes

* decrement active subscription on ws connection close ([329fedd](https://github.com/propeller-heads/tycho-indexer/commit/329fedda025f42d6c65da931d4a9f0af1cc7a94c))
* decrement active subscription on ws connection close ([#462](https://github.com/propeller-heads/tycho-indexer/issues/462)) ([585ec90](https://github.com/propeller-heads/tycho-indexer/commit/585ec90fd6a62166a379e5a5a7a9e2917fd11a44))

## [0.46.1](https://github.com/propeller-heads/tycho-indexer/compare/0.46.0...0.46.1) (2024-12-17)


### Bug Fixes

* improve websocket metrics with extended metadata ([51c70eb](https://github.com/propeller-heads/tycho-indexer/commit/51c70eb54c8be6500f67335a393b89b60e9f9124))
* improve websocket metrics with extended metadata ([#460](https://github.com/propeller-heads/tycho-indexer/issues/460)) ([b8b3e1e](https://github.com/propeller-heads/tycho-indexer/commit/b8b3e1e8d5a9b966a830aed6dab18bfea72ba318))
* remove api key metric metadata ([4c12f56](https://github.com/propeller-heads/tycho-indexer/commit/4c12f5600f2d3cdbd185b8ced5274940c9434fc3))

## [0.46.0](https://github.com/propeller-heads/tycho-indexer/compare/0.45.2...0.46.0) (2024-12-16)


### Features

* **tycho-client:** increase pagination chunksize to 100 ([6c9b2da](https://github.com/propeller-heads/tycho-indexer/commit/6c9b2dac16b1f1ba999dc3028c480c908065037c))
* **tycho-client:** increase pagination chunksize to 100 ([#459](https://github.com/propeller-heads/tycho-indexer/issues/459)) ([c8b7ac6](https://github.com/propeller-heads/tycho-indexer/commit/c8b7ac6c6d30f484d03fffb6bcde29a7a4e41b6b))

## [0.45.2](https://github.com/propeller-heads/tycho-indexer/compare/0.45.1...0.45.2) (2024-12-13)


### Bug Fixes

* add extractor tag to block processing time metric ([d055d29](https://github.com/propeller-heads/tycho-indexer/commit/d055d295d0134e94d28bde725183f1f97686b360))
* add extractor tag to block processing time metric ([#458](https://github.com/propeller-heads/tycho-indexer/issues/458)) ([8305784](https://github.com/propeller-heads/tycho-indexer/commit/830578471caed1462a4e34148ade8680f00b569c))

## [0.45.1](https://github.com/propeller-heads/tycho-indexer/compare/0.45.0...0.45.1) (2024-12-12)


### Bug Fixes

* update SQL script to prune `transaction` table ([e43094a](https://github.com/propeller-heads/tycho-indexer/commit/e43094a529ea41c3a739ad458599bfc7eb627755))
* update SQL script to prune `transaction` table ([#455](https://github.com/propeller-heads/tycho-indexer/issues/455)) ([34ec325](https://github.com/propeller-heads/tycho-indexer/commit/34ec32557a5647bc828015e0813b6297ef9b4dc0))

## [0.45.0](https://github.com/propeller-heads/tycho-indexer/compare/0.44.0...0.45.0) (2024-12-12)


### Features

* add active websocket connections metric ([94f0a44](https://github.com/propeller-heads/tycho-indexer/commit/94f0a44c79a805c2075c30a219c3549811452ea4))
* add block processing time metric ([dc83c5e](https://github.com/propeller-heads/tycho-indexer/commit/dc83c5ea279c7d0140c65ba8404dcda60b9268a3))
* add dropped websocket connections metric ([5349418](https://github.com/propeller-heads/tycho-indexer/commit/53494184d16647c16fb6295e71fa6a45349ab57b))
* add metric for extractors current block ([ce89671](https://github.com/propeller-heads/tycho-indexer/commit/ce896718605054fc1073f1c4281c8b41d717b51b))
* add remaining sync time metric ([49de728](https://github.com/propeller-heads/tycho-indexer/commit/49de7281ae4f686b5332fdeaf32d73efc728c6e1))
* add RPC cache hits and misses count metrics ([10cef88](https://github.com/propeller-heads/tycho-indexer/commit/10cef881e903a1fb233df9dca0dca00fd370010a))
* add RPC failed requests count metric ([1dd9f95](https://github.com/propeller-heads/tycho-indexer/commit/1dd9f950ea7e8982f6b852f8f21597b106d04049))
* add RPC requests count metric ([571af0f](https://github.com/propeller-heads/tycho-indexer/commit/571af0fae7753abdf04eb1ca39f731224dff837d))
* add substream failure metrics ([258acb9](https://github.com/propeller-heads/tycho-indexer/commit/258acb90b1f5cfedc5f908ed7b526846544c72e1))
* add substreams lag metric ([485ea9c](https://github.com/propeller-heads/tycho-indexer/commit/485ea9cbf5d6b1d9508560ad33e072b2136d620f))
* add tycho-indexer metrics  ([#454](https://github.com/propeller-heads/tycho-indexer/issues/454)) ([13f780f](https://github.com/propeller-heads/tycho-indexer/commit/13f780fc4a5af6e5d0aa33987b94104c8f816044))


### Bug Fixes

* improve metric naming ([ddfedab](https://github.com/propeller-heads/tycho-indexer/commit/ddfedabdf85522b99cb8b9f42bc66001a5e1afef))
* improve substream metric labels ([3bfbf37](https://github.com/propeller-heads/tycho-indexer/commit/3bfbf378ce7f1feeed5dcee8601514b730d1a28e))

## [0.44.0](https://github.com/propeller-heads/tycho-indexer/compare/0.43.0...0.44.0) (2024-12-06)


### Features

* add metrics exporter and expose /metrics endpoint ([ff247c7](https://github.com/propeller-heads/tycho-indexer/commit/ff247c7a1b3e01c347bc66899be154b8143d4cfc))
* set up metrics exporter ([#453](https://github.com/propeller-heads/tycho-indexer/issues/453)) ([c426cd3](https://github.com/propeller-heads/tycho-indexer/commit/c426cd3e7588e62286d9cfc86c2d2e55204ff6fa))

## [0.43.0](https://github.com/propeller-heads/tycho-indexer/compare/0.42.3...0.43.0) (2024-11-29)


### Features

* Allow FeedMsg to be deserialized. ([f8d7655](https://github.com/propeller-heads/tycho-indexer/commit/f8d765554194ddd222e4c6f07811e8c99700615a))
* Allow FeedMsg to be deserialized. ([#451](https://github.com/propeller-heads/tycho-indexer/issues/451)) ([5d22803](https://github.com/propeller-heads/tycho-indexer/commit/5d228037843eb71555bb4478ca17a47e0ab996b7))

## [0.42.3](https://github.com/propeller-heads/tycho-indexer/compare/0.42.2...0.42.3) (2024-11-26)


### Bug Fixes

* **client:** remove hardcoded tycho host url ([2d9b1e1](https://github.com/propeller-heads/tycho-indexer/commit/2d9b1e1cda595c4a1329fcdd478bd2e57d77a260))
* **client:** remove hardcoded Tycho host url ([#449](https://github.com/propeller-heads/tycho-indexer/issues/449)) ([0181a1e](https://github.com/propeller-heads/tycho-indexer/commit/0181a1ef8c76a747e2192443525feb91973b155d))

## [0.42.2](https://github.com/propeller-heads/tycho-indexer/compare/0.42.1...0.42.2) (2024-11-25)


### Bug Fixes

* **rpc:** add buffer lookup for version given as block hash ([8fd6a86](https://github.com/propeller-heads/tycho-indexer/commit/8fd6a86eef8a328ef2ea625d71144e27af1529c9))
* **rpc:** add buffer lookup for version given as block hash ([#435](https://github.com/propeller-heads/tycho-indexer/issues/435)) ([a9672ad](https://github.com/propeller-heads/tycho-indexer/commit/a9672ad0e92dcc5af2f04c452e48e9007088a572))

## [0.42.1](https://github.com/propeller-heads/tycho-indexer/compare/0.42.0...0.42.1) (2024-11-20)


### Bug Fixes

* fix token analysis cronjob not setting quality for good tokens ([e0470dd](https://github.com/propeller-heads/tycho-indexer/commit/e0470dd0a97ea209d6789822ed80879e4311df6d))

## [0.42.0](https://github.com/propeller-heads/tycho-indexer/compare/0.41.1...0.42.0) (2024-11-19)


### Features

* **hex_bytes:** change hex bytes conversions to big endian ([3961824](https://github.com/propeller-heads/tycho-indexer/commit/39618244c4bbdc90d09af3af740edc34e6e68f76))
* **hex_bytes:** change hex bytes conversions to big endian ([#429](https://github.com/propeller-heads/tycho-indexer/issues/429)) ([e88a4c6](https://github.com/propeller-heads/tycho-indexer/commit/e88a4c67a9865f9505cf9827bb64e24e9cd73845))
* **tycho-ethereum:** update ether <-> bytes conversions to big endian ([3943560](https://github.com/propeller-heads/tycho-indexer/commit/39435606aaf0e92f9d5051a5df17617a0c7a075e))


### Bug Fixes

* make ethcontract optional ([2482ecc](https://github.com/propeller-heads/tycho-indexer/commit/2482ecc0592c1ef2592252543d00759fe22c11fc))

## [0.41.1](https://github.com/propeller-heads/tycho-indexer/compare/0.41.0...0.41.1) (2024-11-10)


### Bug Fixes

* fix formatting ([02f4d59](https://github.com/propeller-heads/tycho-indexer/commit/02f4d59ab7515a154110030eea97d956b8fcda47))
* fix token preprocessor symbol length to 255 chars ([0af6caa](https://github.com/propeller-heads/tycho-indexer/commit/0af6caa6c559222c12cc23f420c01dd5989a6a6c))
* fix token preprocessor symbol length to 255 chars ([#433](https://github.com/propeller-heads/tycho-indexer/issues/433)) ([466e620](https://github.com/propeller-heads/tycho-indexer/commit/466e6202e2409a47d0585dcbd535b63c23574e4b))
* **indexer:** correctly truncate token symbol ([9d7cd61](https://github.com/propeller-heads/tycho-indexer/commit/9d7cd6126c6e5dead0fa544a08c6d86807730ac8))

## [0.41.0](https://github.com/propeller-heads/tycho-indexer/compare/0.40.0...0.41.0) (2024-11-04)


### Features

* **tycho-client:** return the tokio handle from the stream builder ([06a669e](https://github.com/propeller-heads/tycho-indexer/commit/06a669e59ef2ec05c3aeb9a60b571a44ccf6e5ec))
* **tycho-client:** return the tokio handle from the stream builder ([#441](https://github.com/propeller-heads/tycho-indexer/issues/441)) ([173e774](https://github.com/propeller-heads/tycho-indexer/commit/173e774bd3726df5c93a938eafa8d1762a363250))

## [0.40.0](https://github.com/propeller-heads/tycho-indexer/compare/0.39.0...0.40.0) (2024-11-04)


### Features

* **tycho-client:** create rust client builder ([21d11a1](https://github.com/propeller-heads/tycho-indexer/commit/21d11a1a8dae0d79d675350902bbeaad60fa09a4))
* **tycho-client:** implement a rust client stream builder ([#439](https://github.com/propeller-heads/tycho-indexer/issues/439)) ([20be73c](https://github.com/propeller-heads/tycho-indexer/commit/20be73cdc4391805e2acf405ccf2fb5191dec3b7))
* **tycho-client:** improve error handling on TychoStreamBuilder ([b0a175c](https://github.com/propeller-heads/tycho-indexer/commit/b0a175ce0b4b484eecf85e12330f75925c3fa717))


### Bug Fixes

* **tycho-client:** do not error if no auth key is provided with tsl active ([19ac0d1](https://github.com/propeller-heads/tycho-indexer/commit/19ac0d12b1ed974b5ccb95cf2f62eca0f3b647ed))
* **tycho-client:** support fetching auth token from env var ([c1c03aa](https://github.com/propeller-heads/tycho-indexer/commit/c1c03aaa0a74b9170b57e70cf8e05fc9a8e79573))

## [0.39.0](https://github.com/propeller-heads/tycho-indexer/compare/0.38.0...0.39.0) (2024-11-02)


### Features

* **indexer:** expose s3 bucket as cli arg ([a55e126](https://github.com/propeller-heads/tycho-indexer/commit/a55e1265af2bf5b5bdc6c284c49128fc2590ae2f))
* **indexer:** parse s3 bucket from env variable ([c67fc38](https://github.com/propeller-heads/tycho-indexer/commit/c67fc3812007edc644b86efc45fa499aa098b2a9))
* **indexer:** parse s3 bucket from env variable ([#440](https://github.com/propeller-heads/tycho-indexer/issues/440)) ([104c4e9](https://github.com/propeller-heads/tycho-indexer/commit/104c4e90cf18a81dfe40ea5ea71b57f9be607691))

## [0.38.0](https://github.com/propeller-heads/tycho-indexer/compare/0.37.0...0.38.0) (2024-10-31)


### Features

* **rpc:** mark chain field in version param as deprecated ([8c00bde](https://github.com/propeller-heads/tycho-indexer/commit/8c00bdeb75edac5939f5eff415639cba7dd0d420))
* **rpc:** remove chain param from individual protocol ids ([4386dd0](https://github.com/propeller-heads/tycho-indexer/commit/4386dd0993d94484b39e861c989b559dd1f82ee0))
* **rpc:** remove chain param from individual protocol ids ([#437](https://github.com/propeller-heads/tycho-indexer/issues/437)) ([8c10d6a](https://github.com/propeller-heads/tycho-indexer/commit/8c10d6a9f8960f2d0e6d3bba50d1f82a41e935f2))
* **tycho-client:** update state endpoint body ([c535f79](https://github.com/propeller-heads/tycho-indexer/commit/c535f79a35bad00ca2443a1d643a9a344bb44118))

## [0.37.0](https://github.com/propeller-heads/tycho-indexer/compare/0.36.0...0.37.0) (2024-10-30)


### Features

* **storage:** update protocol state fetch query to apply all given filters ([1f64df2](https://github.com/propeller-heads/tycho-indexer/commit/1f64df222e7b258571e4b176dc779321f8ca504f))
* **storage:** update protocol state fetch query to apply all given filters ([#432](https://github.com/propeller-heads/tycho-indexer/issues/432)) ([5cb824e](https://github.com/propeller-heads/tycho-indexer/commit/5cb824e13c1cb4c86b290162cdeca6200307bb9f))

## [0.36.0](https://github.com/propeller-heads/tycho-indexer/compare/0.35.3...0.36.0) (2024-10-30)


### Features

* **scripts:** add balance check in uniswapv3 validation script ([25dc808](https://github.com/propeller-heads/tycho-indexer/commit/25dc8082d7223d89e4de12a539612d007f66fb5f))
* **scripts:** update uniswapv3 check script ([e1a1ce0](https://github.com/propeller-heads/tycho-indexer/commit/e1a1ce0f7351b29069d2f5b1bf9b9be2f3075012))

## [0.35.3](https://github.com/propeller-heads/tycho-indexer/compare/0.35.2...0.35.3) (2024-10-25)


### Bug Fixes

* **indexer:** correctly handle attributes deletions ([57fad94](https://github.com/propeller-heads/tycho-indexer/commit/57fad947c172ad0b5ecf9078a32580c204df679d))
* **indexer:** correctly handle attributes deletions ([#420](https://github.com/propeller-heads/tycho-indexer/issues/420)) ([faa08f6](https://github.com/propeller-heads/tycho-indexer/commit/faa08f645fc8a63ad2ed1fcd8689f586f96af1b5))

## [0.35.2](https://github.com/propeller-heads/tycho-indexer/compare/0.35.1...0.35.2) (2024-10-25)


### Bug Fixes

* pacakge release workflow ([5881d64](https://github.com/propeller-heads/tycho-indexer/commit/5881d641467325f26a164c73dd7cd64cbb344135))
* pacakge release workflow ([#431](https://github.com/propeller-heads/tycho-indexer/issues/431)) ([7b329a3](https://github.com/propeller-heads/tycho-indexer/commit/7b329a3f90fda2b1571617adcad83e8b7c36d39e))

## [0.35.1](https://github.com/propeller-heads/tycho-indexer/compare/0.35.0...0.35.1) (2024-10-24)


### Bug Fixes

* Fix ProtocolState RPC pagination by pre-paginating IDs ([fa485d2](https://github.com/propeller-heads/tycho-indexer/commit/fa485d25825df645380ea5d155bae010006f5ff4))
* Fix ProtocolState RPC pagination by pre-paginating IDs ([#425](https://github.com/propeller-heads/tycho-indexer/issues/425)) ([64ee5ce](https://github.com/propeller-heads/tycho-indexer/commit/64ee5ce22a56445b1bf7f970e96e78206b57f40d))
* remove unnecessary clone ([287d57f](https://github.com/propeller-heads/tycho-indexer/commit/287d57f9565bf2da4dcc80c406da6d9f845b0867))
* return total components when no id is specified for protocol_states ([8a78cb9](https://github.com/propeller-heads/tycho-indexer/commit/8a78cb9d6a7825afd66f4fa6b15483d8ce3ea771))

## [0.35.0](https://github.com/propeller-heads/tycho-indexer/compare/0.34.1...0.35.0) (2024-10-24)


### Features

* **ci:** Build wheels for python client. ([b882252](https://github.com/propeller-heads/tycho-indexer/commit/b8822526aa73f5643f5cf821ce1f64febc07605a))
* Ship tycho-client-py with binaries ([#427](https://github.com/propeller-heads/tycho-indexer/issues/427)) ([6e55465](https://github.com/propeller-heads/tycho-indexer/commit/6e55465fc8cc6a5f636016b7d45f19310b0c5ea8))
* **tycho-client:** Distribute binary with python lib. ([1540a4a](https://github.com/propeller-heads/tycho-indexer/commit/1540a4a9fb495746f479d4ce1ed5fd9477ae8556))

## [0.34.1](https://github.com/propeller-heads/tycho-indexer/compare/0.34.0...0.34.1) (2024-10-23)


### Bug Fixes

* **rpc:** correctly pass down delta buffer in RPC ([a1f35d8](https://github.com/propeller-heads/tycho-indexer/commit/a1f35d8bd4871ec01386155c894db8d74ff3f180))
* **rpc:** correctly pass down delta buffer in RPC ([#428](https://github.com/propeller-heads/tycho-indexer/issues/428)) ([de328a0](https://github.com/propeller-heads/tycho-indexer/commit/de328a04b529331586d40363e2aa1f4b68f79bbe))

## [0.34.0](https://github.com/propeller-heads/tycho-indexer/compare/0.33.1...0.34.0) (2024-10-22)


### Features

* **indexer:** introduce configurable post processors ([a9b9f2c](https://github.com/propeller-heads/tycho-indexer/commit/a9b9f2ced4f79cd95431a02a9c32dd6234a7dcc4))
* **indexer:** introduce configurable post processors ([#423](https://github.com/propeller-heads/tycho-indexer/issues/423)) ([4627cb4](https://github.com/propeller-heads/tycho-indexer/commit/4627cb430d3a80773e474e29ced4491d4c4e1eae))


### Bug Fixes

* correctly propagate missing post processor error ([5f655ae](https://github.com/propeller-heads/tycho-indexer/commit/5f655aee05c3f7c5cf2fe9c55d30e7c5c495da92))

## [0.33.1](https://github.com/propeller-heads/tycho-indexer/compare/0.33.0...0.33.1) (2024-10-22)


### Bug Fixes

* added secrets for build and push ([#421](https://github.com/propeller-heads/tycho-indexer/issues/421)) ([6567024](https://github.com/propeller-heads/tycho-indexer/commit/656702412ea34b5527c0505cb845dc85b2bd6ddf))
* **rpc:** allow to run `RpcHandler` without pending deltas. ([9ebb629](https://github.com/propeller-heads/tycho-indexer/commit/9ebb6296851d32c3d213bd7772bb60bf7bff0801))
* **rpc:** correctly handle requests with no ids specified ([#412](https://github.com/propeller-heads/tycho-indexer/issues/412)) ([8c04f17](https://github.com/propeller-heads/tycho-indexer/commit/8c04f171031b101b9b409372cc22ad7251676ad4))
* **rpc:** correctly handle when no ids are requested ([5470bf1](https://github.com/propeller-heads/tycho-indexer/commit/5470bf1600984826f0c1f7495ab20ec42121d7ae))
* **rpc:** fix running RPC without extractors ([#411](https://github.com/propeller-heads/tycho-indexer/issues/411)) ([c5b05cb](https://github.com/propeller-heads/tycho-indexer/commit/c5b05cbf6097c83c2ea90ce472ea880329f6d3ea))

## [0.33.0](https://github.com/propeller-heads/tycho-indexer/compare/0.32.0...0.33.0) (2024-10-11)


### Features

* **rpc:** return custom message for RPC error ([02daedb](https://github.com/propeller-heads/tycho-indexer/commit/02daedb1a8fc66d7d0847eaa81f589f020a77881))
* **rpc:** return custom message for RPC error ([#414](https://github.com/propeller-heads/tycho-indexer/issues/414)) ([6a51645](https://github.com/propeller-heads/tycho-indexer/commit/6a51645e986c14b1d9a9ec00dcbae3dd2cca746d))

## [0.32.0](https://github.com/propeller-heads/tycho-indexer/compare/0.31.3...0.32.0) (2024-10-09)


### Features

* **tycho-client:** publicly expose snapshot vm storage ([55e7875](https://github.com/propeller-heads/tycho-indexer/commit/55e78752627dc1ee36f45eeaf7f56797b704f2ee))
* **tycho-client:** publicly expose snapshot vm storage ([#413](https://github.com/propeller-heads/tycho-indexer/issues/413)) ([ca2a3e7](https://github.com/propeller-heads/tycho-indexer/commit/ca2a3e7aef90a4aeee5c4b5d96741fc0aeb6db50))

## [0.31.3](https://github.com/propeller-heads/tycho-indexer/compare/0.31.2...0.31.3) (2024-10-07)

## [0.31.2](https://github.com/propeller-heads/tycho-indexer/compare/0.31.1...0.31.2) (2024-10-07)


### Bug Fixes

* **substreams:** output type in Substreams modules ([7d62512](https://github.com/propeller-heads/tycho-indexer/commit/7d625128d5242f2e7b589bff19e863af06070580))
* **uniswap-v2-substreams:** use correct strucs in store pools module. ([16bbfc3](https://github.com/propeller-heads/tycho-indexer/commit/16bbfc3ce4805b8e5c6fce74c43d0e7f09eb266b))

## [0.31.1](https://github.com/propeller-heads/tycho-indexer/compare/0.31.0...0.31.1) (2024-10-07)


### Bug Fixes

* exit build_wheel script on failed internal command ([47c30c7](https://github.com/propeller-heads/tycho-indexer/commit/47c30c7c66d0d32b4a8a2922e8e9bf06a37acb11))

## [0.31.0](https://github.com/propeller-heads/tycho-indexer/compare/0.30.2...0.31.0) (2024-10-07)


### Features

* Add auth token support to tycho python client ([#406](https://github.com/propeller-heads/tycho-indexer/issues/406)) ([85376a6](https://github.com/propeller-heads/tycho-indexer/commit/85376a624da4d91eef52c584602727cf2a7bf44e))
* add auth token to tycho python client rpc ([1033802](https://github.com/propeller-heads/tycho-indexer/commit/1033802b1c852b87d0e9b0761ad08be79855a8ce))
* add auth token to tycho python client stream constructor ([d1f21bc](https://github.com/propeller-heads/tycho-indexer/commit/d1f21bc18af60e309f289b9f652a031a9d7c9f47))

## [0.30.2](https://github.com/propeller-heads/tycho-indexer/compare/0.30.1...0.30.2) (2024-10-07)


### Bug Fixes

* also cache component requests for specified components ([ce0f559](https://github.com/propeller-heads/tycho-indexer/commit/ce0f55937dc4eda18d5c74df0a2f6aa50d253ee3))
* Also cache component requests for specified components ([#404](https://github.com/propeller-heads/tycho-indexer/issues/404)) ([ff49333](https://github.com/propeller-heads/tycho-indexer/commit/ff49333416299b5b2c58976236baab08d29cee0e))

## [0.30.1](https://github.com/propeller-heads/tycho-indexer/compare/0.30.0...0.30.1) (2024-10-07)


### Bug Fixes

* **tycho-indexer:** correctly `buffered_range` to the span ([761eb72](https://github.com/propeller-heads/tycho-indexer/commit/761eb722a048dc93817e0db6f323d7e7cf5c1de7))
* **tycho-indexer:** correctly `buffered_range` to the span ([#405](https://github.com/propeller-heads/tycho-indexer/issues/405)) ([9adfcf7](https://github.com/propeller-heads/tycho-indexer/commit/9adfcf71f11dd4790c0b557772d60544a88b0fdf))

## [0.30.0](https://github.com/propeller-heads/tycho-indexer/compare/0.29.1...0.30.0) (2024-10-07)


### Features

* **tycho-indexer:** add span for `get_block_range` ([40d3c30](https://github.com/propeller-heads/tycho-indexer/commit/40d3c30b07f52719a64b7906a670e680af9cc8a8))
* **tycho-indexer:** add span for `get_block_range` ([#403](https://github.com/propeller-heads/tycho-indexer/issues/403)) ([a3df5b4](https://github.com/propeller-heads/tycho-indexer/commit/a3df5b416623dc1329375cbe1e4d03c6e7250375))

## [0.29.1](https://github.com/propeller-heads/tycho-indexer/compare/0.29.0...0.29.1) (2024-10-07)


### Bug Fixes

* increase component cache capacity ([4c2c611](https://github.com/propeller-heads/tycho-indexer/commit/4c2c611cbe4176b0d8797ad57a6b19587c5e16d4))
* increase component cache capacity ([#402](https://github.com/propeller-heads/tycho-indexer/issues/402)) ([501afbc](https://github.com/propeller-heads/tycho-indexer/commit/501afbc9303ff9e06af7beb222a561b8c5d8a16c))

## [0.29.0](https://github.com/propeller-heads/tycho-indexer/compare/0.28.0...0.29.0) (2024-10-04)


### Features

* **rpc:** add events in delta buffer ([9047b7c](https://github.com/propeller-heads/tycho-indexer/commit/9047b7ce473478ee6cad50a584bcc0c96b972729))
* **rpc:** add events in delta buffer ([#398](https://github.com/propeller-heads/tycho-indexer/issues/398)) ([8bbe273](https://github.com/propeller-heads/tycho-indexer/commit/8bbe273697b6f760db645a7e3695b0eb55ca512b))

## [0.28.0](https://github.com/propeller-heads/tycho-indexer/compare/0.27.0...0.28.0) (2024-10-04)


### Features

* improve rpc spans ([#397](https://github.com/propeller-heads/tycho-indexer/issues/397)) ([296b71c](https://github.com/propeller-heads/tycho-indexer/commit/296b71c09f37222aed7b54756e7098c0af212099))
* **rpc:** Add pagination and protocol attributes to rpc spans. ([5119d44](https://github.com/propeller-heads/tycho-indexer/commit/5119d446417612684547d9bbc7d90378effca0f3))
* **rpc:** Improve cache tracing spans. ([9241119](https://github.com/propeller-heads/tycho-indexer/commit/9241119353bb4c6c50aa65d8088002a32fc75afd))

## [0.27.0](https://github.com/propeller-heads/tycho-indexer/compare/0.26.0...0.27.0) (2024-10-03)


### Features

* **rpc:** Implement per-key sharded locking in RpcCache ([ef68ca2](https://github.com/propeller-heads/tycho-indexer/commit/ef68ca2491e5fb7c1be58148b8b7e19bb092100b))
* **rpc:** Implement per-key sharded locking in RpcCache ([#396](https://github.com/propeller-heads/tycho-indexer/issues/396)) ([f0337bf](https://github.com/propeller-heads/tycho-indexer/commit/f0337bf2fb01aab478eb7f41c47c9d3f8718b8f6))

## [0.26.0](https://github.com/propeller-heads/tycho-indexer/compare/0.25.1...0.26.0) (2024-10-03)


### Features

* **client:** Ensure StateSynchronizer waits for initialization ([a378483](https://github.com/propeller-heads/tycho-indexer/commit/a378483a73b70d184f25e1093fe256ef0f383437))
* **client:** Ensure StateSynchronizer waits for initialization ([#393](https://github.com/propeller-heads/tycho-indexer/issues/393)) ([7852e43](https://github.com/propeller-heads/tycho-indexer/commit/7852e432f77a41dac6dc6dacba64e4bab9e68153))

## [0.25.1](https://github.com/propeller-heads/tycho-indexer/compare/0.25.0...0.25.1) (2024-10-03)


### Bug Fixes

* avoid concurrent requests for empty pages ([a881bba](https://github.com/propeller-heads/tycho-indexer/commit/a881bba3f780d86928d5b119acf9f973f2fb7513))
* avoid concurrent requests for empty pages ([#392](https://github.com/propeller-heads/tycho-indexer/issues/392)) ([681ae7e](https://github.com/propeller-heads/tycho-indexer/commit/681ae7e145403fc7e876d2216ff215f466ecd075))
* cache condition was reversed ([6f45a26](https://github.com/propeller-heads/tycho-indexer/commit/6f45a2639ca23c8f0447c8d263c02dde58e32f17))
* **client:** only apply concurrency once total is known ([82a65f1](https://github.com/propeller-heads/tycho-indexer/commit/82a65f13c5551ac6061fc38008c1c898e036789c))
* skip caching last page of components response ([7eff7bf](https://github.com/propeller-heads/tycho-indexer/commit/7eff7bfa91fc0229daaf86e158c82f2a7e9caddf))

## [0.25.0](https://github.com/propeller-heads/tycho-indexer/compare/0.24.1...0.25.0) (2024-10-03)


### Features

* **rpc:** add spans and event around the delta buffer and components query ([fe1a1f4](https://github.com/propeller-heads/tycho-indexer/commit/fe1a1f48e1a9d6ece1a540dc440847e71be5af54))
* **rpc:** add spans for cache, tokens and components requests ([e7112b0](https://github.com/propeller-heads/tycho-indexer/commit/e7112b0ebea8533b0ed9c3cc78741734e57a61e9))
* **rpc:** add spans for cache, tokens and components requests ([#394](https://github.com/propeller-heads/tycho-indexer/issues/394)) ([0c67a3d](https://github.com/propeller-heads/tycho-indexer/commit/0c67a3d329a19cb62953a31e9032d3da66bc2836))

## [0.24.1](https://github.com/propeller-heads/tycho-indexer/compare/0.24.0...0.24.1) (2024-10-02)


### Bug Fixes

* add order by to paginated queries ([b198f07](https://github.com/propeller-heads/tycho-indexer/commit/b198f07a3a05af4376c056b1925bbf80c38b63e6))
* add order by to paginated queries ([#391](https://github.com/propeller-heads/tycho-indexer/issues/391)) ([1db42b1](https://github.com/propeller-heads/tycho-indexer/commit/1db42b187daaee72d9a4be278b05270c523a6414))

## [0.24.0](https://github.com/propeller-heads/tycho-indexer/compare/0.23.1...0.24.0) (2024-10-02)


### Features

* Add component cache to rpc ([#390](https://github.com/propeller-heads/tycho-indexer/issues/390)) ([8dfc004](https://github.com/propeller-heads/tycho-indexer/commit/8dfc00430f4a8c0bb0cd4f152c640f91ef58b6b8))
* **rpc:** add component cache ([c7a1894](https://github.com/propeller-heads/tycho-indexer/commit/c7a189430df09aa1c8d517e0fe0a2468f0127cb9))


### Bug Fixes

* order components before pagination ([9eee0af](https://github.com/propeller-heads/tycho-indexer/commit/9eee0afd36706cae0c6a70679491d935664f6327))

## [0.23.1](https://github.com/propeller-heads/tycho-indexer/compare/0.23.0...0.23.1) (2024-10-02)


### Bug Fixes

* increase protocol component pagination page size ([2565c05](https://github.com/propeller-heads/tycho-indexer/commit/2565c051eb971aa697128c41a64a9e45913c1b13))
* increase protocol component pagination page size ([#389](https://github.com/propeller-heads/tycho-indexer/issues/389)) ([a90cad6](https://github.com/propeller-heads/tycho-indexer/commit/a90cad6998a357a259e115e4265b4759bc82699f))

## [0.23.0](https://github.com/propeller-heads/tycho-indexer/compare/0.22.5...0.23.0) (2024-10-02)


### Features

* **tycho-indexer:** make number of worker parametrable ([fc6e334](https://github.com/propeller-heads/tycho-indexer/commit/fc6e334b7b81d9e37076127038ff45c5fcb7518c))
* **tycho-indexer:** make number of worker parametrable ([#388](https://github.com/propeller-heads/tycho-indexer/issues/388)) ([f59397a](https://github.com/propeller-heads/tycho-indexer/commit/f59397a6ad0833ce67d2cc40e80a8746bad21b95))

## [0.22.5](https://github.com/propeller-heads/tycho-indexer/compare/0.22.4...0.22.5) (2024-10-01)


### Bug Fixes

* **otel:** create tracing subscriber inside the runtime ([65c3a02](https://github.com/propeller-heads/tycho-indexer/commit/65c3a02601b8b2cba53ecb8b70c905da2841f87a))
* Tokio runtime issue ([#387](https://github.com/propeller-heads/tycho-indexer/issues/387)) ([40ec9b2](https://github.com/propeller-heads/tycho-indexer/commit/40ec9b2ad29f7fe3f4ee98533fc5dac261b9d669))

## [0.22.4](https://github.com/propeller-heads/tycho-indexer/compare/0.22.3...0.22.4) (2024-10-01)

## [0.22.3](https://github.com/propeller-heads/tycho-indexer/compare/0.22.2...0.22.3) (2024-10-01)


### Bug Fixes

* **client:** populating python client error log bug ([#386](https://github.com/propeller-heads/tycho-indexer/issues/386)) ([87bcae5](https://github.com/propeller-heads/tycho-indexer/commit/87bcae5c37ef5c1d2a422ebbee53e64ed0d3d2e4))
* populating python client error log bug ([e6d9682](https://github.com/propeller-heads/tycho-indexer/commit/e6d9682704bae29e04d4dc4bac356866a4e42d1d))

## [0.22.2](https://github.com/propeller-heads/tycho-indexer/compare/0.22.1...0.22.2) (2024-10-01)


### Bug Fixes

* **client:** improve python client stream error logging ([#385](https://github.com/propeller-heads/tycho-indexer/issues/385)) ([5a243de](https://github.com/propeller-heads/tycho-indexer/commit/5a243dec32f9fda31cdb3cc3cf6d97e12897d63e))
* **client:** make python client error logs more readable ([baa1fa2](https://github.com/propeller-heads/tycho-indexer/commit/baa1fa20175d6a8b53b64aeec75e71c107d7889a))

## [0.22.1](https://github.com/propeller-heads/tycho-indexer/compare/0.22.0...0.22.1) (2024-09-30)


### Bug Fixes

* **client:** use new tokens endpoint ([02fb389](https://github.com/propeller-heads/tycho-indexer/commit/02fb389a648e46eb26199d2108c844934a6c9271))
* **client:** use new tokens endpoint ([#384](https://github.com/propeller-heads/tycho-indexer/issues/384)) ([5d568cd](https://github.com/propeller-heads/tycho-indexer/commit/5d568cd8bd55cac7fa787c0f3c8f68db09400d07))

## [0.22.0](https://github.com/propeller-heads/tycho-indexer/compare/0.21.0...0.22.0) (2024-09-30)


### Features

* **cache:** add tracing spans for every read methods ([be40f4c](https://github.com/propeller-heads/tycho-indexer/commit/be40f4cc1d2c5614d5ecd0a59ee4617edbfec894))
* **cache:** add tracing spans for every write methods on the 'CachedGateway' ([88ac98e](https://github.com/propeller-heads/tycho-indexer/commit/88ac98e61e895a6ac0c3fd3e566cb672c0761e87))
* **extractor:** improve database commits ([4a431df](https://github.com/propeller-heads/tycho-indexer/commit/4a431dfe3e88a3e642cf76455bc3e974dafa5cdd))
* **indexing:** Improve database commits logic ([#380](https://github.com/propeller-heads/tycho-indexer/issues/380)) ([55d40b9](https://github.com/propeller-heads/tycho-indexer/commit/55d40b9df1e315e77b4ea44647ba550412fb4582))

## [0.21.0](https://github.com/propeller-heads/tycho-indexer/compare/0.20.0...0.21.0) (2024-09-30)


### Features

* add method to get protocol components paginated ([7ce3cbe](https://github.com/propeller-heads/tycho-indexer/commit/7ce3cbe56305de3b821cf9b615718182997b3fc3))
* limit the page size for paginated endpoints ([26e8767](https://github.com/propeller-heads/tycho-indexer/commit/26e876767292a8d2751dfd651ff6f018d4c3fec5))
* more fixes ([29f1117](https://github.com/propeller-heads/tycho-indexer/commit/29f1117cb392b6b3cb113d0dd240ac0708828913))
* Return total count to pagination responses, get_contract_state ([d780212](https://github.com/propeller-heads/tycho-indexer/commit/d780212739025d6cda52b55fda82d9e04332857e))
* Return total count to pagination responses, get_protocol_components ([8115c9b](https://github.com/propeller-heads/tycho-indexer/commit/8115c9b41e7ff0e38c568cecb6ddeb5adcc1da1b))
* Return total count to pagination responses, get_protocol_state ([cfafb70](https://github.com/propeller-heads/tycho-indexer/commit/cfafb70c2ce38da01203c5f4a90d62c4815e2413))
* Return total count to pagination responses, get_tokens ([69ab6f7](https://github.com/propeller-heads/tycho-indexer/commit/69ab6f758f5be67c4f0007230ce43bbb5c2cf242))
* **rpc:** add pagination to all rpc endpoints ([39107f3](https://github.com/propeller-heads/tycho-indexer/commit/39107f36ffbd0c47609cb72977a67c3e68acf813))
* **rpc:** add pagination to all rpc endpoints ([#345](https://github.com/propeller-heads/tycho-indexer/issues/345)) ([f945d75](https://github.com/propeller-heads/tycho-indexer/commit/f945d753ed6da04a1b643e517db4949b981a9550))
* use pagination on rpc sync ([fd10192](https://github.com/propeller-heads/tycho-indexer/commit/fd1019207ab391c50f678afc1aefd35c2f2d269a))


### Bug Fixes

* bug with page and page_size swapped ([0aa2afd](https://github.com/propeller-heads/tycho-indexer/commit/0aa2afdd6e8079ee87422529d7c563d7a3891f87))
* correctly handle buffered components in pagination ([9f7bdbc](https://github.com/propeller-heads/tycho-indexer/commit/9f7bdbc01350627aebf0e5d2fdf49d856929c51f))
* correctly handle buffered contract states in pagination ([57209aa](https://github.com/propeller-heads/tycho-indexer/commit/57209aa9250790e6e126ff4c3a17be946dbfdb91))
* correctly handle buffered protocol states in pagination ([6afcc63](https://github.com/propeller-heads/tycho-indexer/commit/6afcc63bfd5c7779de8aa83787b80c3fe748f8a1))
* correctly pass state request ids chunk ([d5282cb](https://github.com/propeller-heads/tycho-indexer/commit/d5282cb9575c202b7353a63f20436c26bb01012c))
* fix pagination for contract_state ([22c8497](https://github.com/propeller-heads/tycho-indexer/commit/22c8497a0e9d1266102997f3bb44f617ac389977))
* fix pagination for contract_state by chain ([3e543e4](https://github.com/propeller-heads/tycho-indexer/commit/3e543e4fc99d1bb3518eee63e6e44a8a9e5d7639))
* fix pagination for fetching ProtocolState, add tests ([0ba95d9](https://github.com/propeller-heads/tycho-indexer/commit/0ba95d9bbbada13db9ccde5617e47383ab8f90a4))
* paginate contract_state using chunked ids ([87e702e](https://github.com/propeller-heads/tycho-indexer/commit/87e702e3ebcaff14633772ec210c9c276ebe6989))
* post rebase fixes, use Bytes instead of contractId ([ae57952](https://github.com/propeller-heads/tycho-indexer/commit/ae57952f6409923001222469eecf536617df4aef))
* rebased contract struct name change ([d04a519](https://github.com/propeller-heads/tycho-indexer/commit/d04a5192354dd5e14754bbaa2ed96bcc16e655a3))
* remove unnecessary filters ([c94be3f](https://github.com/propeller-heads/tycho-indexer/commit/c94be3fc650ce7e104b4d7221eabcbd588fac940))
* remove unnecessary uniqueness constraints ([7187f70](https://github.com/propeller-heads/tycho-indexer/commit/7187f70c8f42a09f178ef6b6bc91c543d9252661))
* undo formatting errors and typos ([dc68a61](https://github.com/propeller-heads/tycho-indexer/commit/dc68a6103e31eb3451a375f526f76bda531679b3))
* use total from pagination response to end pagination looping ([c763f96](https://github.com/propeller-heads/tycho-indexer/commit/c763f96c9cb599844178e3900b2954b84c4c1307))

## [0.20.0](https://github.com/propeller-heads/tycho-indexer/compare/0.19.0...0.20.0) (2024-09-26)


### Features

* **rpc:** Add a cache for contract storage. ([8e9c6d3](https://github.com/propeller-heads/tycho-indexer/commit/8e9c6d3c2c6a2ed5f3d54e572b93979b63e14817))
* **rpc:** Add a cache for protocol state. ([e020b08](https://github.com/propeller-heads/tycho-indexer/commit/e020b081f6102f97c40e3efea938af7e2aca81c5))
* **rpc:** Generalize RPC caching strategy. ([e4e4226](https://github.com/propeller-heads/tycho-indexer/commit/e4e4226b44a5c9dda881a4627831e4c10391f18c))
* **rpc:** Protocol state and contract storage rpc caching ([#378](https://github.com/propeller-heads/tycho-indexer/issues/378)) ([9cccd5d](https://github.com/propeller-heads/tycho-indexer/commit/9cccd5d0abf19d53d74ea07ad0ba6051f53a1d3c))

## [0.19.0](https://github.com/propeller-heads/tycho-indexer/compare/0.18.4...0.19.0) (2024-09-26)


### Features

* **tycho-client-py:** add no-tls flag to `TychoStream` ([68a184e](https://github.com/propeller-heads/tycho-indexer/commit/68a184e4f03654692444a93fa72bc22fed4757d8))
* **tycho-client:** add `no-tls` flag to allow using unsecured transports ([2f56780](https://github.com/propeller-heads/tycho-indexer/commit/2f5678025418ee0066d079d531b790d0ea1075d0))
* **tycho-client:** add `user-agent` to websocket connection requests ([008ab20](https://github.com/propeller-heads/tycho-indexer/commit/008ab20676b5021c5257988d076db547ffb9da5f))
* **tycho-client:** add auth key and support for https ([#379](https://github.com/propeller-heads/tycho-indexer/issues/379)) ([c37c9ad](https://github.com/propeller-heads/tycho-indexer/commit/c37c9adc21c81ee241cd1affe0e7e3425272f485))
* **tycho-client:** add Auth to websocket client ([e2e6ade](https://github.com/propeller-heads/tycho-indexer/commit/e2e6adefcb3e926111f3e0ab3fc75f1f603a7a5f))
* **tycho-client:** enable HTTPS and add auth key ([bbd0eee](https://github.com/propeller-heads/tycho-indexer/commit/bbd0eee1af932ba84e913f5e173e08a3f43010c6))
* **tycho-client:** get `auth-key` from env or cli ([37d02a0](https://github.com/propeller-heads/tycho-indexer/commit/37d02a0ce085eea1b32d7abc84800bdf0143937d))

## [0.18.4](https://github.com/propeller-heads/tycho-indexer/compare/0.18.3...0.18.4) (2024-09-26)


### Bug Fixes

* fix delete protocol script bug ([450e6c0](https://github.com/propeller-heads/tycho-indexer/commit/450e6c048337e40c65f40ef2eee0d91426f3611d))
* remove deleted attributes from default table ([aab9a76](https://github.com/propeller-heads/tycho-indexer/commit/aab9a76ecdd3d87379e5caff80ae587aa0dd9d53))
* Remove deleted attributes from default table ([#374](https://github.com/propeller-heads/tycho-indexer/issues/374)) ([a6b15b6](https://github.com/propeller-heads/tycho-indexer/commit/a6b15b63ca7e851300dde9a382ce66278c271ebc))
* skip deleted attributes delete query if no attr are deleted ([0e7bb6c](https://github.com/propeller-heads/tycho-indexer/commit/0e7bb6cb5c6bbf4d1bd913e9848f81c8034c90ee))

## [0.18.3](https://github.com/propeller-heads/tycho-indexer/compare/0.18.2...0.18.3) (2024-09-23)


### Bug Fixes

* add chain awareness to extraction state block migration ([ef92989](https://github.com/propeller-heads/tycho-indexer/commit/ef92989ee682003a6add6900a397f11cb0e7a9da))
* add chain awareness to extraction state block migration ([#361](https://github.com/propeller-heads/tycho-indexer/issues/361)) ([7a509e1](https://github.com/propeller-heads/tycho-indexer/commit/7a509e10a5d708141d801c65b4c608b0b7a94f40))

## [0.18.2](https://github.com/propeller-heads/tycho-indexer/compare/0.18.1...0.18.2) (2024-09-23)

## [0.18.1](https://github.com/propeller-heads/tycho-indexer/compare/0.18.0...0.18.1) (2024-09-20)

## [0.18.0](https://github.com/propeller-heads/tycho-indexer/compare/0.17.5...0.18.0) (2024-09-19)


### Features

* automate removal of orphaned transactions ([d0939a5](https://github.com/propeller-heads/tycho-indexer/commit/d0939a59bf2b1e05fe48ddec0d0a2af73980f79d))
* Automate removal of orphaned transactions from the DB ([#349](https://github.com/propeller-heads/tycho-indexer/issues/349)) ([898460d](https://github.com/propeller-heads/tycho-indexer/commit/898460dfaf10d40f2f1c714d7deec6ca9fc73ae5))


### Bug Fixes

* delete transactions in batches ([fd128fd](https://github.com/propeller-heads/tycho-indexer/commit/fd128fd2a8bd600a7ae21ca9f0842d0ac38125f9))
* improve transaction clean up script to minimise db locks ([fffe9cc](https://github.com/propeller-heads/tycho-indexer/commit/fffe9cc7d085a1f9633806ebe1664eb3d122600a))
* skip batching on search phase ([ceb5376](https://github.com/propeller-heads/tycho-indexer/commit/ceb5376ed4da853ae80f00410e7c442f59a1cd4d))
* speed up deletions with indexes ([ebea183](https://github.com/propeller-heads/tycho-indexer/commit/ebea1837a051bf36ac2252d6ecaadbc28beb0c23))

## [0.17.5](https://github.com/propeller-heads/tycho-indexer/compare/0.17.4...0.17.5) (2024-09-19)


### Bug Fixes

* fetch contracts from deltas buffer if not in db yet ([1321cbd](https://github.com/propeller-heads/tycho-indexer/commit/1321cbd0da60b732043bb570050d59d164a135d0))
* fetch contracts from deltas buffer if not in db yet ([#370](https://github.com/propeller-heads/tycho-indexer/issues/370)) ([b12538d](https://github.com/propeller-heads/tycho-indexer/commit/b12538d62f912641b81ee006662cdbbde6a7a0c3))
* rebase and fix subsequent changes ([6a2b3e3](https://github.com/propeller-heads/tycho-indexer/commit/6a2b3e32718af7f960d3d4388a519cd00c1ddd9e))

## [0.17.4](https://github.com/propeller-heads/tycho-indexer/compare/0.17.3...0.17.4) (2024-09-19)


### Bug Fixes

* adapt tycho-client-py to work with `Bytes` ([562e45a](https://github.com/propeller-heads/tycho-indexer/commit/562e45ac9d9b13aad358a3fc603b23fc9b42dc41))
* release config wrong crate name ([a024672](https://github.com/propeller-heads/tycho-indexer/commit/a024672489f6944349deba0b3a574135ae3223fd))
* release config wrong crate name ([#371](https://github.com/propeller-heads/tycho-indexer/issues/371)) ([452f88a](https://github.com/propeller-heads/tycho-indexer/commit/452f88a4637a0af7a15199bfe6339efc66d29dae))
* rename tycho analyzer in release config ([0a2a168](https://github.com/propeller-heads/tycho-indexer/commit/0a2a1683d5fdb250b0483a4f0427898fa10ce33b))

## [0.17.3](https://github.com/propeller-heads/tycho-indexer/compare/0.17.2...0.17.3) (2024-09-17)


### Bug Fixes

* protocol system delete script ([#365](https://github.com/propeller-heads/tycho-indexer/issues/365)) ([e3c3313](https://github.com/propeller-heads/tycho-indexer/commit/e3c3313afd76387c274fe39209ea6b6c0c978c1c))
* skip deleting accounts also linked to tokens used by other systems ([baefd07](https://github.com/propeller-heads/tycho-indexer/commit/baefd0713aeee1d45fa8edd0ee6c4b0b51c81c18))

## [0.17.2](https://github.com/propeller-heads/tycho-indexer/compare/0.17.1...0.17.2) (2024-09-16)

## [0.17.1](https://github.com/propeller-heads/tycho-indexer/compare/0.17.0...0.17.1) (2024-09-13)

## [0.17.0](https://github.com/propeller-heads/tycho-indexer/compare/0.16.4...0.17.0) (2024-09-11)


### Features

* expose `items()` directly on `TokenBalances` ([e5eb17e](https://github.com/propeller-heads/tycho-indexer/commit/e5eb17ec6b3c704574c07b97562d953942ff286f))

## [0.16.4](https://github.com/propeller-heads/tycho-indexer/compare/0.16.3...0.16.4) (2024-09-06)

## [0.16.3](https://github.com/propeller-heads/tycho-indexer/compare/0.16.2...0.16.3) (2024-09-06)


### Bug Fixes

* **rpc:** fi handling of default version ts ([9d60af2](https://github.com/propeller-heads/tycho-indexer/commit/9d60af2e3902a9817b5ad9cac91567a788ec9e24))
* **rpc:** Fix handling of default version ts ([#352](https://github.com/propeller-heads/tycho-indexer/issues/352)) ([2820a42](https://github.com/propeller-heads/tycho-indexer/commit/2820a42c8dd33f6d4d62816ceafad605ff493f8f))

## [0.16.2](https://github.com/propeller-heads/tycho-indexer/compare/0.16.1...0.16.2) (2024-09-06)


### Bug Fixes

* Improve protocol system deletion script ([#358](https://github.com/propeller-heads/tycho-indexer/issues/358)) ([6f20892](https://github.com/propeller-heads/tycho-indexer/commit/6f2089251a054d563785817adb7c46dbb8e5e82a))
* remove unnecessary queries from deletion script ([9c6f7a4](https://github.com/propeller-heads/tycho-indexer/commit/9c6f7a453879d5add16b5e00a8796fab590f4d95))

## [0.16.1](https://github.com/propeller-heads/tycho-indexer/compare/0.16.0...0.16.1) (2024-09-05)


### Bug Fixes

* Delete protocol system script to delete tokens as necessary ([#356](https://github.com/propeller-heads/tycho-indexer/issues/356)) ([3e1138b](https://github.com/propeller-heads/tycho-indexer/commit/3e1138ba25d5d8b6cb10c7b43c2cf99d0a9ee1df))
* delete token's account entries too ([0862e39](https://github.com/propeller-heads/tycho-indexer/commit/0862e39ca4165c3053f18d0b21a79c85e1789a3e))
* delete tokens that belong solely to the protocol system ([2289173](https://github.com/propeller-heads/tycho-indexer/commit/2289173bbe93f7bc7116647b5089b1f4bf617d24))
* remove unnecessary count check ([9b40290](https://github.com/propeller-heads/tycho-indexer/commit/9b40290f28d6dd3db322d3ce8319c1ed58d3d846))

## [0.16.0](https://github.com/propeller-heads/tycho-indexer/compare/0.15.2...0.16.0) (2024-09-04)


### Features

* Create remove protocol script ([#311](https://github.com/propeller-heads/tycho-indexer/issues/311)) ([b6b818b](https://github.com/propeller-heads/tycho-indexer/commit/b6b818b3809b05f54a2017f241ae45872b688ce4))
* **db:** add cascade deletes to protocol_system related tables ([f8326e2](https://github.com/propeller-heads/tycho-indexer/commit/f8326e27aab5a1b88fd85f4cc3aece5b12ba4271))
* **db:** add script to delete protocol system from db ([07ffa77](https://github.com/propeller-heads/tycho-indexer/commit/07ffa779c4d4f10fdb1625ecaedeaf298d3c8afa))
* skip deleting shared accounts ([37b17d2](https://github.com/propeller-heads/tycho-indexer/commit/37b17d2c19337b4bae2e05cc1e7aefa0d17ed48c))


### Bug Fixes

* delete substreams cursor too ([be9ebfa](https://github.com/propeller-heads/tycho-indexer/commit/be9ebfa5f0fee568abe089af878d276f3a5de542))
* typo in name of sushiswap configs ([3489aab](https://github.com/propeller-heads/tycho-indexer/commit/3489aab1374a8ca925a737c62d2f45da55899005))
* update protocol delete script to be more configurable ([a76e5db](https://github.com/propeller-heads/tycho-indexer/commit/a76e5db51f7b102c7e29a25fbf36af4633e9d68b))

## [0.15.2](https://github.com/propeller-heads/tycho-indexer/compare/0.15.1...0.15.2) (2024-09-04)


### Bug Fixes

* **tycho-client-py:** backward compatibility of `ContractStateParams` ([c5373a1](https://github.com/propeller-heads/tycho-indexer/commit/c5373a1cfb56a7ef8a9421424410e84a74809d46))
* **tycho-client-py:** backward compatibility of `ContractStateParams` ([#354](https://github.com/propeller-heads/tycho-indexer/issues/354)) ([81f0afc](https://github.com/propeller-heads/tycho-indexer/commit/81f0afcc0e898c7d156c53ebc91cffb2fa745290))

## [0.15.1](https://github.com/propeller-heads/tycho-indexer/compare/0.15.0...0.15.1) (2024-09-03)

## [0.15.0](https://github.com/propeller-heads/tycho-indexer/compare/0.14.0...0.15.0) (2024-09-02)


### Features

* Add block_id column to extraction_state table ([78514f5](https://github.com/propeller-heads/tycho-indexer/commit/78514f58607f851f7e29b0f4085f054189f07072))
* Add block_id column to extraction_state table ([#287](https://github.com/propeller-heads/tycho-indexer/issues/287)) ([ea7434a](https://github.com/propeller-heads/tycho-indexer/commit/ea7434a8cdf727654c8c03658f5552a2ac71cd63))
* add block_id to extraction_state db table ([e0c4f35](https://github.com/propeller-heads/tycho-indexer/commit/e0c4f350f1e0334f37f928d5eb09494223283ad1))


### Bug Fixes

* remove Block from get_state return ([563de75](https://github.com/propeller-heads/tycho-indexer/commit/563de758ce44e1a3e5e60cd63586a65e0a73e699))

## [0.14.0](https://github.com/propeller-heads/tycho-indexer/compare/0.13.0...0.14.0) (2024-09-02)


### Features

* Remove chain from contract id param ([#346](https://github.com/propeller-heads/tycho-indexer/issues/346)) ([3bb61a4](https://github.com/propeller-heads/tycho-indexer/commit/3bb61a49f695dad6e010e831a8e38a2e4d8defe9))
* **rpc:** remove chain from contract id param ([8092a1e](https://github.com/propeller-heads/tycho-indexer/commit/8092a1ef6e53edb3262bdd6d307c0efe78844c14))

## [0.13.0](https://github.com/propeller-heads/tycho-indexer/compare/0.12.0...0.13.0) (2024-08-30)


### Features

* add autodeletion to partition tables ([6302ae8](https://github.com/propeller-heads/tycho-indexer/commit/6302ae8c68368ca1af99c1aab939adfd993b24a7))
* Add autodeletion to partition tables ([#347](https://github.com/propeller-heads/tycho-indexer/issues/347)) ([e482522](https://github.com/propeller-heads/tycho-indexer/commit/e482522cda1ca7df8733c6a7bc41f486ca0c403c))

## [0.12.0](https://github.com/propeller-heads/tycho-indexer/compare/0.11.1...0.12.0) (2024-08-29)


### Features

* Move rpc endpoint params to request body ([#344](https://github.com/propeller-heads/tycho-indexer/issues/344)) ([c6ff178](https://github.com/propeller-heads/tycho-indexer/commit/c6ff17817b330957eff8250bc22e9fb6faff9f92))
* move rpc endpoints url params to request body ([4ad2a90](https://github.com/propeller-heads/tycho-indexer/commit/4ad2a908202e1411958770b739c09510a854cffb))
* **tycho-client-py:** update rpc client to use new endpoints ([f934269](https://github.com/propeller-heads/tycho-indexer/commit/f934269727f8521b8113994ef171878ba64de3f4))
* **tycho-client:** update rpc to use new endpoints ([6e79ed1](https://github.com/propeller-heads/tycho-indexer/commit/6e79ed113fb86b8d2064092fdd75e9728dd84fe8))

## [0.11.1](https://github.com/propeller-heads/tycho-indexer/compare/0.11.0...0.11.1) (2024-08-27)


### Bug Fixes

* **dto:** Use capitalize enum values. ([0707bde](https://github.com/propeller-heads/tycho-indexer/commit/0707bde431ea4da6b1ca5e76677e8e958e13abea))
* **dto:** Use capitalize enum values. ([#339](https://github.com/propeller-heads/tycho-indexer/issues/339)) ([1989d0c](https://github.com/propeller-heads/tycho-indexer/commit/1989d0c960b01cd2391e1364dc571922b0728d27))

## [0.11.0](https://github.com/propeller-heads/tycho-indexer/compare/0.10.0...0.11.0) (2024-08-26)


### Features

* add non-SIP protected binary directory option ([b4d3d69](https://github.com/propeller-heads/tycho-indexer/commit/b4d3d694e0c9c6601ab04dcd74da0aa75383f818))


### Bug Fixes

* support range tvl threshold on client stream creation ([fbbd8cf](https://github.com/propeller-heads/tycho-indexer/commit/fbbd8cf9404e74002f87ea80af9bba83d26e1dd4))
* **tycho-client:** remove hardcoded versioning on cli ([5a721f4](https://github.com/propeller-heads/tycho-indexer/commit/5a721f44c423330c18ba17874f2434b956c77a7b))
* update contract request body to include protocol_system ([b2858e9](https://github.com/propeller-heads/tycho-indexer/commit/b2858e9c61f66017fd6feb3f396758216eea94f9))
* Update python client ([#338](https://github.com/propeller-heads/tycho-indexer/issues/338)) ([0b3e59d](https://github.com/propeller-heads/tycho-indexer/commit/0b3e59dfcb967eb65e92fbeeb7cba64b701e5c61))

## [0.10.0](https://github.com/propeller-heads/tycho-indexer/compare/0.9.1...0.10.0) (2024-08-19)


### Features

* **tycho-client:** Add tvl range as a component filter ([6a197b7](https://github.com/propeller-heads/tycho-indexer/commit/6a197b745aaf7219f4b25fb3409dbcca704e70f1))
* **tycho-client:** Add tvl range as a component filter ([#328](https://github.com/propeller-heads/tycho-indexer/issues/328)) ([a33fb5c](https://github.com/propeller-heads/tycho-indexer/commit/a33fb5c518977add7f3ade77125cf408fe930c0f))
* **tycho-client:** update cli to accept min tvl range input ([78873c9](https://github.com/propeller-heads/tycho-indexer/commit/78873c9dedd185a9feae8198b4e3312d74709e82))

## [0.9.1](https://github.com/propeller-heads/tycho-indexer/compare/0.9.0...0.9.1) (2024-08-16)


### Bug Fixes

* deserialise WebSocketMessage workaround ([8021493](https://github.com/propeller-heads/tycho-indexer/commit/80214933c76d228a67ab4420df0642bd2f7821a4))
* improve deserialisation error messages ([d9e56b1](https://github.com/propeller-heads/tycho-indexer/commit/d9e56b1cbef1bb874fa401f1df6d40a10028e690))
* WebSocketMessage deserialisation bug ([#327](https://github.com/propeller-heads/tycho-indexer/issues/327)) ([6dfebb0](https://github.com/propeller-heads/tycho-indexer/commit/6dfebb0e5718979023cb2bb8890566cc740647f1))

## [0.9.0](https://github.com/propeller-heads/tycho-indexer/compare/0.8.3...0.9.0) (2024-08-15)


### Features

* **rpc:** make serde error if unknown field in bodies ([2aaaf0e](https://github.com/propeller-heads/tycho-indexer/commit/2aaaf0edbc814d26a8a89c965c2d3800e82dc0c9))

## [0.8.3](https://github.com/propeller-heads/tycho-indexer/compare/0.8.2...0.8.3) (2024-08-15)


### Bug Fixes

* **client-py:** fix hexbytes decoding and remove camelCase aliases ([4a0432e](https://github.com/propeller-heads/tycho-indexer/commit/4a0432e4446c6b0595168d0c99663f894d490694))
* **client-py:** fix hexbytes encoding and remove camelCase aliases ([#322](https://github.com/propeller-heads/tycho-indexer/issues/322)) ([10272a4](https://github.com/propeller-heads/tycho-indexer/commit/10272a4a2d35ece95713bf983efd5978a7587ca4))

## [0.8.2](https://github.com/propeller-heads/tycho-indexer/compare/0.8.1...0.8.2) (2024-08-14)


### Bug Fixes

* skip buggy clippy warning ([feeb6a1](https://github.com/propeller-heads/tycho-indexer/commit/feeb6a11692d6fabd171cff8cc0bd9be46ad4461))
* specify extractor on rpc requests ([98d57d2](https://github.com/propeller-heads/tycho-indexer/commit/98d57d281c32edcf0790e1d33fadcca0ca13a613))
* Specify extractor on rpc requests ([#323](https://github.com/propeller-heads/tycho-indexer/issues/323)) ([a45df90](https://github.com/propeller-heads/tycho-indexer/commit/a45df90fe5010a965404e368febca4dc414fe0f0))

## [0.8.1](https://github.com/propeller-heads/tycho-indexer/compare/0.8.0...0.8.1) (2024-08-09)


### Bug Fixes

* Hanging client on max connection attempts reached ([#317](https://github.com/propeller-heads/tycho-indexer/issues/317)) ([f9ca57a](https://github.com/propeller-heads/tycho-indexer/commit/f9ca57a1ad9af8af3d5b8e136abc5ea85641ef16))
* hanging client when max connection attempts reached ([feddb47](https://github.com/propeller-heads/tycho-indexer/commit/feddb4725143bde9cb0c99a8b7ca9c4d60ec741f))
* propagate max connection attempts error correctly ([6f7f35f](https://github.com/propeller-heads/tycho-indexer/commit/6f7f35fa9d56a8efe2ed2538b7f02543a5300b4a))
* **tycho-client:** reconnection error handling ([4829f97](https://github.com/propeller-heads/tycho-indexer/commit/4829f976e092da5ef0fdb96e353fb6157557f825))

## [0.8.0](https://github.com/propeller-heads/tycho-indexer/compare/0.7.5...0.8.0) (2024-08-09)


### Features

* change workflow behaviour ([61f7517](https://github.com/propeller-heads/tycho-indexer/commit/61f7517b64cb62468160a88eb485c2a91bceef49))
* change workflow behaviour ([#316](https://github.com/propeller-heads/tycho-indexer/issues/316)) ([3ca195b](https://github.com/propeller-heads/tycho-indexer/commit/3ca195b9f1a7ce76f857e3b7ad76d39d2a374a60))

## [0.7.5](https://github.com/propeller-heads/tycho-indexer/compare/0.7.4...0.7.5) (2024-08-07)


### chore

* black format code ([7dcb55a](https://github.com/propeller-heads/tycho-indexer/commit/7dcb55af3eea7c807e3c9491bd9d0574533ff8df))
* Remove unneeded new method and outdated comment ([d402acb](https://github.com/propeller-heads/tycho-indexer/commit/d402acb6c2e52f537f27b82d8b6dfd8449627a4a))

### fix

* Add missing requests dependency ([d64764c](https://github.com/propeller-heads/tycho-indexer/commit/d64764ca07cadc8f312c6d1c26f00da367d06447))
* Add property aliases to ResponseAccount. ([298c688](https://github.com/propeller-heads/tycho-indexer/commit/298c688fd8acca21da8c3cf45be953fbf1153b8e))

## [0.7.4](https://github.com/propeller-heads/tycho-indexer/compare/0.7.3...0.7.4) (2024-08-07)


### fix

* fix usv2 substreams merge bug ([88ce6c6](https://github.com/propeller-heads/tycho-indexer/commit/88ce6c6f7a440681113e442342e877cb6091656d))

## [0.7.3](https://github.com/propeller-heads/tycho-indexer/compare/0.7.2...0.7.3) (2024-08-06)


### chore

* Add trace logging for tokens queries ([01a5bbc](https://github.com/propeller-heads/tycho-indexer/commit/01a5bbcca61d8dde3620790ab11529e635b07cce))

### fix

* add defaults for initialized_accounts configs ([2becb5e](https://github.com/propeller-heads/tycho-indexer/commit/2becb5ea60a24f51fee9a49ce5b2b1b2edd213f9))
* changed tag format ([764d9e6](https://github.com/propeller-heads/tycho-indexer/commit/764d9e6bb33e623780f58c4be4628ba6985e0d58))
* ci-cd-templates path ([1c21f79](https://github.com/propeller-heads/tycho-indexer/commit/1c21f793bfabfdd233efa1c58af6cf0c686d2a8e))
* clean up defaults and spkg name ([eac825c](https://github.com/propeller-heads/tycho-indexer/commit/eac825c2d5d29586d53ba773c8f3695504a4298b))
* dockerfile restore quotes ([1d73485](https://github.com/propeller-heads/tycho-indexer/commit/1d73485f97b4dbe7dba188b8bd1b772b3107a01d))
* revert sushiswap config change ([b10921e](https://github.com/propeller-heads/tycho-indexer/commit/b10921e7510b229990c767d10795041a138e7a9f))

### update

* Cargo.lock ([9b129ef](https://github.com/propeller-heads/tycho-indexer/commit/9b129efa09bcd1956b56fa4c2ad1724d3a1dda12))
