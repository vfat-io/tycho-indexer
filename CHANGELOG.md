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

# 1.0.0 (2024-08-06)


### _get_or_create_protocol_system_id

* add draft ([404c662](https://github.com/propeller-heads/tycho-indexer/commit/404c662aa57703eb0ac800125cd4a1bb73c53ac0))

### Account

* Change From<&TransactionUpdates> impl to impl for Vec<Account> ([b21f392](https://github.com/propeller-heads/tycho-indexer/commit/b21f392239e2691cde356f207766da3d66cfba27))

### AccountUpdate

* Implement ref_into_account ([fd03d4b](https://github.com/propeller-heads/tycho-indexer/commit/fd03d4b822415e0da1670d1f3ae799c2d50ef251))

### add_components

* Change from upsert_components ([49e0ac0](https://github.com/propeller-heads/tycho-indexer/commit/49e0ac09b900b11626d3916bdbd6d8d98f72fa20))

### add_protocol_components

* Adjust test ([d49c7a6](https://github.com/propeller-heads/tycho-indexer/commit/d49c7a66beb413aadbc388741d41c9f452f85e07))
* Construct & Insert token to component relations ([56acab5](https://github.com/propeller-heads/tycho-indexer/commit/56acab5654376c03599e84d54364a44479a35ed7))
* Establish component-contract junction ([7c91a8f](https://github.com/propeller-heads/tycho-indexer/commit/7c91a8ff80bb8bfb75ad53f259457ed8733b0aa2))
* Refactor 1st draft of component-token-relation ([71fb132](https://github.com/propeller-heads/tycho-indexer/commit/71fb132daf054b916eecb86c94f1d82c4472dfae))

### Ambient

* Call from_signed_bytes_be when converting the balance deltas. ([d5b40ef](https://github.com/propeller-heads/tycho-indexer/commit/d5b40efe24e48ac84f62834a0324fa40a1bb861f))
* fix inconsistency when reading balance. ([43efb52](https://github.com/propeller-heads/tycho-indexer/commit/43efb5212fc58effe6dceaccbb2dd087f8400771))
* Major bugfix in pool index casting. ([7c89278](https://github.com/propeller-heads/tycho-indexer/commit/7c8927885f237e59d9bb78e352d9c0f7e785e536))
* Move contract logic into one file per contract. ([1809cb5](https://github.com/propeller-heads/tycho-indexer/commit/1809cb53c26cdbac508910fcc57b9e2cb338fe8f))
* Process only final blocks; ([3184b93](https://github.com/propeller-heads/tycho-indexer/commit/3184b93622bdab4b733a92ad721bafb608c4b3ae))
* Use match statements for TVL extraction. ([9f5e801](https://github.com/propeller-heads/tycho-indexer/commit/9f5e8012ec78a2cc508dabb701370193554a1196))

### BlockContractChanges

* Adjust aggregate_updates ([7e980cb](https://github.com/propeller-heads/tycho-indexer/commit/7e980cbc987241113f5cfafb41c3c029c2b62b49))

### BlockSynchronizer

* Add a first test. ([ad16092](https://github.com/propeller-heads/tycho-indexer/commit/ad16092fa0cb92122df57c30819a736b8fbe6438))
* Add some docs, address todos. ([ed274b2](https://github.com/propeller-heads/tycho-indexer/commit/ed274b206a6488f92e92b4e3304a445cf0b8eb84))
* Improve docs. ([a3a2de7](https://github.com/propeller-heads/tycho-indexer/commit/a3a2de76027b23fd952c2003059c0be2c3d321e0))

### Bugfix

* components and balances were duplicated in ambient substreams ([3bdbb58](https://github.com/propeller-heads/tycho-indexer/commit/3bdbb584cdaa36f09fd830c803c5953374dec878))
* ensure_protocol_types needs to be impl in AmbientGateway ([d7e5b94](https://github.com/propeller-heads/tycho-indexer/commit/d7e5b94c7fb9bf15522209e35ecf06a2d955a1a2))

### BugFix

* PostProcessing must happen before we ingest the msg. ([85b772c](https://github.com/propeller-heads/tycho-indexer/commit/85b772c3a1caff2875afa1553e1bf64f41f8498a))

### CachedGateway

* Fix a bug where the open_tx state would be shared. ([2aaa43c](https://github.com/propeller-heads/tycho-indexer/commit/2aaa43c95bb2f642c3f63dd1a0726b707107b6ff))
* Fix commit transaction. ([f311e5e](https://github.com/propeller-heads/tycho-indexer/commit/f311e5e4e9ebf824344c77d3a4147affbbbb70b8))
* Push changes of syncing extractors also in a transaction. ([fccc17e](https://github.com/propeller-heads/tycho-indexer/commit/fccc17ec178d14bfcbaff0151e06d0ff0ef2dca2))
* Remove revert and flush functionality. ([3d0d3e7](https://github.com/propeller-heads/tycho-indexer/commit/3d0d3e784fb75443af72d3c56bb9031dfe21047b))

### ChainGateway

* implement _get_tx_ids ([32da173](https://github.com/propeller-heads/tycho-indexer/commit/32da1737f642744c85bc7bcdd720d78e824743e7))

### chore

* Add trace logging for tokens queries ([01a5bbc](https://github.com/propeller-heads/tycho-indexer/commit/01a5bbcca61d8dde3620790ab11529e635b07cce))
* Add tracing_subscriber initialization with default environment filter ([17c80d2](https://github.com/propeller-heads/tycho-indexer/commit/17c80d2c6cb91dee26918c6f311520c3d93962a7))
* bump cargo versions ([78543b5](https://github.com/propeller-heads/tycho-indexer/commit/78543b546d13571d4129f8ebe46e266f7e96c9a2))
* Remove RPC server example ([861da36](https://github.com/propeller-heads/tycho-indexer/commit/861da363ea1e72659deef8389ab5b51f29fe8f49))
* Update CI workflow to build custom PostgreSQL Docker image ([71c9d9a](https://github.com/propeller-heads/tycho-indexer/commit/71c9d9acacb26ad2c538c85350de29303b95a739))
* Update logging to use trace level for message forwarding in ws.rs ([fad32c6](https://github.com/propeller-heads/tycho-indexer/commit/fad32c6b187a4986cbc9b9d35d405608c618b295))

### ci

* make clippy happy ([32d2291](https://github.com/propeller-heads/tycho-indexer/commit/32d22917580def3354149d5cfc4b8f305079ff57))

### CI

* add buf action ([7045cc4](https://github.com/propeller-heads/tycho-indexer/commit/7045cc40affdcdc66719380a60c126d8aab133a9))
* Add protobuf formatting check ([e0d5e52](https://github.com/propeller-heads/tycho-indexer/commit/e0d5e523ae054714639b80ed6f31187e3ae6e7cf))
* fix db setup ([40361c7](https://github.com/propeller-heads/tycho-indexer/commit/40361c727a435254f7c6bd26afc510711ae18123))
* Happiness ([727a672](https://github.com/propeller-heads/tycho-indexer/commit/727a67200f165414b9303abad2856bd400fd2cd0))

### Cleanup

* Remove some unused variables. ([6f17fb4](https://github.com/propeller-heads/tycho-indexer/commit/6f17fb4fa82e75df675fce2fbeea0fd4a4cad674))

### CleanUP

* Remove now unused Storable trait implementations. ([6d6452c](https://github.com/propeller-heads/tycho-indexer/commit/6d6452c977f20fa3983cb645078b39743211aee5))

### ContractGW

* Ergonomic interfaces ([6d8abf3](https://github.com/propeller-heads/tycho-indexer/commit/6d8abf3dc069c1c9559c7609e889d05a54c00e8e))

### ContractId

* Add for documentation purposes ([9d775c3](https://github.com/propeller-heads/tycho-indexer/commit/9d775c377c4d7aaa185de9b11348149d32a69ae6))

### Database

* Add orm and schema for component-contract junction ([ebf739d](https://github.com/propeller-heads/tycho-indexer/commit/ebf739dd57c5b863a52a3e94158aedd4bfb58fb0))
* Remove protocol_system_type and use string instead ([7dc7d2b](https://github.com/propeller-heads/tycho-indexer/commit/7dc7d2ba9115ed146fadfc42873af51a76843807))

### DB

* Add protocol_system_type and enum to migration ([2ca9fbe](https://github.com/propeller-heads/tycho-indexer/commit/2ca9fbef16698350a04b6a4733e6cb01b1512a94))

### delete_protocol_components

* Add block_ts ([d5e4ec5](https://github.com/propeller-heads/tycho-indexer/commit/d5e4ec5b3e579d4da7b9d7d3e973e5ea76460e30))
* Remove updating modified_ts ([6909865](https://github.com/propeller-heads/tycho-indexer/commit/6909865c458f72ccd81ad9d70878bf3eac442f77))

### Docker

* separate repository for the build chache. ([8c0fc36](https://github.com/propeller-heads/tycho-indexer/commit/8c0fc36add0e4a3027513a77e236f84b37998dad))

### Dockerfile

* drop awk and use jq ([1c16d7e](https://github.com/propeller-heads/tycho-indexer/commit/1c16d7e8b0a9f437e10a386c3849cea65d64cc5f))
* use x84_64 instead of amd64 ([74e2d59](https://github.com/propeller-heads/tycho-indexer/commit/74e2d593acb840b809cd30de82e5a1c8f48891ac))

### docs

* Add docs and correct typos. ([01d8f97](https://github.com/propeller-heads/tycho-indexer/commit/01d8f977070e622ab67429db85d9c79131ad39d8))
* Add docs and improve naming ([34f5dff](https://github.com/propeller-heads/tycho-indexer/commit/34f5dff05d2c29242246d1f79b4ccad933c95d9d))
* add get_new_components description ([6f92d32](https://github.com/propeller-heads/tycho-indexer/commit/6f92d32ca9615da75438f042d5239039437b10d2))

### Draft

* add get_new_tokens to AmbientPgGateway ([aa6e4f9](https://github.com/propeller-heads/tycho-indexer/commit/aa6e4f93ac462dc2dbf2f9aef7c2fc31b47c4563))
* Save ProtocolSystem to DB ([d815cd5](https://github.com/propeller-heads/tycho-indexer/commit/d815cd5f012356222ba78d244299bcf1ffa527f5))
* upsert_components ([71aa578](https://github.com/propeller-heads/tycho-indexer/commit/71aa5785a5db834bc3b537f5bd964391d1a2d3f1))

### EnumTableCache

* Adjust to hold string as well ([4676d6b](https://github.com/propeller-heads/tycho-indexer/commit/4676d6bffc5c585208392dd3938574cc3df9ca78))

### evm

* :ProtocolComponent: Add creation_tx and created_at ([23da4c7](https://github.com/propeller-heads/tycho-indexer/commit/23da4c72921e7594dcd83b4cd02df902cea697d5))

### feat

* add block index in migration ([88f091a](https://github.com/propeller-heads/tycho-indexer/commit/88f091a7ba5877af38752b61e9ed0ff1879af0b7))
* Add CLI for running one extractor or RPC ([89110e3](https://github.com/propeller-heads/tycho-indexer/commit/89110e369610dd0ee1d84a1a7c51461b4f38cecc))
* Add open telemetry tracing. ([9a6ab0e](https://github.com/propeller-heads/tycho-indexer/commit/9a6ab0e7fd53c588d7b3d737fd209ed9e9189d4a))
* Add pg_cron extension and pdate docker-compose.yaml to use custom postgres.Dockerfile ([5496f69](https://github.com/propeller-heads/tycho-indexer/commit/5496f69986a68162104dc4f0ce0b52a551860d95))
* add Substreams config file for uniswaps on Arbitrum ([4a5816a](https://github.com/propeller-heads/tycho-indexer/commit/4a5816aa3cbf7c1c21603723f70075f7aa60ad02))
* add Substreams config file for uniswaps on Arbitrum ([f5c22eb](https://github.com/propeller-heads/tycho-indexer/commit/f5c22eb04a46ea715e064c17652884f93a5a4394))
* implement Revert Buffer for BlockEntityChanges ([331b73b](https://github.com/propeller-heads/tycho-indexer/commit/331b73b48dc1af0f70559fd0f3eeda29ce653d74))
* make tycho-client chain agnostic ([b227a3e](https://github.com/propeller-heads/tycho-indexer/commit/b227a3e556e2834bbc00246a44b5f06532785400))
* remove last hardcoded ethereum ([4cf8d3c](https://github.com/propeller-heads/tycho-indexer/commit/4cf8d3cc89cb52aaeff2802c804819badb180c00))
* script to compare vm protocols against the node. ([3fe614c](https://github.com/propeller-heads/tycho-indexer/commit/3fe614c8b9045d24a6bdd98d1e4ca85940767ad6))

### fix

* add defaults for initialized_accounts configs ([2becb5e](https://github.com/propeller-heads/tycho-indexer/commit/2becb5ea60a24f51fee9a49ce5b2b1b2edd213f9))
* add extractor id to hybrid gateway transaction ([6ae257a](https://github.com/propeller-heads/tycho-indexer/commit/6ae257a496d803246eaa45bf0196a7d8fa709d22))
* add postprocessor for curve ([52ad1e2](https://github.com/propeller-heads/tycho-indexer/commit/52ad1e2c04e21050a175a1a3563d65215171cd7b))
* Add versionning to protocol_state ([5ea9da6](https://github.com/propeller-heads/tycho-indexer/commit/5ea9da67e823fb1b923205b9bf0c3f26feff0d45))
* allow empty BlockContractChanges.tx_updates ([3bf49e9](https://github.com/propeller-heads/tycho-indexer/commit/3bf49e98b847b49ccfdcb70a84f264da90e9e6d9))
* CI health command II ([7b0b439](https://github.com/propeller-heads/tycho-indexer/commit/7b0b439e03c2ff70e027fe159e216961919c688f))
* CI health command. ([0da599d](https://github.com/propeller-heads/tycho-indexer/commit/0da599d1583ef4bbe2122a5813381910380f6a4f))
* ci-cd-templates path ([1c21f79](https://github.com/propeller-heads/tycho-indexer/commit/1c21f793bfabfdd233efa1c58af6cf0c686d2a8e))
* clean up defaults and spkg name ([eac825c](https://github.com/propeller-heads/tycho-indexer/commit/eac825c2d5d29586d53ba773c8f3695504a4298b))
* clippy ([8761e23](https://github.com/propeller-heads/tycho-indexer/commit/8761e239652f7777f5d8a398a709ffbfaa9a33e4))
* Correctly handle when the extractor receives a undo signal as a first message ([135ea16](https://github.com/propeller-heads/tycho-indexer/commit/135ea16a5634f152db2862b2147da07f4ec94505))
* correctly init protocol_system_cache ([6e3fc71](https://github.com/propeller-heads/tycho-indexer/commit/6e3fc71c3becd92ab19a2f9f8eefe1ab06f387e8))
* correctly return protocol type names ([b52c90a](https://github.com/propeller-heads/tycho-indexer/commit/b52c90aa2caaea6b5280e08bd7ec97ec7f1def2c))
* dockerfile restore quotes ([1d73485](https://github.com/propeller-heads/tycho-indexer/commit/1d73485f97b4dbe7dba188b8bd1b772b3107a01d))
* encode component as utf8 in BalanceChange ([324d856](https://github.com/propeller-heads/tycho-indexer/commit/324d8561cb0ba652f2052ba3e89eb3c910fbfc80))
* fix postprocessor merge issue ([350e66a](https://github.com/propeller-heads/tycho-indexer/commit/350e66a45978a4b7299f8dc5419565da568cf1c7))
* format abi files ([5043b50](https://github.com/propeller-heads/tycho-indexer/commit/5043b502c09b7049355cbf2f640eb4a14dd9e8f1))
* format abi files ([36a911e](https://github.com/propeller-heads/tycho-indexer/commit/36a911eb052f32b0cabe08c4aeea6b46b04e0d6e))
* import skpgs in the docker image and fix path ([9d85587](https://github.com/propeller-heads/tycho-indexer/commit/9d8558734dd101c4576024919939326ff627dacb))
* imports after rebase ([ea44a02](https://github.com/propeller-heads/tycho-indexer/commit/ea44a028b908e58115aac1ef617736cc667f9a4d))
* integration tests. ([1fb0689](https://github.com/propeller-heads/tycho-indexer/commit/1fb06894e86c231fa4dc03ba8e7a375d3d01761d))
* keep system filtering for cached protocol components ([2604589](https://github.com/propeller-heads/tycho-indexer/commit/2604589b1dc5e431f02f6876ef2fe46cd910c11e))
* make block ts distinguishable ([0821203](https://github.com/propeller-heads/tycho-indexer/commit/082120391a7c1d5c0a632d7acafc5d59c3984249))
* make migration folder name match release version ([e47b73f](https://github.com/propeller-heads/tycho-indexer/commit/e47b73fac7a29f61d931c1d3923fc300c08970a1))
* move the pool contract address to be an attribute. ([2553013](https://github.com/propeller-heads/tycho-indexer/commit/2553013b22783d40b7a3dc72ee6128cba71d637d))
* order by c_id to allow correct group_by ([7c85a0e](https://github.com/propeller-heads/tycho-indexer/commit/7c85a0eb7282d5db5eb280a40acf249a6e380267))
* re-expose retention horizon post rebase. ([6fa4e02](https://github.com/propeller-heads/tycho-indexer/commit/6fa4e02819853d5757ab69628c0c4265221ea07b))
* read erc20.json at compile time ([d6a2fd0](https://github.com/propeller-heads/tycho-indexer/commit/d6a2fd03bd5017986bee86e41649eff5b0180966))
* remove UniswapV2 postprocessor for blockchain that uses v2 of the Substreams module ([3c8936c](https://github.com/propeller-heads/tycho-indexer/commit/3c8936c609ce9580f87afe59a222d3867977ad4f))
* remove unnecessary clones ([6743b5b](https://github.com/propeller-heads/tycho-indexer/commit/6743b5b9f28fdf365dc44bec0a60eb6236fb867a))
* revert sushiswap config change ([b10921e](https://github.com/propeller-heads/tycho-indexer/commit/b10921e7510b229990c767d10795041a138e7a9f))
* skip first message from substream on restart ([913ebbd](https://github.com/propeller-heads/tycho-indexer/commit/913ebbd96082f92ddce0c4b3bdb90392007a90d5))
* sort balance changes by ordinal ([781f844](https://github.com/propeller-heads/tycho-indexer/commit/781f844185e6a347d43a2c29ea4bf6afef9b7e5b))
* test cases for new storage methods. ([e6269b0](https://github.com/propeller-heads/tycho-indexer/commit/e6269b066c45acc19d7ab15546d9983f1946e795))
* tests reusing integration test fixtures. ([2bb4fe9](https://github.com/propeller-heads/tycho-indexer/commit/2bb4fe9f141997a07ed349f8a77301a813bd6518))
* uniswapv3 module attributes values encoding ([8d93a5e](https://github.com/propeller-heads/tycho-indexer/commit/8d93a5e296d6cd10faff3a9426cbef2cd6c27445))
* Update run to use Utc time for indexing ([b8f1a20](https://github.com/propeller-heads/tycho-indexer/commit/b8f1a2081f3c5c560ac53ffb31a72cba96e404fa))

### Fix

* also return slots in /contract_state/ ([ef806a8](https://github.com/propeller-heads/tycho-indexer/commit/ef806a8a1a040e52a7bccb551295144e4d4b630a))
* correctly get previous_value ([dc58946](https://github.com/propeller-heads/tycho-indexer/commit/dc589464d5c297b0144d746b39f68845eac500ce))
* Sort tx by index before applying them ([a4fa319](https://github.com/propeller-heads/tycho-indexer/commit/a4fa319040799a3cc0ee63259068997108776d1a))

### get_balance_deltas

* add test for backward case ([ad969fa](https://github.com/propeller-heads/tycho-indexer/commit/ad969fa1f077f4f9a7993d39357c79928dcc8e80))
* add test for forward case ([93686a7](https://github.com/propeller-heads/tycho-indexer/commit/93686a762f0d7ed455f3f40ca3deb72b3ef0a78a))
* Return a vec instead of a hashmap. ([0080987](https://github.com/propeller-heads/tycho-indexer/commit/0080987127c3fae11677391c715710126474f30a))
* return external_id and token_address as keys ([191770e](https://github.com/propeller-heads/tycho-indexer/commit/191770eaf47950ddf1ef116773d0e78fd572da27))

### get_protocol_components

* implement join ([f8656c2](https://github.com/propeller-heads/tycho-indexer/commit/f8656c29bd5b424aeb38839c6357489087cb799b))
* Make system input mandatory & extensive testing ([e2997c3](https://github.com/propeller-heads/tycho-indexer/commit/e2997c3dc1ddb902c18cb2b3f713e285d3342948))
* Query token and contracts related to component ([15c253c](https://github.com/propeller-heads/tycho-indexer/commit/15c253c3b9adbf854b0b42cf3d65426dc5688ded))

### Init

* Public message types for tycho ([f94578a](https://github.com/propeller-heads/tycho-indexer/commit/f94578ab1719a1f64dac4c6c834f036de03763e3))

### insert_protocol_system

* Add function for better test setup ([dd0bbab](https://github.com/propeller-heads/tycho-indexer/commit/dd0bbab56d877e94b04249e7b52df5c1dc0f025a))

### map_changes

* Adjust TransactionChanges construction ([40745c7](https://github.com/propeller-heads/tycho-indexer/commit/40745c7788cb5726011c1214805b9e4569a83c1f))
* bail! instead of `return Err(anyhow!` ([9478b69](https://github.com/propeller-heads/tycho-indexer/commit/9478b69cf441b568d832916c0501b9fb229a2497))
* Check ambient cmd code before decoding inner call. ([4b59cb9](https://github.com/propeller-heads/tycho-indexer/commit/4b59cb924f89c72de2fd92a82cb25dd65b157525))
* Compare input bytes, no string conversion. ([af1f47b](https://github.com/propeller-heads/tycho-indexer/commit/af1f47b2ca0eea22771da48ee861f2244e5056f8))
* Emit ProtocolComponent for ambient pool creations ([643df40](https://github.com/propeller-heads/tycho-indexer/commit/643df40ebc647ba6e55bc9ab2155fcfa0c7a4fe0))
* Fix ProtocolComponent msg types (and other fixes). ([23b30eb](https://github.com/propeller-heads/tycho-indexer/commit/23b30ebf0bbca27680a2abf4dbc0b0049fdcfb9f))
* Iterate through calls, not transactions. ([e3a0183](https://github.com/propeller-heads/tycho-indexer/commit/e3a01834617af0b09eabfed0774fedb6c8f0c5f7))
* Remove match statements and transform panics. ([ecb7ea9](https://github.com/propeller-heads/tycho-indexer/commit/ecb7ea9cd07cb2737ae078ad70ebc6fb4834c6e2))

### Migration

* Add Component-Contract junction ([4a5ec69](https://github.com/propeller-heads/tycho-indexer/commit/4a5ec6920e58d06a10d5637367114b778f4f3137))
* Add junction table to enable many-to-many relation ([abaa82d](https://github.com/propeller-heads/tycho-indexer/commit/abaa82d3f1b7390adfd7a8a13f3c6194c0057e85))
* Remove redundant table ([f27d067](https://github.com/propeller-heads/tycho-indexer/commit/f27d067bd844472554c8a7f7b345d59199c1c3b5))
* replace SERIAL type with BIGSERIAL ([7de18e7](https://github.com/propeller-heads/tycho-indexer/commit/7de18e7e77a612c63f7a32169eb67309b8ec6382))

### Migrations

* Drop unique_name_constraint in down.sql ([a1c3086](https://github.com/propeller-heads/tycho-indexer/commit/a1c308634dee06d5f63c7886823c21b84868e05f))

### Murphy

* Tycho got stuck exactly at a revert. ([0866eb3](https://github.com/propeller-heads/tycho-indexer/commit/0866eb313adf8948720dbd33c77add2ba8709776))

### NewProtocolComponent

* add created_at ([1e4426c](https://github.com/propeller-heads/tycho-indexer/commit/1e4426cee17008331b5fcc9b94085ad21ff699bf))
* Initial implementation ([f61b94d](https://github.com/propeller-heads/tycho-indexer/commit/f61b94d94adac070b6a3977cc5ebf347643d9d42))
* Remove created_at ([77faab1](https://github.com/propeller-heads/tycho-indexer/commit/77faab10bcff15de470810a02bdda231b545a2e6))

### NewProtocolHoldsToken

* Move and rename ([9cf0267](https://github.com/propeller-heads/tycho-indexer/commit/9cf026790eaf728f1b51bd80ed87fb028a9f8b59))

### NewProtocolSystemType

* Remove id field ([a641ca5](https://github.com/propeller-heads/tycho-indexer/commit/a641ca51765de439e7979472aaa717c8e0115dd5))

### orm

* Add NewProtocolSystemType and ProtocolSystemType ([9cc2a05](https://github.com/propeller-heads/tycho-indexer/commit/9cc2a05654549b2a370a9f4e3317cdf0bb687f4c))

### Orms

* Add ProtocolSystemType as Queryable ([e8342f1](https://github.com/propeller-heads/tycho-indexer/commit/e8342f181ec2f901d1e8056cb9aa57e1fbc09180))

### PostgresGateway

* Add token filter test ([77e29a4](https://github.com/propeller-heads/tycho-indexer/commit/77e29a48d9ba23a9fdfd0e04f6300cfef90c2a45))
* Filter tokens by chain ([ab0609d](https://github.com/propeller-heads/tycho-indexer/commit/ab0609dee25cd6ef3268027d70f32f3a8109916e))
* Implement _get_or_create_protocol_system_id ([e97b8ea](https://github.com/propeller-heads/tycho-indexer/commit/e97b8ea3e215d12bc496a187489225a3c98e2fa3))

### Protobuf

* Add ProtocolType and respective enums ([ca1684b](https://github.com/propeller-heads/tycho-indexer/commit/ca1684b33cf1e060666cc41c3448aed87ccbf7c6))
* Add ProtocolType to ambient substream ([63f9ac4](https://github.com/propeller-heads/tycho-indexer/commit/63f9ac43ac7f63f145988ef89866c6d1071559fb))
* Lint ([4efe384](https://github.com/propeller-heads/tycho-indexer/commit/4efe38454c35a507874599d8fc572f6bbc616f4c))
* ProtocolComponent.contracts bytes->string ([74f5767](https://github.com/propeller-heads/tycho-indexer/commit/74f5767e65490227d192507518bd3ef5654bf967))
* Rename TVLChange to BalanceChange ([4c4e611](https://github.com/propeller-heads/tycho-indexer/commit/4c4e6117306f6d14bd92060739a4497711ac84a1))

### ProtoBuff

* Add ProtocolComponent and TVLUpdate ([c57aee2](https://github.com/propeller-heads/tycho-indexer/commit/c57aee209385688ad738d553ad35859117642537))
* Generate files ([0d92a50](https://github.com/propeller-heads/tycho-indexer/commit/0d92a50c23d9db9d980f86ceda58b221d5111255))

### ProtocolComponent

* Add att to protobuf message ([783c2ad](https://github.com/propeller-heads/tycho-indexer/commit/783c2ad04c37b2381d03d041e0631fe722aef6ea))
* Add get_byte_contract_addresses ([981cd61](https://github.com/propeller-heads/tycho-indexer/commit/981cd6114c79592527662e8db04190b80e294880))
* Add get_byte_token_addresses ([3f17525](https://github.com/propeller-heads/tycho-indexer/commit/3f17525ee75d0dfbe889327d40b8a8f959dbbabe))
* Add try_from_message ([1930f91](https://github.com/propeller-heads/tycho-indexer/commit/1930f917a38762f9488135583452d160e6ff1463))
* Adjust contract_ids typing ([6f0334f](https://github.com/propeller-heads/tycho-indexer/commit/6f0334faa703ac2fd37d7fa69349e43e7455ce98))
* adjust to match table schema ([c57505e](https://github.com/propeller-heads/tycho-indexer/commit/c57505ed468ca31c48fa917d0e4916e451852c15))
* Change token address type ([4795a2a](https://github.com/propeller-heads/tycho-indexer/commit/4795a2a46eaaae93b7a3f5cbb260a63dc613bcf9))
* Change token address type ([639a14a](https://github.com/propeller-heads/tycho-indexer/commit/639a14a5f0f2ec4e263b9a0b2c50a92a39d57b72))
* Change token address type ([086d05f](https://github.com/propeller-heads/tycho-indexer/commit/086d05f55344a44877e02328f9d9acdfc16e8c98))
* change try_from_message interface from String to str ([f3e5a19](https://github.com/propeller-heads/tycho-indexer/commit/f3e5a19ff000bb026de38bf51bc73c6ebe7ff966))
* Change type of static_attributes to HashMap<String, Bytes> ([bbba73a](https://github.com/propeller-heads/tycho-indexer/commit/bbba73a89225b82305681adf17c6bcdb8f265a3e))
* Improve test by using copy ([1576853](https://github.com/propeller-heads/tycho-indexer/commit/15768534263a6a1d5e4c5a55c4bd308729b95e62))
* Make att public ([30428c8](https://github.com/propeller-heads/tycho-indexer/commit/30428c8e11a516fe079700cb97c3b79c696290d4))
* Remove generics ([764d831](https://github.com/propeller-heads/tycho-indexer/commit/764d83142d7d96e5aba3bbb5f13e0c8b56bcdceb))
* Rename attribute_schema to static_attributes ([eebd618](https://github.com/propeller-heads/tycho-indexer/commit/eebd61867706bc68025c5ced478da35d9a349577))

### ProtocolGateway

* add assertion to protocol_type test. ([0b7e2bf](https://github.com/propeller-heads/tycho-indexer/commit/0b7e2bfd6d9864e0ffe8c14433c8f50c65727d82))
* add method to upsert ProtocolTypes. ([b787ce0](https://github.com/propeller-heads/tycho-indexer/commit/b787ce0bdf51dd01e9a80e001736f00470de5c4f))
* Add ProtocolComponent to types ([4451799](https://github.com/propeller-heads/tycho-indexer/commit/44517998a4b27d8eeb02e65f70112df8b3bd140e))
* allow unused imports. ([1c2c691](https://github.com/propeller-heads/tycho-indexer/commit/1c2c691df31ecf8842adc70a5af215422c867381))
* Create StorableProtocolType ([3bcd5f4](https://github.com/propeller-heads/tycho-indexer/commit/3bcd5f4a45093fb3571ac17dc3fe218945e5180c))
* Don't allow H160 ethereum specific type references. ([e40de71](https://github.com/propeller-heads/tycho-indexer/commit/e40de712404536924e4f7aa92a0fb9611f121e0f))
* implement delete_protocol_components ([5278322](https://github.com/propeller-heads/tycho-indexer/commit/52783226775a61826ecb7addb957e4ca1e83a576))
* Implement get_components ([1232e20](https://github.com/propeller-heads/tycho-indexer/commit/1232e20d80bab34402023463950ac4310f733512))
* insert one protocol type at a time. ([6a95292](https://github.com/propeller-heads/tycho-indexer/commit/6a9529238585044d411ba692c8ce462beef00b62))
* remove unused imports. ([7538b7f](https://github.com/propeller-heads/tycho-indexer/commit/7538b7f1b0bbc3de6fd517f20ae6e953b919eacb))

### ProtocolSystem

* Add unique constraint to name column ([1aac058](https://github.com/propeller-heads/tycho-indexer/commit/1aac058090f63f501443e6f496bdc243605a4748))
* Remove enum and retype to string ([dbf9cfd](https://github.com/propeller-heads/tycho-indexer/commit/dbf9cfd9bf19c64ea65445ccdf0cb343fd5a2326))

### ProtocolSystemEnumCache

* Add ([b7253a3](https://github.com/propeller-heads/tycho-indexer/commit/b7253a345789e429f14c1632f19ebca704375110))

### ProtocolSystemType

* Rename to ProtocolSystemDBEnum ([2edc4a0](https://github.com/propeller-heads/tycho-indexer/commit/2edc4a0c334540ab30f0ae4f38a2fe55e6897998))

### ProtocolType

* DB migration to make name unique. ([edba463](https://github.com/propeller-heads/tycho-indexer/commit/edba463c0450c594ba55af2e82c522198f49d133))
* Don't add to PostgresGateway generic type. ([2d4ee12](https://github.com/propeller-heads/tycho-indexer/commit/2d4ee128bb9449a05a7376fc1f0a6ad28b53b513))
* Fix docstring of upsert method ([c948e4d](https://github.com/propeller-heads/tycho-indexer/commit/c948e4d8d63fb998df3f04aedf926265edb20449))
* Move enums and structs back to models.rs ([106d916](https://github.com/propeller-heads/tycho-indexer/commit/106d916a78b12170055e7c2489901cc62aaf4a33))

### refactor

* add trace logs for all block cases in hybrid extractor ([8c492ea](https://github.com/propeller-heads/tycho-indexer/commit/8c492ea131e28d31c628c30e22eb0606e8a74cda))
* improve variable name and add this branch to CI build ([52f1d34](https://github.com/propeller-heads/tycho-indexer/commit/52f1d34aeea371ac000606d94b89b2857ffcec41))
* only shift block ts on Arbitrum ([a5c8535](https://github.com/propeller-heads/tycho-indexer/commit/a5c85354b9398258dc4646a7649e5afda31481b6))
* rename existing substreams config files ([c17bbb1](https://github.com/propeller-heads/tycho-indexer/commit/c17bbb1384485d963b846e3d1dd913c239e4ed93))
* rename existing substreams config files ([2ddbdab](https://github.com/propeller-heads/tycho-indexer/commit/2ddbdab42ec2266b50e8cf63cf771c27b49086ff))
* rename substreams folder into evm instead of ethereum ([94fac52](https://github.com/propeller-heads/tycho-indexer/commit/94fac52133e3743f7ca5f58dbeabbc90ffd43ccd))
* rename substreams folder into evm instead of ethereum ([ab3dd5e](https://github.com/propeller-heads/tycho-indexer/commit/ab3dd5e390dc31e2be763b9ba9eae2148e1a28d2))
* Update UniswapV2 formating ([e9369ac](https://github.com/propeller-heads/tycho-indexer/commit/e9369ac08e7b4da05239e9c6a6dbc8c97fa17ba8))
* Update UniswapV2 formating ([a2df53e](https://github.com/propeller-heads/tycho-indexer/commit/a2df53e366add7cff0253db026be7f5671897114))

### RPCClient

* Add pagination to get protocol states. ([71a6264](https://github.com/propeller-heads/tycho-indexer/commit/71a626431b8e1037df490f09807f85132f0510bb))

### Schema

* Add join relation between tx and component ([c48218e](https://github.com/propeller-heads/tycho-indexer/commit/c48218e85aecc35c8a1a6110819df58cdc14743e))
* Adjust table junction ([e356e3a](https://github.com/propeller-heads/tycho-indexer/commit/e356e3ae59855ff994f91f6c1dc339f4a6f336bd))

### StateSync

* Log number of states retrieved. ([4059bee](https://github.com/propeller-heads/tycho-indexer/commit/4059bee3175bc240c9c44975d65bd62f12bd2966))

### StateSynchronizer

* Avoid making empty requests. ([837ca59](https://github.com/propeller-heads/tycho-indexer/commit/837ca59e1b2dbd0d78268019976fe6e4d90d283a))
* Implement close mechanism. ([78bff4b](https://github.com/propeller-heads/tycho-indexer/commit/78bff4b936a7ca2d57077141d1ae1dd393bcd9f2))

### StorableComponent

* impl for evm::ProtocolComponent ([f7b131b](https://github.com/propeller-heads/tycho-indexer/commit/f7b131b372f3ba9a8ea532384285603fddf2bcd5))
* Initial interface implementation ([8719889](https://github.com/propeller-heads/tycho-indexer/commit/871988953d9691bd1c00fed7ba5f15600f364c0a))

### StorableProtocolComponent

* Adjust interface to take address instead of H160 ([73478e4](https://github.com/propeller-heads/tycho-indexer/commit/73478e4b041960abcc9e4587e439c103bba41714))
* Rename ([a0d3c50](https://github.com/propeller-heads/tycho-indexer/commit/a0d3c50ad2f56f02005bb72a2d4da487e5ab3fa6))
* Test renaming ([3929c75](https://github.com/propeller-heads/tycho-indexer/commit/3929c752b5e62344ea973884907073b63bf88b04))

### StorableProtocolType

* Update to and from storage methods. ([0f237ed](https://github.com/propeller-heads/tycho-indexer/commit/0f237ed529e15b4f11f75d2f683fa73bbbfdaa1b))

### StoredVersionedRow

* Remove get_valid_to ([96e12d8](https://github.com/propeller-heads/tycho-indexer/commit/96e12d8413d64a07089f0f8ad44ba42b018d8bef))

### TEMP

* build docker from dev branch ([b07b5b3](https://github.com/propeller-heads/tycho-indexer/commit/b07b5b3282f8d4acedb4c985581ec210e32ca6a9))
* Ignore reverts at startup. ([998a151](https://github.com/propeller-heads/tycho-indexer/commit/998a1511263606981a10e1b987ec9d0f6b13e19c))

### test_get_balance_deltas

* gix retrieving token id ([2726e8d](https://github.com/propeller-heads/tycho-indexer/commit/2726e8d04a496ab7e4ef72b9f23c0aa32365819f))

### test_to_storage_protocol_component

* test against static value. ([9692dce](https://github.com/propeller-heads/tycho-indexer/commit/9692dcefafe59c3994cebf4f6c52ba96e366fe9d))

### tests

* insert fixture data with versioning. ([82f3452](https://github.com/propeller-heads/tycho-indexer/commit/82f345209943356064b5858ef79bfb10ca15eee5))

### Tests

* Add data for testing ([25caeca](https://github.com/propeller-heads/tycho-indexer/commit/25caecaedd72369e11035d0e1b96fed5f2ad1c9a))

### TMP

* Add branch to docker until this is merged ([62b945b](https://github.com/propeller-heads/tycho-indexer/commit/62b945bfb67540cb1fdd26e275d1a63c56091822))
* usv3 and usv2 only process final blocks. ([352ebc0](https://github.com/propeller-heads/tycho-indexer/commit/352ebc03d6ace0bf2aa1793df4056cceadb35003))

### TokenPreProcessor

* Add Middleware generic ([ee5d172](https://github.com/propeller-heads/tycho-indexer/commit/ee5d17296c4d1e9fa283641476fe367fb71604e3))
* Load ABI on construction ([f59dcbc](https://github.com/propeller-heads/tycho-indexer/commit/f59dcbcc658ad27d5bc768048c17a4a79ad7d080))

### Transaction

* Add hash_by_id ([e29f7a6](https://github.com/propeller-heads/tycho-indexer/commit/e29f7a6b8bd3071e1fe6f7764d984725c638d387))
* remove hash_by_id ([c488f89](https://github.com/propeller-heads/tycho-indexer/commit/c488f891ca3a7dd6c3a0dbf8658f62471736fc1d))

### TransactionUpdates

* Adjust merge ([71a9eb3](https://github.com/propeller-heads/tycho-indexer/commit/71a9eb3984b06f5eac30024c15ad0d9d912f2e77))

### TransactionVMUpdates

* Change attr from vecs to hashmaps ([5f74243](https://github.com/propeller-heads/tycho-indexer/commit/5f7424397c67a17d83e6237a4a0443c09da859e1))
* Rename ([2a04f84](https://github.com/propeller-heads/tycho-indexer/commit/2a04f842403594c844d602e73ffe248074ea6a24))

### try_from_message

* Move to extractor/evm/mod file ([76b958d](https://github.com/propeller-heads/tycho-indexer/commit/76b958de9d1e6c8ed12331dfd7daf5d3a4c0b914))

### TvlChange

* Add component_id ([45086d2](https://github.com/propeller-heads/tycho-indexer/commit/45086d212479d52dd8b1bb524a796ca623971a40))
* Change modify_tx type to H256 ([15dcec4](https://github.com/propeller-heads/tycho-indexer/commit/15dcec4d623f38a6b96e8e1118f9959ae9921f14))
* Move struct ([e126337](https://github.com/propeller-heads/tycho-indexer/commit/e1263372e9bd3b5bfaa9f5ee17d52535add5c47f))
* Settle for type H160 instead of generic ([c8bcdae](https://github.com/propeller-heads/tycho-indexer/commit/c8bcdaed87b6e809f3470416b4602a5f5b9d696c))

### TxUpdates

* Adjust from single AccUpdate to handling Vec<AccUpdate> ([dfd7536](https://github.com/propeller-heads/tycho-indexer/commit/dfd7536ae754869a3546853b85fcc37863da8748))

### UniswapV2

* Aggregate pools per tx. ([a1671ef](https://github.com/propeller-heads/tycho-indexer/commit/a1671ef36da315903f318b732c2bdccdbebb87e1))

### update

* Cargo.lock ([9b129ef](https://github.com/propeller-heads/tycho-indexer/commit/9b129efa09bcd1956b56fa4c2ad1724d3a1dda12))

### upsert_components

* Expand test ([26bd44e](https://github.com/propeller-heads/tycho-indexer/commit/26bd44e1f3f2280311f3751b28a15a38e1b20d90))
* Remove chain ID from interface ([1ce2604](https://github.com/propeller-heads/tycho-indexer/commit/1ce26046e49c52e08a312b711becd05e78801dc4))

### ValueIdTableCache

* Rename from EnumTableCache ([9c6a8ff](https://github.com/propeller-heads/tycho-indexer/commit/9c6a8ff3bb3cca34a8275b9d4f238a18057cc565))

### WIP

* BalanceChange insertion - More small fixes and typos. ([c229af2](https://github.com/propeller-heads/tycho-indexer/commit/c229af28ab57a47291055cd2046deb051310d8f3))
* BalanceChange insertion method fix - it compiles now. ([ee95bfa](https://github.com/propeller-heads/tycho-indexer/commit/ee95bfa31cb83bcb2a771c96dbaf082d97738839))
* ComponentBalance delta versioning: get previous value from db. ([cdd4745](https://github.com/propeller-heads/tycho-indexer/commit/cdd47451961d4f03bcea3b8743ed49d27b4a4533))
* get_balance_deltas initial attempt ([d3d08a0](https://github.com/propeller-heads/tycho-indexer/commit/d3d08a0e7ef7e806dbd2501db3304132e8ed4947))
* Insert TvlChange to db. ([aa20611](https://github.com/propeller-heads/tycho-indexer/commit/aa20611500376fae3efcdc58150da8b20e41be42))
* Make extractor creation more flexible ([02afd15](https://github.com/propeller-heads/tycho-indexer/commit/02afd1513fb91d3fa7d63d18f864e2fcd0a68f92))
* More steps towards inserting TvlChange to db. ([a0c9cd6](https://github.com/propeller-heads/tycho-indexer/commit/a0c9cd621fef305551072e451d9873ed364f8622))
* ProtocolGateway: Use StorableProtocolType ([439f166](https://github.com/propeller-heads/tycho-indexer/commit/439f166d46a1e74c6a186745b6597c320b1134c3))
* Substreams TVL extraction on ambient swap calls. ([e540b8e](https://github.com/propeller-heads/tycho-indexer/commit/e540b8e9daec7d95a63e39c25772ac0ec40d316c))
* Tycho Client ([2556e43](https://github.com/propeller-heads/tycho-indexer/commit/2556e43f9c90a31812eecdd4a6552c159508d32f))
