# Change Log

## 2.6.0 [#84](https://github.com/singer-io/tap-quickbooks/pull/84)
   * Streams the credentials cannot access (403) are now excluded from the catalog during discovery. If credentials cannot access any supported streams, discovery still fails with a 403.
   * Batch-query stream access is probed with a single combined request instead of one call per stream, cutting discovery from 31 calls down to as few as 3 when fully authorized.
   * A plain sync run without an explicit `--catalog` no longer pays the per-stream access-check cost.
   * Added unit tests for discovery access checks and bookmark handling.
   * Bump `singer-python` to 6.8.0, `requests` to 2.34.2.

## 2.5.0
   * Add authentication check during discovery [#83](https://github.com/singer-io/tap-quickbooks/pull/83)

## 2.4.2
   * Add sandbox authentication helper text to error message [#82](https://github.com/singer-io/tap-quickbooks/pull/82)

## 2.4.1
   * Remove unused `patternProperties` from schemas [#81](https://github.com/singer-io/tap-quickbooks/pull/81)

## 2.4.0
   * Use batch endpoint for querying basic streams [#80](https://github.com/singer-io/tap-quickbooks/pull/80)

## 2.3.3
   * Increase default page size to 1000 [#79](https://github.com/singer-io/tap-quickbooks/pull/79)

## 2.3.2
   * Pylint updates [#78](https://github.com/singer-io/tap-quickbooks/pull/78)

## 2.3.1
   * Bump dependency versions for twistlock compliance [#75](https://github.com/singer-io/tap-quickbooks/pull/75)

## 2.3.0

   * Upgrade latest API minor version (75) [#74](https://github.com/singer-io/tap-quickbooks/pull/74)
   * Add new fields in the schema

## 2.2.0

   * Revise backoff logic to handle 429 error [#72](https://github.com/singer-io/tap-quickbooks/pull/72)

## 2.1.0

   * Add support for dev mode [#64](https://github.com/singer-io/tap-quickbooks/pull/64)

## 2.0.0

   * Updated field types and added new fields as per the doc [#58](https://github.com/singer-io/tap-quickbooks/pull/58)
   * Added latest minor version (65) in the API requests [#57](https://github.com/singer-io/tap-quickbooks/pull/57)
   * Added new stream - CustomerType [#62](https://github.com/singer-io/tap-quickbooks/pull/62)
   * Added custom exception handling [#56](https://github.com/singer-io/tap-quickbooks/pull/56)
   * Added missing tap tester & unit tests [#61](https://github.com/singer-io/tap-quickbooks/pull/61) [#59](https://github.com/singer-io/tap-quickbooks/pull/59)

## 1.1.2

   * Updated manifest file to fix issues created in 1.1.1 deployment [#53](https://github.com/singer-io/tap-quickbooks/pull/53)
## 1.1.1

   * Updated Schemas with missing fields [#50](https://github.com/singer-io/tap-quickbooks/pull/50)
   * Request Timeout [#48](https://github.com/singer-io/tap-quickbooks/pull/48)

## 1.1.0

   * Add ProfitAndLoss report stream [#37] (https://github.com/singer-io/tap-quickbooks/pull/37)
   * Update custom fields [#36] (https://github.com/singer-io/tap-quickbooks/pull/36)

## 1.0.4

* Bumps `singer-python` to `5.12.1`

## 1.0.3

* Added AcctNum and DocNumber fields

## 1.0.2

* Fix EffectiveTaxRates issue in tax_rates schema

## 1.0.1

* Fix bug with sandbox check

## 1.0.0

* Releasing GA

## 0.1.1

* Adding many fields to schemas and using patternProperties for others [#17](https://github.com/singer-io/tap-quickbooks/pull/17)

## [0.1.0] - 2020-09-03

* Prepare for beta
* Add testing and streams

## [0.0.1] - 2020-08-04
