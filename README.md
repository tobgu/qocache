[![Build Status](https://travis-ci.org/tobgu/qocache.png)](https://travis-ci.org/tobgu/qocache)
[![Go Report Card](https://goreportcard.com/badge/github.com/tobgu/qocache)](https://goreportcard.com/report/github.com/tobgu/qocache)

Qocache is a Go port of [Qcache](https://github.com/tobgu/qcache).

## Functionality in Qocache that does not exist in Qcache
* Defined enum order
* Key-value fields
* Configuration possible through environment variables and config
  file in addition to command line arguments.
* LZ4 frame based compression

## Functionality in Qcache planned but still missing in Qocache
* Subqueries in `in` clause

## Functionality in Qcache not planned in Qocache
* GZIP compression support in HTTP request/response
