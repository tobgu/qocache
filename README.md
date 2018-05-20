Qocache is a Go port of [Qcache](https://github.com/tobgu/qcache).

## Functionality in Qocache that does not exist in Qcache
* Defined enum order
* Key-value fields
* Configuration possible throught environment variables and config
  file in addition to command line arguments.

## Functionality in Qcache still missing in Qocache
* Subqueries in `in` clause
* TLS support, server and client certificates
* Basic auth support
* Graceful termination of clients on SIGTERM