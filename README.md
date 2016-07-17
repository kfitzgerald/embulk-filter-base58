# Base58 filter plugin for Embulk

[![Build Status](https://travis-ci.org/kfitzgerald/embulk-filter-base58.svg?branch=master)](https://travis-ci.org/kfitzgerald/embulk-filter-base58) ![Coverage Status](https://coveralls.io/repos/github/kfitzgerald/embulk-filter-base58/badge.svg?branch=master)](https://coveralls.io/github/kfitzgerald/embulk-filter-base58?branch=master)

Embulk filter plugin to convert hex to base58 and vice versa.

## Overview

* **Plugin type**: filter

## Configuration

- **columns**: Columns to encode/decode (array, required)
  - **name**: Name of input column (string, required)
  - **encode**: Whether to encode or decode the value. (boolean, default:`true`)
  - **prefix**: Adds a prefix when encoding, or strips the prefix when decoding. (string, default:`""`)
  - **new_name**: New column name if you want to rename (string, default: `null`)

## Example

```yaml
filters:
  - type: base58
    columns:
    - { name: _id }
    - { name: account_id, encode: true, prefix: account_, new_name: public_account_id }
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

*Hat tip to [kamatama41/embulk-filter-hash](https://github.com/kamatama41/embulk-filter-hash)*