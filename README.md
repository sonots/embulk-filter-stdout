# Stdout filter plugin for Embulk

Embulk filter plugin to print embulk records to stdout

## Overview

* **Plugin type**: filter

## Configuration

- no option

## Example

```yaml
filters:
  - type: stdout
```

## ChangeLog

[CHANGELOG.md](CHANGELOG.md)

## ToDo

* Write test

## Development

Run example:

```
$ ./gradlew classpath
$ embulk run -I lib example.yml
```

Run test:

```
$ ./gradlew test
```

Release gem:

```
$ ./gradlew gemPush
```
