# logspout-fluentd

Logspout module for forwarding logs to fluentd.

## Usage

This module works by acting as a fluentd forwarder. Configure Logspout to receive forwarded messages, something like this:

```
<source>
  type forward
  port 24224
  bind 0.0.0.0
</source>
```