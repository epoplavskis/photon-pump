## [0.7.2] - 2019-01-29
Fixed: Iterators restart at the last processed event number when the connection drops.
Refactor: MessageReader returns a TcpCommand in the header rather than an int.
Chore: Removed unused dependencies.

## [0.7.1] - 2019-01-29
Fixed: Volatile subscriptions fail to restart when the connection is recreated.

## [0.7.0] - 2019-01-29
Fixed: Volatile subscriptions fail to yield all events for a projection.
This was caused by a confusion between the linked event and original event.

### Breaking Changes
 - `Event.original_event` is now `Event.received_event` because the original name was unclear.
 - `Event.event_number` is now equal to the value of `received_event.event_number`, not the value of the linked event number.

## [0.6.0.1] - 2019-01-03
Add automagic deployment to pypi with Travis and Versioneer

## [0.6.0] - 2018-12-21
Added batch size param to subscribe_to method

## [0.6.0-alpha-5] - 2018-11-09
Fixed: CreatePersistentSubscription command was never cleaned up after success

## [0.6.0-alpha-4] - 2018-10-05
Fixed: We now handle deleted messages correctly.

## [0.6.0-alpha-2] - 2018-09-17
Discovery now supports "selectors" to control how we pick a node from gossip

## [0.6.0-alpha-1] - 2018-09-14
Added support for catch-up subscriptions.

## [0.5] - 2018-04-27
### Breaking changes
- Dropped the ConnectionContextManager class.
- "Connection" class is now "Client" and acts as a context manager in its own right
- Rewrote the connection module completely.
- PersistentSubscriptions no longer use a maxsize parameter when creating a streaming iterator. This is a workaround for https://github.com/madedotcom/photon-pump/issues/49

## [0.4] - 2018-04-27
### Fixes
- Added cluster discovery for HA scenarios.

## [0.3] - 2018-04-11
### Fixes
- `iter` properly supports iterating a stream in reverse.
### Breaking change
- `published_event` reversed order of type and stream


[0.7.2]: https://github.com/madedotcom/photon-pump/compare/v0.7.1..v0.7.2
[0.7.1]: https://github.com/madedotcom/photon-pump/compare/v0.7.0..v0.7.1
[0.7.0]: https://github.com/madedotcom/photon-pump/compare/v0.6.0.1..v0.7.0
[0.6.0.1]: https://github.com/madedotcom/photon-pump/compare/v0.6.0..v0.6.0.1
[0.6.0]: https://github.com/madedotcom/photon-pump/compare/v0.6.0-alpha-5..v0.6.0
[0.6.0-alpha-5]: https://github.com/madedotcom/photon-pump/compare/v0.6.0-alpha-4..v0.6.0-alpha-5
[0.6.0-alpha-4]: https://github.com/madedotcom/photon-pump/compare/v0.6.0-alpha-2..v0.6.0-alpha-4
[0.6.0-alpha-2]: https://github.com/madedotcom/photon-pump/compare/v0.6.0-alpha-1..v0.6.0-alpha-2
[0.6.0-alpha-2]: https://github.com/madedotcom/photon-pump/compare/v0.6.0-alpha-1..v0.6.0-alpha-2
[0.6.0-alpha-1]: https://github.com/madedotcom/photon-pump/compare/v0.5.0..v0.6.0-alpha-1
[0.5]: https://github.com/madedotcom/photon-pump/compare/v0.4.0..v0.5.0
[0.4]: https://github.com/madedotcom/photon-pump/compare/v0.3.0..v0.4.0
[0.3]: https://github.com/madedotcom/photon-pump/compare/v0.2.5..v0.3
[0.2.5]: https://github.com/madedotcom/photon-pump/compare/v0.2.4..v0.2.5
