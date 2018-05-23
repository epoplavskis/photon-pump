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


[0.4]: https://github.com/madecom/photon-pump/compare/v0.3.0...v0.4.0
[0.3]: https://github.com/madecom/photon-pump/compare/v0.2.5...v0.3
[0.2.5]: https://github.com/madecom/photon-pump/compare/v0.2.4...v0.2.5
