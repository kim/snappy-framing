# Snappy Framing Format

[![Build Status](https://secure.travis-ci.org/kim/snappy-framing.png)](http://travis-ci.org/kim/snappy-framing)

Implementation of the [Snappy framing format](http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt?r=71),
SVN rev 71.

## Build, Install, Use

Build from source using `cabal install`, from Hackage using `cabal install
snappy-framing`.

The library exports a single module, `Codec.Compression.Snappy.Framing`, with a
instance of `Data.Binary` doing all the work. A couple of convenience functions
are also provided, but for serious stream processing use of a streaming library
of your liking (eg. pipes, iteratees, conduit) is recommended.

## Bugs and Contributing

Please report issues via [Github Issues](https://github.com/kim/snappy-framing/issues),
patches are welcome as [Pull Requests](https://github.com/kim/snappy-framing/pulls).

## License

Mozilla Public License Version 2.0, see LICENSE file.
