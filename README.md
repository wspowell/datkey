# datkey

Which key? Dat key. Dat one.

This library provides an in-memory key/value store for a single process, intended for use in diskey.

# Benchmarks

```
go test -benchmem -bench . ./
goos: linux
goarch: amd64
pkg: github.com/wspowell/datkey
cpu: AMD Ryzen 9 4900HS with Radeon Graphics         
BenchmarkDatKeySet_sync-8                        6529369               198.5 ns/op           128 B/op          2 allocs/op
BenchmarkDatKeySet_async-8                       4725535               246.3 ns/op           128 B/op          2 allocs/op
BenchmarkDatKeySet_multikey_sync-8               1000000              1099 ns/op             522 B/op          2 allocs/op
BenchmarkDatKeySet_multikey_async-8             11551862               144.3 ns/op           167 B/op          2 allocs/op
BenchmarkDatKeyGet_sync-8                        5769628               193.2 ns/op            56 B/op          2 allocs/op
BenchmarkDatKeyGet_async-8                       4065962               270.7 ns/op            56 B/op          2 allocs/op
BenchmarkDatKeyGet_multikey_sync-8               1777310               681.8 ns/op            56 B/op          2 allocs/op
BenchmarkDatKeyGet_multikey_async-8             12924740                96.51 ns/op           56 B/op          2 allocs/op
BenchmarkDatKeySet_sync_with_TTL-8               4245423               281.5 ns/op           128 B/op          2 allocs/op
BenchmarkDatKeySet_async_with_TTL-8              3462932               331.1 ns/op           128 B/op          2 allocs/op
BenchmarkDatKeySet_multikey_sync_with_TTL-8      1000000              1216 ns/op             522 B/op          2 allocs/op
BenchmarkDatKeySet_multikey_async_with_TTL-8    10445972               152.5 ns/op           169 B/op          2 allocs/op
BenchmarkDatKeyGet_sync_with_TTL-8               5753263               188.1 ns/op            56 B/op          2 allocs/op
BenchmarkDatKeyGet_async_with_TTL-8              3634720               287.6 ns/op            56 B/op          2 allocs/op
BenchmarkDatKeyGet_multikey_sync_with_TTL-8      1767836               677.7 ns/op            56 B/op          2 allocs/op
BenchmarkDatKeyGet_multikey_async_with_TTL-8    12291062                97.57 ns/op           56 B/op          2 allocs/op
```

```
go test -benchmem -bench . ./lib/errors
goos: linux
goarch: amd64
pkg: datkey/lib/errors
cpu: AMD Ryzen 9 4900HS with Radeon Graphics         
BenchmarkErrorsNewGolang-8              33139898                35.25 ns/op           16 B/op          1 allocs/op
BenchmarkErrorsNew-8                    24317346                49.97 ns/op           64 B/op          1 allocs/op
BenchmarkErrorsWrapGolang-8              6140090               177.6 ns/op            48 B/op          2 allocs/op
BenchmarkErrorsNewFromError-8           24258693                51.13 ns/op           64 B/op          1 allocs/op
BenchmarkErrorsErrorGolang-8            1000000000               0.3668 ns/op          0 B/op          0 allocs/op
BenchmarkErrorsError-8                  350791375                3.394 ns/op           0 B/op          0 allocs/op
```