# datkey

Which key? Dat key. Dat one.

This library provides an in-memory key/value store for a single process, intended for use in diskey.

# Benchmarks

go test -benchmem -bench . ./
goos: linux
goarch: amd64
pkg: datkey
cpu: AMD Ryzen 9 4900HS with Radeon Graphics         
BenchmarkDatKeySet_sync-8                 817545              1367 ns/op             258 B/op          3 allocs/op
BenchmarkDatKeySet_async-8               1256470               913.8 ns/op           248 B/op          3 allocs/op
BenchmarkDatKeySet_multikey_sync-8        519902              3184 ns/op             587 B/op          3 allocs/op
BenchmarkDatKeySet_multikey_async-8      2462672               519.8 ns/op           265 B/op          3 allocs/op
BenchmarkDatKeyGet_sync-8                 928467              1244 ns/op              98 B/op          2 allocs/op
BenchmarkDatKeyGet_async-8               1000000              1033 ns/op              89 B/op          2 allocs/op
BenchmarkDatKeyGet_multikey_sync-8        378552              2739 ns/op              78 B/op          2 allocs/op
BenchmarkDatKeyGet_multikey_async-8      2969695               455.9 ns/op            72 B/op          2 allocs/op

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