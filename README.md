## Exchange Rate Collector in Go

### Requirements
- Scale horizontally (can run in multiple machines and utilize all cores)
- Fast (native binaries)
- Portable (build for MacOSX, Linux, Windows, FreeBSD and different arch x86_64, x86_32, ARM)
- Multicore support (using light weight goroutines)
- Static binaries (no dependencies one binary)

### How does it work

1. Get a payload for exchange rate from beanstalkd
  {
	from: "USD",
	to: "HKD"
  }
2. According to job description it parsers xe.com for required exchange rates.
3. If getting exchange rate successful put job back with 1 min delay, repeat job for 10 successful times, every time data is saved to mongodb with a timestamp, then that currency converstaion job is done.
4. If there is any problem during the get rate, retry it with a 3s delay
5. If failed more than 3 times in total (not consecutive), job buried.

### How to use
1. Get Go compiler https://golang.org/dl/
2. Install it https://golang.org/doc/install, set GOPATH env
3. Get it
```
go get github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ
```
4. go to repository
```
cd $GOPATH/src/github.com/mmirolim/HsNlaEWBgkaYrFKu2BQHSQ
```
5. build executable with
```
go get ./...
go build -o app
```
6. run it
```
./app
```
7. There is producer  in producer dir
```
cd producer
go run job-producer.go -n 1 -f USD -t HKD
```

### TODO
1. Add Makefile
2. Add tests


