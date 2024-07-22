# Mit-go
This is a repository for labs of mit 6.824. The original repository is: [6.824-golabs-2022
](https://github.com/keenJoe/6.824-golabs-2022)

## Map reduce
### Build
Init:
```
cd src
go mod init 6.824
```

Build mr apps:
```
cd src/main
go build -race -buildmode=plugin ../mrapps/wc.go
```

### Run
Run sequential map reduce: (-> mr-out-*)
```
go run -race mrsequential.go wc.so pg*.txt 
```

Run distributed map reduce:
```
rm mr-out*
go run -race mrcoordinator.go pg-*.txt
go run -race mrworker.go wc.so
```

### Implementation
main/mrcoordinator.go and main/mrworker.go;  
mr/coordinator.go, mr/worker.go, and mr/rpc.go.