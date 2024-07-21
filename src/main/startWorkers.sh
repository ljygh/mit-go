#!/usr/bin/env bash
app=$1 

go run -race mrworker.go $app > worker1.log &
go run -race mrworker.go $app > worker2.log &
go run -race mrworker.go $app > worker3.log &
go run -race mrworker.go $app> worker4.log &
go run -race mrworker.go $app > worker5.log &