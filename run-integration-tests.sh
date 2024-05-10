#!/bin/bash
go test -v -coverpkg ./... -coverprofile cover.out -count=1 github.com/steinarvk/poindexter/tests/integration
