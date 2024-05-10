#!/bin/bash
go test -coverpkg ./... -coverprofile cover.out -v -count=1 github.com/steinarvk/poindexter/tests/integration
