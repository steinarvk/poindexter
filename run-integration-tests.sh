#!/bin/bash
go test -coverpkg ./... -coverprofile cover.out -count=1 github.com/steinarvk/poindexter/tests/integration
