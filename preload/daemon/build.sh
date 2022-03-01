#!/usr/bin/env bash
RootPath=$(cd $(dirname $0)/..; pwd)

Version=`git describe --abbrev=0 --tags 2>/dev/null`
BranchName=`git rev-parse --abbrev-ref HEAD 2>/dev/null`
CommitID=`git rev-parse HEAD 2>/dev/null`
BuildTime=`date +%Y-%m-%d\ %H:%M`

SrcPath=${RootPath}/preload/daemon
case `uname` in
    Linux)
        TargetFile=${1:-${SrcPath}/cfs-preload-daemon}
        ;;
    *)
        echo "Unsupported platform"
        exit 0
        ;;
esac

[[ "-$GOPATH" == "-" ]] && { echo "GOPATH not set"; exit 1; }

LDFlags="-X github.com/chubaofs/chubaofs/proto.Version=${Version} \
    -X github.com/chubaofs/chubaofs/proto.CommitID=${CommitID} \
    -X github.com/chubaofs/chubaofs/proto.BranchName=${BranchName} \
    -X 'github.com/chubaofs/chubaofs/proto.BuildTime=${BuildTime}' "

go build \
    -ldflags "${LDFlags}" \
    -o $TargetFile \
    ${SrcPath}/*.go
