package mr

import "os"
import "strconv"

type JobType int32

const (
	MAP JobType = iota
	REDUCE
	WAIT
	NO_MORE_JOB
)

func (jt JobType) String() string {
	switch jt {
	case MAP:
		return "MAP"
	case REDUCE:
		return "REDUCE"
	case WAIT:
		return "WAIT"
	case NO_MORE_JOB:
		return "NO_MORE_JOB"
	}

	return "UNDEFINED"
}

type GetJobRequest struct{}
type GetJobResponse struct {
	JobId                 int
	FileNames             []string
	JobType               JobType
	ReduceTasksBucketSize int
}

type JobDoneReq struct {
	JobId     int
	JobOutput []string
	JobType   JobType
}
type JobDoneResp struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
