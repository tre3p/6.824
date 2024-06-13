package mr

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Input
	ReduceBucketsSize int

	// Lock
	masterGlobalLock sync.Mutex

	// Common
	masterPhase MasterPhase

	JobsToBeDone   map[int]Job
	JobsInProgress map[int]Job

	MapTasksInProgressCount    int
	ReduceTasksInProgressCount int

	DoneFlag bool

	IntermediateKeysFiles []string
}

type MasterPhase int32

const (
	MAP_PHASE MasterPhase = iota
	REDUCE_PHASE
	DONE_PHASE
)

type Job struct {
	JobId        int
	FileNames    []string
	JobType      JobType
	JobStartTime time.Time
}

func (m *Master) getJobToBeDoneId() (int, error) {
	if len(m.JobsToBeDone) <= 0 {
		return -1, errors.New("No more jobs to be done")
	}

	var k int
	for key := range m.JobsToBeDone {
		k = key
		break
	}

	return k, nil
}

// Your code here -- RPC handlers for the worker to call.
func handleGetJob(m *Master, response *GetJobResponse) {
	jobId, err := m.getJobToBeDoneId()
	if err != nil {
		response.JobType = WAIT
		response.JobId = -1
		return
	}

	job := m.JobsToBeDone[jobId]
	job.JobStartTime = time.Now()
	delete(m.JobsToBeDone, jobId)
	m.JobsInProgress[jobId] = job

	response.JobType = job.JobType
	response.JobId = job.JobId
	response.FileNames = job.FileNames
	response.ReduceTasksBucketSize = m.ReduceBucketsSize
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) GetJob(args *GetJobRequest, response *GetJobResponse) error {
	m.masterGlobalLock.Lock()
	defer m.masterGlobalLock.Unlock()

	fmt.Println("Got 'GetJob' request")

	switch m.masterPhase {
	case MAP_PHASE:
		handleGetJob(m, response)
		if response.JobId != -1 {
			m.MapTasksInProgressCount += 1
		}
	case REDUCE_PHASE:
		handleGetJob(m, response)
		if response.JobId != -1 {
			m.ReduceTasksInProgressCount += 1
		}
	case DONE_PHASE:
		response.JobType = NO_MORE_JOB
	}

	if response.JobId != -1 {
		fmt.Println("Sending job ID " + strconv.Itoa(response.JobId))
	} else {
		fmt.Printf("Sending %v job type\n", response.JobType)
	}

	return nil
}

func (m *Master) MarkJobDone(args *JobDoneReq, response *JobDoneResp) error {
	m.masterGlobalLock.Lock()
	defer m.masterGlobalLock.Unlock()

	fmt.Println("Got 'MarkJobDone' for job " + strconv.Itoa(args.JobId))

	if doneJob, ok := m.JobsInProgress[args.JobId]; ok { // If job doesn't exists - do nothing, probably it has been completed by another worker
		if doneJob.JobType == MAP {
			_, jobExists := m.JobsInProgress[args.JobId]
			if m.masterPhase != MAP_PHASE || m.MapTasksInProgressCount == 0 || !jobExists {
				fmt.Println("Got stalled MAP job result from worker, ignoring")
				return nil
			} // Do not accept stalled job results

			m.MapTasksInProgressCount -= 1
			fmt.Println(m.MapTasksInProgressCount)
			delete(m.JobsInProgress, doneJob.JobId)
			m.IntermediateKeysFiles = append(m.IntermediateKeysFiles, args.JobOutput...)

			if m.MapTasksInProgressCount == 0 && len(m.JobsToBeDone) == 0 {
				m.masterPhase = REDUCE_PHASE
				fmt.Println("Master moved to REDUCE phase")
				// TODO: trigger pulling reduce tasks based on retrieved intermediate keys
			}
		} else if doneJob.JobType == REDUCE {
			_, jobExists := m.JobsInProgress[args.JobId]

			if m.masterPhase != REDUCE_PHASE || m.ReduceTasksInProgressCount == 0 || !jobExists {
				fmt.Println("Got stalled REDUCE job result from worker, ignoring")
				return nil
			} // Do not accept stalled jobs

			m.ReduceTasksInProgressCount -= 1
			delete(m.JobsInProgress, doneJob.JobId)
			if m.ReduceTasksInProgressCount == 0 && len(m.JobsToBeDone) == 0 {
				m.DoneFlag = true
				m.masterPhase = DONE_PHASE
				fmt.Println("Master moved to DONE phase")
			}
		}
	}

	return nil
}

func (m *Master) stalledJobsHealthcheck(jobTimeoutSec int) {
	for {
		m.masterGlobalLock.Lock()

		for jobId, job := range m.JobsInProgress {
			jobExpirationTime := job.JobStartTime.Add(time.Second * time.Duration(jobTimeoutSec))
			if time.Now().After(jobExpirationTime) {
				fmt.Println("Job with ID " + strconv.Itoa(jobId) + " is stalled, evicting..")

				if job.JobType == MAP {
					m.MapTasksInProgressCount -= 1
				} else if job.JobType == REDUCE {
					m.ReduceTasksInProgressCount -= 1
				}

				delete(m.JobsInProgress, jobId)
				m.JobsToBeDone[jobId] = job
			}
		}

		m.masterGlobalLock.Unlock()
		time.Sleep(time.Second * 5)
	}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.DoneFlag
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	jobsToBeDone := make(map[int]Job, len(files))
	for idx, file := range files {
		jobsToBeDone[idx] = Job{
			JobId:        idx,
			FileNames:    []string{file},
			JobType:      MAP,
			JobStartTime: time.Now(),
		}
	}

	m := Master{
		masterPhase:                MAP_PHASE,
		ReduceBucketsSize:          nReduce,
		masterGlobalLock:           sync.Mutex{},
		MapTasksInProgressCount:    0,
		ReduceTasksInProgressCount: 0,
		DoneFlag:                   false,
		JobsInProgress:             make(map[int]Job),
		JobsToBeDone:               jobsToBeDone,
		IntermediateKeysFiles:      []string{},
	}

	go m.server()
	go m.stalledJobsHealthcheck(10)

	return &m
}
