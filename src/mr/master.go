package mr

import (
	"errors"
	"log"
	"strconv"
	"strings"
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

	return nil
}

func (m *Master) MarkJobDone(args *JobDoneReq, response *JobDoneResp) error {
	m.masterGlobalLock.Lock()
	defer m.masterGlobalLock.Unlock()

	if doneJob, ok := m.JobsInProgress[args.JobId]; ok { // If job doesn't exists - do nothing, probably it has been completed by another worker
		if doneJob.JobType == MAP {
			if m.masterPhase != MAP_PHASE || m.MapTasksInProgressCount == 0 {
				return nil
			} // Do not accept stalled job results

			m.MapTasksInProgressCount -= 1
			delete(m.JobsInProgress, doneJob.JobId)
			m.IntermediateKeysFiles = append(m.IntermediateKeysFiles, args.JobOutput...)

			if m.MapTasksInProgressCount == 0 && len(m.JobsToBeDone) == 0 {
				m.masterPhase = REDUCE_PHASE
				m.prepareIntermediateKeysForReduceTasks()
			}
		} else if doneJob.JobType == REDUCE {
			if m.masterPhase != REDUCE_PHASE || m.ReduceTasksInProgressCount == 0 {
				return nil
			} // Do not accept stalled jobs

			m.ReduceTasksInProgressCount -= 1
			delete(m.JobsInProgress, doneJob.JobId)
			if m.ReduceTasksInProgressCount == 0 && len(m.JobsToBeDone) == 0 {
				m.DoneFlag = true
				m.masterPhase = DONE_PHASE
			}
		}
	}

	return nil
}

func (m *Master) prepareIntermediateKeysForReduceTasks() {
	result := make(map[int]Job)

	for _, intermediateFile := range m.IntermediateKeysFiles {
		reduceTaskId, err := strconv.Atoi(strings.Split(intermediateFile, "-")[2])
		if err != nil {
			log.Fatalf("Error while parsing file name "+intermediateFile, err)
		}

		if job, ok := result[reduceTaskId]; ok {
			job.FileNames = append(job.FileNames, intermediateFile)
			result[reduceTaskId] = job
		} else {
			result[reduceTaskId] = Job{
				JobId:        reduceTaskId,
				FileNames:    []string{intermediateFile},
				JobType:      REDUCE,
				JobStartTime: time.Now(),
			}
		}
	}

	m.JobsToBeDone = result
}

func (m *Master) stalledJobsHealthcheck(jobTimeoutSec int) {
	for {
		m.masterGlobalLock.Lock()

		for jobId, job := range m.JobsInProgress {
			jobExpirationTime := job.JobStartTime.Add(time.Second * time.Duration(jobTimeoutSec))
			if time.Now().After(jobExpirationTime) {

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
