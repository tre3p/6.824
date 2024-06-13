package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFile(fileName string) string {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %s", fileName)
	}

	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %s", fileName)
	}

	return string(content)
}

func handleMapJob(fileToMap string, reduceTasksSize int, jobId int, mapf func(string, string) []KeyValue) {
	intermediateKeys := mapf(fileToMap, readFile(fileToMap))
	intermediateKeysSplitted := splitIntermediateKeys(reduceTasksSize, intermediateKeys)
	var outputFiles []string

	for reduceTaskId, intermediateKv := range intermediateKeysSplitted {
		intermediateKeysFileName := fmt.Sprintf("mr-%s-%s", strconv.Itoa(jobId), strconv.Itoa(reduceTaskId))

		f, err := ioutil.TempFile(".", "tmp_"+intermediateKeysFileName)
		if err != nil {
			log.Fatalf("error while creating temporary file")
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		for _, kv := range intermediateKv {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Error while encoding intermediate file")
			}
		}

		// TODO: There can be situation when other worker creating the same files. I guess we need to check if file with such name exists
		// and if it exists - proceed to other kv's
		err = os.Rename(f.Name(), intermediateKeysFileName)
		if err != nil {
			log.Fatalf("Error while renaming temporary intermediate keys file", err)
		}
		outputFiles = append(outputFiles, intermediateKeysFileName)
	}

	CallJobDone(jobId, outputFiles, MAP)
}

func splitIntermediateKeys(reduceTasksSize int, intermediateKeys []KeyValue) map[int][]KeyValue {
	result := make(map[int][]KeyValue)

	for _, kv := range intermediateKeys {
		bucketIdx := ihash(kv.Key) % reduceTasksSize
		if bucket, ok := result[bucketIdx]; ok {
			bucket = append(bucket, kv)
			result[bucketIdx] = bucket
		} else {
			result[bucketIdx] = []KeyValue{kv}
		}
	}

	return result
}

func handleReduceJob(filesToReduce []string, jobId int, reducef func(string, []string) string) {
	var kva []KeyValue

	for _, file := range filesToReduce {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("error while opening file "+file, err)
		}

		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))
	outFileName := "mr-out-" + strconv.Itoa(jobId)
	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatalf("Error while creating file "+outFileName, err)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	CallJobDone(jobId, []string{outFileName}, REDUCE)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		job, err := CallGetJob()
		if err != nil {
			return // Master is not available, probably global task is done
		}

		switch job.JobType {
		case MAP:
			handleMapJob(job.FileNames[0], job.ReduceTasksBucketSize, job.JobId, mapf)
		case REDUCE:
			handleReduceJob(job.FileNames, job.JobId, reducef)
		case NO_MORE_JOB:
			return
		case WAIT:
			// Do nothing
		}

		time.Sleep(time.Second * 2)
	}

}

func CallJobDone(jobId int, jobOutputFiles []string, jobType JobType) {
	args := JobDoneReq{
		JobId:     jobId,
		JobOutput: jobOutputFiles,
		JobType:   jobType,
	}
	reply := JobDoneResp{}

	call("Master.MarkJobDone", &args, &reply)
}

func CallGetJob() (GetJobResponse, error) {
	args := GetJobRequest{}
	reply := GetJobResponse{}

	success := call("Master.GetJob", &args, &reply)
	if !success {
		return reply, errors.New("Unable to get job")
	}

	return reply, nil
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
