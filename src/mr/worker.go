package mr

import (
	"fmt"
)
import "log"
import "time"
import "encoding/json"
import "io"
import "sort"
import "path/filepath"
import "os"
import "strings"

import "net/rpc"
import "hash/fnv"

const CALLINTERVAL = time.Second * 2

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func parse_name(task string) ([]string, error) {
	// parse all names under tmp directory match "*-{task}"

	// Walk through the directory
	var names []string
	err := filepath.Walk(TEMPDIR, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Check if the file is not a directory, has ".json" extension, and ends with task
		if !info.IsDir() &&
			strings.HasSuffix(info.Name(), task+".json") &&
			!strings.HasPrefix(info.Name(), "temp") {
			names = append(names, TEMPDIR+info.Name())
		}
		return nil
	})
	if err != nil {
		fmt.Println("Error walking through directory:", err)
		return names, err
	}
	return names, nil
}

func parseKV(task string) ([]KeyValue, error) {
	var kva []KeyValue
	names, err := parse_name(task)
	if err != nil {
		return kva, err
	}
	for _, name := range names {
		file, err := os.Open(name)
		if err != nil {
			fmt.Printf("Error opening file %s: %v\n", name, err)
			continue
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva, nil
}

func hash_KV(kva []KeyValue, nReduce int) map[int][]KeyValue {
	mp := make(map[int][]KeyValue)
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		mp[idx] = append(mp[idx], kv)
	}
	return mp
}

func write_intermediate(kva []KeyValue, nReduce int, worker_id int) ([]string, error) {
	mp := hash_KV(kva, nReduce)
	var tempfiles []string
	for i, kva := range mp {
		// create temp-workerid-taskid
		filename := fmt.Sprintf("temp-map-%d-%d.json", worker_id, i)
		// delete all temp file later

		file, err := os.Create(TEMPDIR + filename)
		if err != nil {
			panic(err)
		}
		tempfiles = append(tempfiles, file.Name())
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			err = enc.Encode(&kv)
		}
		if err != nil {
			return tempfiles, err
		}
		err = file.Close()
		if err != nil {
			return tempfiles, err
		}
	}
	return tempfiles, nil
}

func read_file(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func MapHandler(task_name string, nReduce int, worker_id int,
	mapf func(string, string) []KeyValue) bool {
	content := read_file(task_name)
	intermediate := mapf(task_name, string(content))
	filenames, err := write_intermediate(intermediate, nReduce, worker_id)
	defer func() {
		// garbage collection
		for _, file := range filenames {
			os.Remove(file)
		}
	}()
	if err != nil {
		fmt.Printf("write intermediate failed\n")
		return false
	}
	ask := Ask{Worker_id: worker_id,
		Filenames: filenames,
		Is_map:    true,
		Task:      task_name}
	var reply Reply
	ok := call("Coordinator.Commit", &ask, &reply)
	if !ok {
		fmt.Printf("map commit failed!\n")
		return false
	}
	return true
}

func ReduceHandler(task string, worker_id int, reducef func(string, []string) string) bool {
	intermediate, err := parseKV(task)
	if err != nil {
		return false
	}
	sort.Sort(ByKey(intermediate))

	oname := TEMPDIR + "temp-mr-out-" + task
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	defer os.Remove(oname)
	ask := Ask{Worker_id: worker_id,
		Filenames: []string{oname},
		Is_map:    false,
		Task:      task}
	var reply Reply
	ok := call("Coordinator.Commit", &ask, &reply)
	if !ok {
		fmt.Printf("reduce commit failed!\n")
		return false
	}
	return true
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		<-time.After(CALLINTERVAL)
		// request a task
		var ask Ask
		reply := Reply{Please_exit: false}
		ok := call("Coordinator.Register", &ask, &reply)
		if !ok {
			fmt.Printf("worker failed, restart\n")
			continue
		}
		if reply.Please_exit {
			fmt.Printf("job done, exit")
			break
		}
		task_name := reply.Task
		worker_id := reply.Worker_id
		is_map := reply.Is_map
		nReduce := reply.NReduce

		res := false
		if is_map {
			res = MapHandler(task_name, nReduce, worker_id, mapf)
		} else {
			res = ReduceHandler(task_name, worker_id, reducef)
		}
		if !res {
			fmt.Printf("worker failed, restart\n")
		}
	}

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	//fmt.Println(err)
	return false
}
