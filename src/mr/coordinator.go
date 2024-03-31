package mr

import "log"
import "net"
import "os"
import "errors"
import "path/filepath"
import "time"
import "sync"
import "strings"
import "fmt"
import "strconv"

import "net/rpc"
import "net/http"

const OCCUP = 1
const READY = 0
const DONE = -1

const TIMEOUT = time.Second * 10

const TEMPDIR = "./"

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex
	map_task         map[string]int
	reduce_task      map[string]int
	worker_id        int
	finished_workers map[int]bool
	nReduce          int
	map_finished     bool
	reduce_finished  bool
}

func fileRename(filenames []string) error {
	// wrap all temp files of the worker and rename
	dir := TEMPDIR
	for _, file := range filenames {
		// Extract the base filename without directory
		base := filepath.Base(file)
		// Remove the "temp-" prefix from the filename
		newName := strings.TrimPrefix(base, "temp-")

		// Create the new path including the directory
		newPath := filepath.Join(dir, newName)
		// Rename the file
		err := os.Rename(file, newPath)
		if err != nil {
			//fmt.Printf("Error renaming file %s to %s: %v\n", file, newPath, err)
			continue
		}
		//fmt.Printf("File %s renamed to %s\n", file, newPath)
	}
	return nil
}

func RemoveAllTempfile() {
	tmpDir := TEMPDIR
	prefix := "temp"

	// Get a list of files in the tmp directory
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	// Iterate over each file in the directory
	for _, file := range files {
		if file.IsDir() {
			continue // Skip directories
		}
		filename := file.Name()
		// Check if the file has the specified prefix
		if strings.HasPrefix(filename, prefix) || strings.HasPrefix(filename, "map") {
			// Remove the file
			os.Remove(filepath.Join(tmpDir, filename))
		}
	}

}

func (c *Coordinator) TimeoutHandler(id int, task string, is_map bool) {
	<-time.After(TIMEOUT)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.finished_workers[id]; !ok {
		c.finished_workers[id] = true
		if is_map {
			c.map_task[task] = READY
		} else {
			c.reduce_task[task] = READY
		}
	}
}

func (c *Coordinator) Assign(task string, is_map bool, reply *Reply) {
	if is_map {
		c.map_task[task] = OCCUP
	} else {
		c.reduce_task[task] = OCCUP
	}
	go c.TimeoutHandler(c.worker_id, task, is_map)
	reply.Task = task
	reply.Worker_id = c.worker_id
	reply.Is_map = is_map
	reply.NReduce = c.nReduce
	c.worker_id++
}

func (c *Coordinator) Register(ask *Ask, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduce_finished {
		reply.Please_exit = true
		return nil
	}
	finished := true
	request := false
	if !c.map_finished {
		for task, stat := range c.map_task {
			if stat == DONE {
				continue
			}
			finished = false
			if stat == READY {
				c.Assign(task, true, reply)
				request = true
				break
			}
		}
		if finished {
			c.map_finished = true
			fmt.Printf("Map Phase over\n")
		}
	} else {
		for task, stat := range c.reduce_task {
			if stat == DONE {
				continue
			}
			finished = false
			if stat == READY {
				c.Assign(task, false, reply)
				request = true
				break
			}
		}
		if finished {
			c.reduce_finished = true
			fmt.Printf("Reduce Phase over\n")
		}
	}
	if !request {
		return errors.New("request fail")
	}
	return nil
}

func (c *Coordinator) Commit(ask *Ask, reply *Reply) error {
	// rename temp file
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.finished_workers[ask.Worker_id]
	if ok {
		fmt.Printf("worker id is outdated!\n")
		return nil
	}
	err := fileRename(ask.Filenames)
	if err != nil {
		return err
	}
	// file already commit, release resource
	c.finished_workers[ask.Worker_id] = true
	if ask.Is_map {
		c.map_task[ask.Task] = DONE
	} else {
		c.reduce_task[ask.Task] = DONE
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// rpcs := rpc.NewServer()
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if c.reduce_finished {
		fmt.Printf("Job Done, remove all tempfiles and intermediate\n")
		RemoveAllTempfile()
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		map_task:         make(map[string]int),
		reduce_task:      make(map[string]int),
		finished_workers: make(map[int]bool),
		nReduce:          nReduce,
		worker_id:        0,
		map_finished:     false,
		reduce_finished:  false,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, file := range files {
		c.map_task[file] = READY
	}
	for i := 0; i < nReduce; i++ {
		c.reduce_task[strconv.Itoa(i)] = READY
	}

	c.server()
	return c
}
