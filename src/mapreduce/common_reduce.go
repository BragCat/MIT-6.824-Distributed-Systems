package mapreduce

import (
	"encoding/json"
	"io"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var kvs []KeyValue
	for i := 0; i < nMap; i++ {
		imFile := reduceName(jobName, i, reduceTask)
		imFD, err := os.Open(imFile)
		if err != nil {
			debug("Reduce Task %v open intermediate file %v failed.", i, imFile)
		} else {
			decoder := json.NewDecoder(imFD)
			for {
				var kv KeyValue
				err := decoder.Decode(&kv)
				if err == io.EOF {
					break
				}
				if err != nil {
					debug("Reduce Task %v decode kv from intermediate file %v failed with error %v.",
						i, imFile, err)
				} else {
					kvs = append(kvs, kv)
				}
			}
			imFD.Close()
		}
	}
	sort.Sort(ByKey(kvs))

	outFD, err := os.Create(outFile)
	if err != nil {
		debug("Reduce Task %v create output file %v failed.", reduceTask, outFile)
		return
	}
	defer outFD.Close()
	if len(kvs) == 0 {
		return
	}
	encoder := json.NewEncoder(outFD)
	key := kvs[0].Key
	var values []string
	values = append(values, kvs[0].Value)
	for i := 1; i < len(kvs); i++ {
		if key != kvs[i].Key {
			encoder.Encode(KeyValue{key, reduceF(key, values)})
			key = kvs[i].Key
			values = values[:0]
		}
		values = append(values, kvs[i].Value)
	}
	encoder.Encode(KeyValue{key, reduceF(key, values)})
}

// slice of KeyValue type to sort
type ByKey []KeyValue

func (kvs ByKey) Len() int {
	return len(kvs)
}

func (kvs ByKey) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

func (kvs ByKey) Less(i, j int) bool {
	return kvs[i].Key < kvs[j].Key
}


