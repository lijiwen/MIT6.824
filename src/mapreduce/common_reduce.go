package mapreduce

import (
	"io/ioutil"
	"log"
	"encoding/json"
	"sort"
	"os"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
	resPairs := []KeyValue{}
	for i := 0; i < nMap; i++{
		curFileName := reduceName(jobName, i, reduceTaskNumber)
		b, err := ioutil.ReadFile(curFileName)
		if err != nil{
			log.Fatalf("common_reduce.go doReduce read file %s error\n", curFileName)
		}

		var kvPairs []KeyValue
		err = json.Unmarshal(b, &kvPairs)
		if err != nil{
			log.Fatalf("common_reduce.go doReduce %d json unmarshal err\n", i)
		}
		resPairs = append(resPairs, kvPairs...)
	}
	sort.Sort(KeyValueSilce(resPairs))

	file, err := os.OpenFile(outFile, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
	if err != nil{
		log.Fatal("common_reduce doReduce ", err)
	}
	defer file.Close()

	var key string
	var values []string
	for i:= 0; i < len(resPairs); i++{
		pair := resPairs[i]
		if pair.Key == key{
			values = append(values, pair.Value)
		}else{
			if i != 0{
				ret := reduceF(key, values)
				_, err := file.Write(append([]byte(ret), []byte("\n")...))
				if err != nil{
					log.Fatal(err)
				}
			}

			values = make([]string, 0)
			key = pair.Key
			values = append(values, pair.Value)
		}
	}
	if key != ""{
		ret := reduceF(key, values)
		_, err := file.Write(append([]byte(ret), []byte("\n")...))
		if err != nil{
			log.Fatal(err)
		}
	}
}

type KeyValueSilce []KeyValue

func (s KeyValueSilce) Len() int{
	return len(s)
}

func (s KeyValueSilce) Less(i, j int) bool{
	return s[i].Key < s[j].Key
}

func (s KeyValueSilce) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
