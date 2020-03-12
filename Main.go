package main

import (
	"fmt"
	"sync"
)

// PUT function
func put(hashMap map[string]chan int, key string, value int, wg *sync.WaitGroup) {
	// defer executes after the execution of the entire function

	defer wg.Done()

	// initializes channel for the key if it doesnt exist
	// we assume length of only 1 for each key
	if _, mapValExist := hashMap[key]; !mapValExist {
		hashMap[key] = make(chan int, 1)
	}

	//sending to the channel
	hashMap[key] <- value
	fmt.Printf("PUT sent %d\n", value)
}

// GET function
func get(hashMap map[string]chan int, key string, wg *sync.WaitGroup) {
	defer wg.Done()

	// checking if the key exists in the map
	if _, mapValExist := hashMap[key]; !mapValExist {
		fmt.Printf("GET error: %s key is not found", key)
	} else {
		val := <-hashMap[key]
		fmt.Printf("GET received %d", val)
	}

}

// READ function
func read(hashMap map[string]chan int, key string, wg *sync.WaitGroup) {
	defer wg.Done()

	// checking if the key exists in the map
	if _, mapValExist := hashMap[key]; !mapValExist {
		fmt.Printf("READ error: %s key is not found", key)
	} else {
		// we are removing the key out of the channel and again writing it back
		val := <-hashMap[key]
		fmt.Printf("READ outputs %d\n", val)

		//write back
		wg.Add(1)
		go put(hashMap, key, val, wg)
	}
}

// LIST function
func list(hashMap map[string]chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	// checking if list is empty
	if len(hashMap) == 0 {
		fmt.Println("LIST outputs empty list")
	} else {
		fmt.Println("LIST outputs, key and value")
		for key := range hashMap {
			// we are removing the key out of the channel and again writing it back
			val := <-hashMap[key]
			fmt.Printf("%s, %d\n", key, val)

			//write back
			wg.Add(1)
			go put(hashMap, key, val, wg)
		}
	}
}

// DELETE function
func del(hashMap map[string]chan int, key string, wg *sync.WaitGroup) {
	defer wg.Done()
	if _, mapValExist := hashMap[key]; !mapValExist {
		fmt.Printf("DELETE error: No such key named %s available\n", key)
	} else {
		// this automatically deletes it
		fmt.Printf("DELETE deleted key %s, value %d\n", key, <-hashMap[key])
	}
}

func main() {

	var value, bufferLength int
	var key, commandName string

	wg := &sync.WaitGroup{}

	// creating a map
	fmt.Println("Input the length of the buffer")
	fmt.Scanf("%d", &bufferLength)
	hashMap := make(map[string]chan int, bufferLength)

	for {
		fmt.Println("Enter the command(put/get/read/list/delete/exit)")
		fmt.Scanf("%s", &commandName)

		// if else blocks based on the command received
		// they execute until exit is given
		if commandName == "put" {
			fmt.Println("Enter key(string) and value(int)")
			fmt.Scanf("%s%d", &key, &value)
			wg.Add(1)
			go put(hashMap, key, value, wg)
		} else if commandName == "get" {
			fmt.Println("Enter key(string)")
			fmt.Scanf("%s", &key)
			wg.Add(1)
			go get(hashMap, key, wg)
		} else if commandName == "read" {
			fmt.Println("Enter key(string)")
			fmt.Scanf("%s", &key)
			wg.Add(1)
			go read(hashMap, key, wg)
		} else if commandName == "list" {
			wg.Add(1)
			go list(hashMap, wg)
		} else if commandName == "delete" {
			fmt.Println("Enter key(string)")
			fmt.Scanf("%s", &key)
			wg.Add(1)
			go del(hashMap, key, wg)
		} else if commandName == "exit" {
			break
		} else {
			fmt.Println("Wrong command, Enter again")
		}
	}

	// wait till the goroutines run
	wg.Wait()
}
