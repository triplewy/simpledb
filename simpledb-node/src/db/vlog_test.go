package db

// func TestVlogAsync(t *testing.T) {
// 	vlog, err := NewVLog("vlog.log")
// 	if err != nil {
// 		t.Fatalf("Error creating vlog: %v\n", err)
// 	}

// 	go vlog.run()

// 	values := []string{"test", "again", "big", "suck", "my", "long", "thick", "hairy"}
// 	var wg sync.WaitGroup

// 	for _, item := range values {
// 		go func(item string) {
// 			offset, numBytes, err := vlog.Append([]byte(item))
// 			if err != nil {
// 				t.Fatalf("Error appending to vlog: %v\n", err)
// 			}
// 			result, err := vlog.Read(offset, numBytes)
// 			if err != nil {
// 				t.Fatalf("Error reading from vlog: %v\n", err)
// 			}

// 			if item != string(result) {
// 				t.Fatalf("vlog read incorrect. Expected: %s, Got: %s\n", item, result)
// 			}
// 			fmt.Println(string(result))
// 		}(item)
// 	}

// 	wg.Wait()
// 	vlog.closeChan <- struct{}{}
// }
