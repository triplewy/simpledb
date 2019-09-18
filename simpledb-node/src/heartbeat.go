package simpledb

func (node *Node) run() {
	for {
		select {
		case remote := <-node.transferChan:
			_, err := node.TransferKVRPC(remote)
			if err != nil {
				Error.Printf("TransferRPC error: %v\n", err)
			}
		}
	}
}
