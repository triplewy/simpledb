package gossip

import (
	"log"
	"os"
)

// Debug is debug logger
var Debug *log.Logger

// Out is out logger
var Out *log.Logger

// Error is error logger
var Error *log.Logger

// Initialize the loggers
func init() {
	Debug = log.New(os.Stdout, "DEBUG: ", log.Ltime|log.Lshortfile)
	Out = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	Error = log.New(os.Stdout, "ERROR: ", log.Ltime|log.Lshortfile)
}
