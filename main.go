package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	// Shortest Job First, "shortest burst" with preemption
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	// Shortest Job First, Highest Priority with preemption
	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	// Round Robin Schedule, 1 time unit on the cpu then rotate
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		completion      int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)

		timeUnit          int64 = 1
		currentTime       int64 = 0
		readyQueue        []Process
		tempQueue         []Process
		dummyProces       Process
		lastArriveProcess = findLastArrival(processes)
	)

	// Priming ready queue to run the while loop
	// Will delete the dummy process after entering the loop
	readyQueue = append(readyQueue, dummyProces)

	for len(readyQueue) != 0 {

		// Delete the dummy Process
		if currentTime == 0 {
			readyQueue = nil
		}

		//Add a process to ready queue if the arrival time is same as current time
		if currentTime <= lastArriveProcess {

			for i := range processes {

				if processes[i].ArrivalTime == currentTime {
					readyQueue = append(readyQueue, processes[i])
				}

			}
		}
		//Run the process on cpu
		if len(readyQueue) > 1 {
			var targetProcessLocation int = 0

			//Find highest priority process
			targetProcessLocation = findHighestPriority(readyQueue)
			// Add the process to the gantt schedule
			gantt = append(gantt, TimeSlice{
				PID:   readyQueue[targetProcessLocation].ProcessID,
				Start: currentTime,
				Stop:  currentTime + timeUnit,
			})

			readyQueue[targetProcessLocation].BurstDuration = readyQueue[targetProcessLocation].BurstDuration - 1

		} else {
			// Add the process to the gantt schedule
			gantt = append(gantt, TimeSlice{
				PID:   readyQueue[0].ProcessID,
				Start: currentTime,
				Stop:  currentTime + timeUnit,
			})

			readyQueue[0].BurstDuration = readyQueue[0].BurstDuration - 1
		}

		// If burst duration of a process is 0 in ready queue, then it is complete and needs to be deleted from ready queue
		for j := 0; j < len(readyQueue); j++ {

			// tempQueue will contain process that have burst duration greater than 0 or "not done"
			if readyQueue[j].BurstDuration > 0 {
				tempQueue = append(tempQueue, readyQueue[j])
			}
		}

		//Set readyQueue to tempQueue and erase tempQueue
		readyQueue = nil

		readyQueue = tempQueue
		tempQueue = nil

		// Increase current time by 1
		currentTime = currentTime + timeUnit

	}
	// Process Done. Time to report process schedule

	//Make Gantt Schedule Pretty ie Not have broken into each time unit
	//Setting the start time of next element in gantt to previous element if it has same PID
	for n := 0; n < len(gantt)-1; n++ {
		if gantt[n].PID == gantt[n+1].PID {
			gantt[n+1].Start = gantt[n].Start
		}
	}
	//Deleting any element in gantt that has same start time as next element. This will make the PID only have 1 entry in gantt from start until stop until next PID
	for n := 0; n < len(gantt)-1; n++ {
		if gantt[n].Start == gantt[n+1].Start {
			gantt = append(gantt[:n], gantt[n+1:]...)
			n-- // Has to be used to have correct counter in for loop. Source Used: https://dinolai.com/notes/golang/golang-delete-slice-item-in-range-problem.html
		}
	}

	// Calculate table
	for i := range processes {
		completion = findExitTime(processes[i].ProcessID, gantt)
		turnaround := completion - processes[i].ArrivalTime
		waitingTime = turnaround - processes[i].BurstDuration

		totalTurnaround += float64(turnaround)
		totalWait += float64(waitingTime)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
	}

	// Calculate last few things
	lastCompletion = float64(findHighestExitTime(gantt))
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	// Outputs for function
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		completion      int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)

		timeUnit          int64 = 1
		currentTime       int64 = 0
		readyQueue        []Process
		tempQueue         []Process
		dummyProces       Process
		lastArriveProcess = findLastArrival(processes)
	)

	// Priming ready queue to run the while loop
	// Will delete the dummy process after entering the loop
	readyQueue = append(readyQueue, dummyProces)

	for len(readyQueue) != 0 {

		// Delete the dummy Process
		if currentTime == 0 {
			readyQueue = nil
		}

		//Add a process to ready queue if the arrival time is same as current time
		if currentTime <= lastArriveProcess {

			for i := range processes {

				if processes[i].ArrivalTime == currentTime {
					readyQueue = append(readyQueue, processes[i])
				}

			}
		}
		//Run the process on cpu
		if len(readyQueue) > 1 {
			var targetProcessLocation int = 0

			//Find shorstest burstduration
			targetProcessLocation = findShortestBurst(readyQueue)
			// Add the process to the gantt schedule
			gantt = append(gantt, TimeSlice{
				PID:   readyQueue[targetProcessLocation].ProcessID,
				Start: currentTime,
				Stop:  currentTime + timeUnit,
			})

			readyQueue[targetProcessLocation].BurstDuration = readyQueue[targetProcessLocation].BurstDuration - 1

		} else {
			// Add the process to the gantt schedule
			gantt = append(gantt, TimeSlice{
				PID:   readyQueue[0].ProcessID,
				Start: currentTime,
				Stop:  currentTime + timeUnit,
			})

			readyQueue[0].BurstDuration = readyQueue[0].BurstDuration - 1
		}

		// If burst duration of a process is 0 in ready queue, then it is complete and needs to be deleted from ready queue
		for j := 0; j < len(readyQueue); j++ {

			// tempQueue will contain process that have burst duration greater than 0 or "not done"
			if readyQueue[j].BurstDuration > 0 {
				tempQueue = append(tempQueue, readyQueue[j])
			}
		}

		//Set readyQueue to tempQueue and erase tempQueue
		readyQueue = nil

		readyQueue = tempQueue
		tempQueue = nil

		// Increase current time by 1
		currentTime = currentTime + timeUnit

	}
	// Process Done. Time to report process schedule

	//Make Gantt Schedule Pretty ie Not have broken into each time unit
	//Setting the start time of next element in gantt to previous element if it has same PID
	for n := 0; n < len(gantt)-1; n++ {
		if gantt[n].PID == gantt[n+1].PID {
			gantt[n+1].Start = gantt[n].Start
		}
	}
	//Deleting any element in gantt that has same start time as next element. This will make the PID only have 1 entry in gantt from start until stop until next PID
	for n := 0; n < len(gantt)-1; n++ {
		if gantt[n].Start == gantt[n+1].Start {
			gantt = append(gantt[:n], gantt[n+1:]...)
			n-- // Has to be used to have correct counter in for loop. Source Used: https://dinolai.com/notes/golang/golang-delete-slice-item-in-range-problem.html
		}
	}

	// Calculate table
	for i := range processes {
		completion = findExitTime(processes[i].ProcessID, gantt)
		turnaround := completion - processes[i].ArrivalTime
		waitingTime = turnaround - processes[i].BurstDuration

		totalTurnaround += float64(turnaround)
		totalWait += float64(waitingTime)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
	}

	// Calculate last few things
	lastCompletion = float64(findHighestExitTime(gantt))
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	// Outputs for function
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		completion      int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)

		timeUnit          int64 = 1
		currentTime       int64 = 0
		readyQueue        []Process
		tempQueue         []Process
		dummyProces       Process
		lastArriveProcess = findLastArrival(processes)
	)

	// Priming ready queue to run the while loop
	// Will delete the dummy process after entering the loop
	readyQueue = append(readyQueue, dummyProces)

	for len(readyQueue) != 0 {
		// Delete the dummy Process
		if currentTime == 0 {
			readyQueue = nil
			tempQueue = nil
		}

		//Add a process to ready queue if the arrival time is same as current time
		if currentTime <= lastArriveProcess {

			for i := range processes {

				if processes[i].ArrivalTime == currentTime {

					// For Round Robin we want the newest Arrival to be at the front of the queue
					tempQueue = append(tempQueue, processes[i])

					for j := 0; j < len(readyQueue); j++ {
						tempQueue = append(tempQueue, readyQueue[j])
					}
					readyQueue = nil
					readyQueue = tempQueue
					tempQueue = nil
				}

			}
		}

		// Add the process to the gantt schedule
		gantt = append(gantt, TimeSlice{
			PID:   readyQueue[0].ProcessID,
			Start: currentTime,
			Stop:  currentTime + timeUnit,
		})

		// Remove one burst duration that was added to gantt chart
		readyQueue[0].BurstDuration = readyQueue[0].BurstDuration - 1

		// Rotate the readyQueue so that Round Robin occurs. Only occurs when len(readyQueue) > 1
		if len(readyQueue) > 1 {

			tempQueue = readyQueue[1:]
			tempQueue = append(tempQueue, readyQueue[0])
			readyQueue = tempQueue

		}
		tempQueue = nil

		// If burst duration of a process is 0 in ready queue, then it is complete and needs to be deleted from ready queue
		for j := 0; j < len(readyQueue); j++ {

			// tempQueue will contain process that have burst duration greater than 0 or "not done"
			if readyQueue[j].BurstDuration > 0 {
				tempQueue = append(tempQueue, readyQueue[j])
			}
		}

		//Set readyQueue to tempQueue and erase tempQueue
		readyQueue = nil

		readyQueue = tempQueue
		tempQueue = nil

		// Increase current time by 1
		currentTime = currentTime + timeUnit

		if currentTime > 40 {
			readyQueue = nil
		}

	}
	// Process Done. Time to report process schedule

	//Make Gantt Schedule Pretty ie Not have broken into each time unit
	//Setting the start time of next element in gantt to previous element if it has same PID
	for n := 0; n < len(gantt)-1; n++ {
		if gantt[n].PID == gantt[n+1].PID {
			gantt[n+1].Start = gantt[n].Start
		}
	}
	//Deleting any element in gantt that has same start time as next element. This will make the PID only have 1 entry in gantt from start until stop until next PID
	for n := 0; n < len(gantt)-1; n++ {
		if gantt[n].Start == gantt[n+1].Start {
			gantt = append(gantt[:n], gantt[n+1:]...)
			n-- // Has to be used to have correct counter in for loop. Source Used: https://dinolai.com/notes/golang/golang-delete-slice-item-in-range-problem.html
		}
	}

	// Calculate table
	for i := range processes {
		completion = findExitTime(processes[i].ProcessID, gantt)
		turnaround := completion - processes[i].ArrivalTime
		waitingTime = turnaround - processes[i].BurstDuration

		totalTurnaround += float64(turnaround)
		totalWait += float64(waitingTime)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
	}

	// Calculate last few things
	lastCompletion = float64(findHighestExitTime(gantt))
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	// Outputs for function
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion

//region Finding Process Info

// Finds the last Arrival Time of the process for the scheduler
func findLastArrival(myProcesses []Process) int64 {
	var lastArrival int64 = 0

	for i := range myProcesses {
		if myProcesses[i].ArrivalTime > lastArrival {
			lastArrival = myProcesses[i].ArrivalTime
		}
	}

	return lastArrival
}

// Finds the process with the shortest burst duration
func findShortestBurst(myProcesses []Process) int {
	var processLocation int = 0
	var shortestBurst int64 = 10000

	for i := range myProcesses {
		if myProcesses[i].BurstDuration < shortestBurst {
			shortestBurst = myProcesses[i].BurstDuration
			processLocation = i
		}
	}

	return processLocation
}

func findHighestPriority(myProcesses []Process) int {
	var processLocation int = 0
	var highestPriority int64 = 10000 //Arbitrarily large so that first process will be top priority

	for i := range myProcesses {
		if myProcesses[i].Priority < highestPriority {
			highestPriority = myProcesses[i].Priority
			processLocation = i
		}
	}

	return processLocation

}

// Finds the completion time for a process
func findExitTime(findExit int64, myGantt []TimeSlice) int64 {
	var exitTime int64 = 0
	for n := 0; n < len(myGantt); n++ {
		if myGantt[n].PID == findExit {
			if myGantt[n].Stop >= exitTime {
				exitTime = myGantt[n].Stop
			}
		}
	}
	return exitTime
}

// Finds the completion time for a process
func findHighestExitTime(myGantt []TimeSlice) int64 {
	var exitTime int64 = 0
	for n := 0; n < len(myGantt); n++ {
		if myGantt[n].Stop > exitTime {
			exitTime = myGantt[n].Stop
		}
	}
	return exitTime
}

//endregion
