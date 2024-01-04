package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	bolt "go.etcd.io/bbolt"
)

var (
	// ErrBatchNonDivisibleBatchSize is returned when the batch size can't be evenly
	// divided by the iteration count.
	ErrBatchNonDivisibleBatchSize = errors.New("the number of iterations must be divisible by the batch size")
)

func newBenchCobraCommand() *cobra.Command {
	o := newBenchOptions()
	benchCmd := &cobra.Command{
		Use:   "bench",
		Short: "run synthetic benchmark against bbolt",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return benchFunc(o)
		},
	}

	o.AddFlags(benchCmd.Flags())
	return benchCmd
}

type benchOptions struct {
	batchSize    uint32
	blockProfile string
	iterations   uint32
	cpuProfile   string
	fillPercent  float64
	keySize      int
	memProfile   string
	noSync       bool
	path         string
	profileMode  string
	readMode     string
	valueSize    int
	work         bool
	writeMode    string
}

// Returns a new benchOptions for the `bench` command with the default values applied.
func newBenchOptions() benchOptions {
	return benchOptions{
		iterations:  1000,
		fillPercent: bolt.DefaultFillPercent,
		keySize:     8,
		profileMode: "rw",
		readMode:    "seq",
		valueSize:   32,
		writeMode:   "seq",
	}
}

// AddFlags sets the flags for the `bench` command.
func (o *benchOptions) AddFlags(fs *pflag.FlagSet) {
	fs.Uint32Var(&o.batchSize, "batch-size", o.batchSize, "the step size for each iteration, if not provided iteration size is used, it needs to be evenly divided by the iteration count (count)")
	fs.StringVar(&o.blockProfile, "blockprofile", o.blockProfile, "output file for the pprof block profile")
	fs.Uint32Var(&o.iterations, "count", o.iterations, "the number of iterations")
	fs.StringVar(&o.cpuProfile, "cpuprofile", o.cpuProfile, "output file for the pprof CPU profile")
	fs.Float64Var(&o.fillPercent, "fill-percent", o.fillPercent, "the percentage that split pages are filled")
	fs.IntVar(&o.keySize, "key-size", o.keySize, "the size for the key, from the key value insertion")
	fs.StringVar(&o.memProfile, "memprofile", o.memProfile, "output file for the pprof memoery profile")
	fs.BoolVar(&o.noSync, "no-sync", o.noSync, "skip fsync() calls after each commit")
	fs.StringVar(&o.path, "path", o.path, "path to the database file")
	fs.StringVar(&o.profileMode, "profile-mode", o.profileMode, "the profile mode to execute, valid modes are r, w and rw")
	// TODO: Remove read mode
	fs.StringVar(&o.readMode, "read-mode", o.readMode, "the only valid read mode is seq!!!")
	fs.IntVar(&o.valueSize, "value-size", o.valueSize, "the size for the value, from the key value insertion")
	fs.BoolVar(&o.work, "work", o.work, "if set, the database path won't be removed after the execution")
	fs.StringVar(&o.writeMode, "write-mode", o.writeMode, "the write mode, valid values are seq, rnd, seq-nest and rnd-nest")
}

func (o *benchOptions) Validate() error {
	// Require that batch size can be evenly divided by the iteration count, if set.
	if o.batchSize > 0 && o.iterations%o.batchSize != 0 {
		return ErrBatchNonDivisibleBatchSize
	}

	return nil
}

func benchFunc(cfg benchOptions) error {
	// Generate temp path if one is not passed in.
	path := cfg.path
	if path == "" {
		f, err := os.CreateTemp("", "bolt-bench-")
		if err != nil {
			return fmt.Errorf("temp file: %s", err)
		}
		f.Close()
		os.Remove(f.Name())
		path = f.Name()
	}

	// Remove path if "-work" is not set. Otherwise keep path.
	if cfg.work {
		fmt.Fprintf(os.Stderr, "work: %s\n", path)
	} else {
		defer os.Remove(path)
	}

	// Set batch size to iteration size if not set.
	if cfg.batchSize == 0 {
		cfg.batchSize = cfg.iterations
	}

	// Create database.
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	}
	db.NoSync = cfg.noSync
	defer db.Close()

	// Write to the database.
	var writeResults benchResults
	fmt.Fprintf(os.Stderr, "starting write benchmark.\n")
	if err := benchWrites(db, cfg, &writeResults); err != nil {
		return fmt.Errorf("write: %v", err)
	}

	var readResults benchResults
	fmt.Fprintf(os.Stderr, "starting read benchmark.\n")
	// Read from the database.
	if err := benchReads(db, cfg, &readResults); err != nil {
		return fmt.Errorf("bench: read: %s", err)
	}

	// Print results.
	fmt.Fprintf(os.Stderr, "# Write\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", writeResults.CompletedOps(), writeResults.Duration(), writeResults.OpDuration(), writeResults.OpsPerSecond())
	fmt.Fprintf(os.Stderr, "# Read\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", readResults.CompletedOps(), readResults.Duration(), readResults.OpDuration(), readResults.OpsPerSecond())
	fmt.Fprintln(os.Stderr, "")
	return nil
}

// benchResults represents the performance results of the benchmark and is thread-safe.
type benchResults struct {
	completedOps int64
	duration     int64
}

func (r *benchResults) AddCompletedOps(amount int64) {
	atomic.AddInt64(&r.completedOps, amount)
}

func (r *benchResults) CompletedOps() int64 {
	return atomic.LoadInt64(&r.completedOps)
}

func (r *benchResults) SetDuration(dur time.Duration) {
	atomic.StoreInt64(&r.duration, int64(dur))
}

func (r *benchResults) Duration() time.Duration {
	return time.Duration(atomic.LoadInt64(&r.duration))
}

// Returns the duration for a single read/write operation.
func (r *benchResults) OpDuration() time.Duration {
	if r.CompletedOps() == 0 {
		return 0
	}
	return r.Duration() / time.Duration(r.CompletedOps())
}

// Returns average number of read/write operations that can be performed per second.
func (r *benchResults) OpsPerSecond() int {
	var op = r.OpDuration()
	if op == 0 {
		return 0
	}
	return int(time.Second) / int(op)
}

func benchWrites(db *bolt.DB, cfg benchOptions, results *benchResults) error {
	// Start profiling for writes.
	if cfg.profileMode == "rw" || cfg.profileMode == "w" {
		if err := benchStartProfiling(cfg); err != nil {
			return err
		}
	}

	finishChan := make(chan interface{})
	go benchCheckProgress(results, finishChan, os.Stderr)
	defer close(finishChan)

	t := time.Now()

	var err error
	switch cfg.writeMode {
	case "seq":
		err = benchWritesSequential(db, cfg, results)
	case "rnd":
		err = benchWritesRandom(db, cfg, results)
	case "seq-nest":
		err = benchWritesSequentialNested(db, cfg, results)
	case "rnd-nest":
		err = benchWritesRandomNested(db, cfg, results)
	default:
		return fmt.Errorf("invalid write mode: %s", cfg.writeMode)
	}

	// Save time to write.
	results.SetDuration(time.Since(t))

	// Stop profiling for writes only.
	if cfg.profileMode == "w" {
		if err := benchStopProfiling(); err != nil {
			return err
		}
	}

	return err
}

func benchWritesSequential(db *bolt.DB, cfg benchOptions, results *benchResults) error {
	var i = uint32(0)
	return benchWritesWithSource(db, cfg, results, func() uint32 { i++; return i })
}

func benchWritesRandom(db *bolt.DB, cfg benchOptions, results *benchResults) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return benchWritesWithSource(db, cfg, results, func() uint32 { return r.Uint32() })
}

func benchWritesSequentialNested(db *bolt.DB, cfg benchOptions, results *benchResults) error {
	var i = uint32(0)
	return benchWritesNestedWithSource(db, cfg, results, func() uint32 { i++; return i })
}

func benchWritesRandomNested(db *bolt.DB, cfg benchOptions, results *benchResults) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return benchWritesNestedWithSource(db, cfg, results, func() uint32 { return r.Uint32() })
}

func benchWritesWithSource(db *bolt.DB, cfg benchOptions, results *benchResults, keySource func() uint32) error {
	for i := uint32(0); i < cfg.iterations; i += cfg.batchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = cfg.fillPercent

			fmt.Fprintf(os.Stderr, "Starting write iteration %d/%d+%d\n", i, cfg.iterations, cfg.batchSize)
			for j := uint32(0); j < cfg.batchSize; j++ {
				key := make([]byte, cfg.keySize)
				value := make([]byte, cfg.valueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}

				results.AddCompletedOps(1)
			}
			fmt.Fprintf(os.Stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func benchWritesNestedWithSource(db *bolt.DB, cfg benchOptions, results *benchResults, keySource func() uint32) error {
	for i := uint32(0); i < cfg.iterations; i += cfg.batchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			top, err := tx.CreateBucketIfNotExists(benchBucketName)
			if err != nil {
				return err
			}
			top.FillPercent = cfg.fillPercent

			// Create bucket key.
			name := make([]byte, cfg.keySize)
			binary.BigEndian.PutUint32(name, keySource())

			// Create bucket.
			b, err := top.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
			b.FillPercent = cfg.fillPercent

			fmt.Fprintf(os.Stderr, "Starting write iteration %d\n", i)
			for j := uint32(0); j < cfg.batchSize; j++ {
				var key = make([]byte, cfg.keySize)
				var value = make([]byte, cfg.valueSize)

				// Generate key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert value into subbucket.
				if err := b.Put(key, value); err != nil {
					return err
				}

				results.AddCompletedOps(1)
			}
			fmt.Fprintf(os.Stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Reads from the database.
func benchReads(db *bolt.DB, cfg benchOptions, results *benchResults) error {
	// Start profiling for reads.
	if cfg.profileMode == "r" {
		if err := benchStartProfiling(cfg); err != nil {
			return err
		}
	}

	finishChan := make(chan interface{})
	go benchCheckProgress(results, finishChan, os.Stderr)
	defer close(finishChan)

	t := time.Now()

	var err error
	switch cfg.readMode {
	case "seq":
		switch cfg.writeMode {
		case "seq-nest", "rnd-nest":
			err = benchReadsSequentialNested(db, cfg, results)
		default:
			err = benchReadsSequential(db, cfg, results)
		}
	default:
		return fmt.Errorf("invalid read mode: %s", cfg.readMode)
	}

	// Save read time.
	results.SetDuration(time.Since(t))

	// Stop profiling for reads.
	if cfg.profileMode == "rw" || cfg.profileMode == "r" {
		if err := benchStopProfiling(); err != nil {
			return err
		}
	}

	return err
}

func benchReadsSequential(db *bolt.DB, cfg benchOptions, results *benchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := uint32(0)
			c := tx.Bucket(benchBucketName).Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				numReads++
				results.AddCompletedOps(1)
				if v == nil {
					return errors.New("invalid value")
				}
			}

			if cfg.writeMode == "seq" && numReads != cfg.iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", cfg.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func benchReadsSequentialNested(db *bolt.DB, cfg benchOptions, results *benchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := uint32(0)
			var top = tx.Bucket(benchBucketName)
			if err := top.ForEach(func(name, _ []byte) error {
				if b := top.Bucket(name); b != nil {
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						numReads++
						results.AddCompletedOps(1)
						if v == nil {
							return ErrInvalidValue
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}

			if cfg.writeMode == "seq-nest" && numReads != cfg.iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", cfg.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func benchCheckProgress(results *benchResults, finishChan chan interface{}, stderr io.Writer) {
	ticker := time.Tick(time.Second)
	lastCompleted, lastTime := int64(0), time.Now()
	for {
		select {
		case <-finishChan:
			return
		case t := <-ticker:
			completed, taken := results.CompletedOps(), t.Sub(lastTime)
			fmt.Fprintf(stderr, "Completed %d requests, %d/s \n",
				completed, ((completed-lastCompleted)*int64(time.Second))/int64(taken),
			)
			lastCompleted, lastTime = completed, t
		}
	}
}

// Starts all profiles set on the options.
func benchStartProfiling(cfg benchOptions) error {
	var err error

	// Start CPU profiling.
	if cfg.cpuProfile != "" {
		cpuprofile, err = os.Create(cfg.cpuProfile)
		if err != nil {
			return fmt.Errorf("could not create cpu profile %q: %v\n", cfg.cpuProfile, err)
		}
		err = pprof.StartCPUProfile(cpuprofile)
		if err != nil {
			return fmt.Errorf("could not start cpu profile %q: %v\n", cfg.cpuProfile, err)
		}
	}

	// Start memory profiling.
	if cfg.memProfile != "" {
		memprofile, err = os.Create(cfg.memProfile)
		if err != nil {
			return fmt.Errorf("could not create memory profile %q: %v\n", cfg.memProfile, err)
		}
		runtime.MemProfileRate = 4096
	}

	// Start fatal profiling.
	if cfg.blockProfile != "" {
		blockprofile, err = os.Create(cfg.blockProfile)
		if err != nil {
			return fmt.Errorf("could not create block profile %q: %v\n", cfg.blockProfile, err)
		}
		runtime.SetBlockProfileRate(1)
	}

	return nil
}

// Stops all profiles.
func benchStopProfiling() error {
	if cpuprofile != nil {
		pprof.StopCPUProfile()
		cpuprofile.Close()
		cpuprofile = nil
	}

	if memprofile != nil {
		err := pprof.Lookup("heap").WriteTo(memprofile, 0)
		// TODO: Collect errors and return after closing
		if err != nil {
			return fmt.Errorf("bench: could not write mem profile")
		}
		memprofile.Close()
		memprofile = nil
	}

	if blockprofile != nil {
		err := pprof.Lookup("block").WriteTo(blockprofile, 0)
		if err != nil {
			return fmt.Errorf("bench: could not write block profile")
		}
		blockprofile.Close()
		blockprofile = nil
		runtime.SetBlockProfileRate(0)
	}
	return nil
}
