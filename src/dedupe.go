package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func main() {
	// Parse command-line flags
	partitions := flag.String("partitions", "", "Comma-separated list of partitions/directories to scan")
	dryRun := flag.Bool("dry-run", false, "Show what would be deleted without actually deleting")
	confirm := flag.Bool("confirm", false, "Require confirmation before deletion")
	verbose := flag.Bool("verbose", false, "Show detailed progress")
	flag.Parse()

	if *partitions == "" {
		log.Fatal("Please specify partitions/directories to scan using --partitions")
	}

	// Collect all files with their sizes
	sizeMap := make(map[int64][]string)
	startTime := time.Now()

	// Walk through each partition
	for _, partition := range filepath.SplitList(*partitions) {
		if *verbose {
			log.Printf("Scanning partition: %s", partition)
		}

		err := filepath.Walk(partition, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Printf("Error accessing %s: %v", path, err)
				return nil
			}
			if info.IsDir() {
				return nil
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return nil
			}
			size := info.Size()
			if size == 0 {
				return nil
			}
			sizeMap[size] = append(sizeMap[size], path)
			return nil
		})
		if err != nil {
			log.Printf("Error walking partition %s: %v", partition, err)
		}
	}

	if *verbose {
		log.Printf("Scanned %d files across partitions in %v", len(sizeMap), time.Since(startTime))
	}

	// Find potential duplicates by size
	potentialDuplicates := 0
	for _, files := range sizeMap {
		if len(files) > 1 {
			potentialDuplicates += len(files) - 1
		}
	}

	if *verbose {
		log.Printf("Found %d potential duplicates by size", potentialDuplicates)
	}

	// Check actual file contents using hash (SHA256)
	hashMap := make(map[string][]string)
	totalFiles := 0

	for size, files := range sizeMap {
		if len(files) < 2 {
			continue
		}

		if *verbose {
			log.Printf("Processing %d files of size %d bytes", len(files), size)
		}

		for _, path := range files {
			hash, err := fileHash(path)
			if err != nil {
				log.Printf("Error hashing %s: %v", path, err)
				continue
			}

			hashMap[hash] = append(hashMap[hash], path)
			totalFiles++
		}
	}

	// Process duplicates
	duplicateCount := 0
	totalSaved := int64(0)

	for hash, files := range hashMap {
		if len(files) < 2 {
			continue
		}

		// Sort files by partition and modification time (oldest first)
		sort.Slice(files, func(i, j int) bool {
			infoI, _ := os.Stat(files[i])
			infoJ, _ := os.Stat(files[j])
			partitionI := filepath.VolumeName(files[i])
			partitionJ := filepath.VolumeName(files[j])

			// Prioritize files on the same partition
			if partitionI != partitionJ {
				return partitionI < partitionJ
			}
			return infoI.ModTime().Before(infoJ.ModTime())
		})

		// Keep the first file, delete others
		keeper := files[0]
		duplicates := files[1:]

		fmt.Printf("\nDuplicate group (%s):\n", hash[:8])
		fmt.Printf("  Keeper: %s\n", keeper)
		for _, dup := range duplicates {
			duplicateCount++
			info, _ := os.Stat(dup)
			totalSaved += info.Size()

			fmt.Printf("  Duplicate: %s\n", dup)

			if !*dryRun {
				if *confirm {
					fmt.Printf("Delete %s? (y/N) ", dup)
					var response string
					fmt.Scanln(&response)
					if response != "y" && response != "Y" {
						continue
					}
				}

				err := os.Remove(dup)
				if err != nil {
					log.Printf("Error deleting %s: %v", dup, err)
				} else {
					fmt.Printf("Deleted %s\n", dup)
				}
			}
		}
	}

	fmt.Printf("\nSummary:\n")
	fmt.Printf("\tTotal files processed: %d\n", totalFiles)
	fmt.Printf("\tTotal duplicates found: %d\n", duplicateCount)
	fmt.Printf("\tPotential space saved: %d MB\n", totalSaved/(1024*1024))
	if *dryRun {
		fmt.Println("\n\t**RUN IN DRY-RUN MODE - NO FILES WERE DELETED**")
	}
}

// Calculate file's hash (SHA256)
func fileHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
