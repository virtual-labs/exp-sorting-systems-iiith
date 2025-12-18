/*
 * DISTRIBUTED SORTING ALGORITHM - STANDALONE C++ IMPLEMENTATION
 * =============================================================
 * 
 * This file implements the same distributed sorting algorithm used in the web simulation.
 * It's a complete, self-contained program that requires no external dependencies.
 * 
 * COMPILATION (choose one):
 *   Windows: cl standalone_distributed_sort.cpp
 *   GCC:     g++ standalone_distributed_sort.cpp -o sort
 *   Clang:   clang++ standalone_distributed_sort.cpp -o sort
 *   Online:  Copy-paste into https://onlinegdb.com or https://replit.com
 * 
 * ALGORITHM: Two-Phase MapReduce Distributed Sort
 *   Phase 1: Pivot Selection (mappers select local pivots, create global pivots)
 *   Phase 2: Bucket Distribution & Sort (partition data, sort buckets, merge)
 * 
 * This matches the logic in the JavaScript simulation exactly.
 */

#include <iostream>
#include <vector>
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <map>
#include <ctime>
#include <climits>

class DistributedSortSimulator {
private:
    // Configuration
    int numMappers;
    int numReducers;
    
    // Data structures matching the web simulation
    std::vector<int> inputData;
    std::vector<std::vector<int>> mapperData;
    std::vector<int> localPivots;
    std::vector<int> globalPivots;
    std::map<int, std::vector<std::vector<int>>> mapperBuckets;
    std::vector<std::vector<int>> reducerBuckets;
    std::vector<int> finalOutput;
    
    // Statistics
    int communicationRounds;
    int dataMovements;
    
public:
    DistributedSortSimulator(int mappers = 3, int reducers = 3) 
        : numMappers(mappers), numReducers(reducers), communicationRounds(0), dataMovements(0) {
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
    }
    
    // ========== PUBLIC INTERFACE ==========
    
    void runInteractiveDemo() {
        printHeader();
        
        while (true) {
            printMenu();
            int choice = getChoice();
            
            switch (choice) {
                case 1:
                    runWithDemoData();
                    break;
                case 2:
                    runWithRandomData();
                    break;
                case 3:
                    runWithCustomData();
                    break;
                case 4:
                    configureSystem();
                    break;
                case 5:
                    runPerformanceTest();
                    break;
                case 6:
                    std::cout << "\nThank you for using the Distributed Sort Simulator!\n";
                    return;
                default:
                    std::cout << "\nInvalid choice. Please try again.\n";
            }
            
            std::cout << "\nPress Enter to continue...";
            std::cin.ignore();
            std::cin.get();
        }
    }
    
    // ========== CORE ALGORITHM ==========
    
    std::vector<int> distributedSort(const std::vector<int>& data) {
        reset();
        inputData = data;
        
        std::cout << "\n=== DISTRIBUTED SORT EXECUTION ===\n";
        std::cout << "Input: ";
        printVector(inputData, 15);
        std::cout << "Mappers: " << numMappers << ", Reducers: " << numReducers << "\n\n";
        
        // Phase 1: Pivot Selection (matches JavaScript selectLocalPivots + selectGlobalPivots)
        phase1_PivotSelection();
        
        // Phase 2: Bucket Distribution and Sorting (matches JavaScript distributeToBuckets + performFinalSort)
        phase2_BucketSortAndMerge();
        
        printResults();
        return finalOutput;
    }
    
private:
    // ========== PHASE 1: PIVOT SELECTION ==========
    
    void phase1_PivotSelection() {
        std::cout << "--- PHASE 1: PIVOT SELECTION ---\n";
        
        // Step 1: Distribute data among mappers (round-robin like the web simulation)
        distributeDataToMappers();
        
        // Step 2: Each mapper selects a local pivot (random selection like JavaScript)
        selectLocalPivots();
        
        // Step 3: Create global pivots from local pivots (sort local pivots)
        createGlobalPivots();
        
        communicationRounds++;
        std::cout << "Communication round completed. Total rounds: " << communicationRounds << "\n\n";
    }
    
    void distributeDataToMappers() {
        mapperData.clear();
        mapperData.resize(numMappers);
        
        // Round-robin distribution (matches web simulation logic)
        for (int i = 0; i < inputData.size(); i++) {
            mapperData[i % numMappers].push_back(inputData[i]);
        }
        
        std::cout << "Data distribution to mappers:\n";
        for (int i = 0; i < numMappers; i++) {
            std::cout << "  Mapper " << i << " (" << mapperData[i].size() << " elements): ";
            printVector(mapperData[i], 8);
        }
    }
    
    void selectLocalPivots() {
        localPivots.clear();
        
        std::cout << "Local pivot selection:\n";
        for (int i = 0; i < numMappers; i++) {
            if (!mapperData[i].empty()) {
                // Random pivot selection (matches JavaScript random selection)
                int pivotIndex = std::rand() % mapperData[i].size();
                int pivot = mapperData[i][pivotIndex];
                localPivots.push_back(pivot);
                std::cout << "  Mapper " << i << " selected pivot: " << pivot << "\n";
            }
        }
    }
    
    void createGlobalPivots() {
        // Sort local pivots to create global pivots (matches JavaScript logic)
        globalPivots = localPivots;
        std::sort(globalPivots.begin(), globalPivots.end());
        
        std::cout << "Global pivots (sorted): ";
        printVector(globalPivots);
    }
    
    // ========== PHASE 2: BUCKET DISTRIBUTION & SORTING ==========
    
    void phase2_BucketSortAndMerge() {
        std::cout << "--- PHASE 2: BUCKET DISTRIBUTION & SORTING ---\n";
        
        // Step 1: Distribute data to buckets based on global pivots
        distributeToBuckets();
        
        // Step 2: Each reducer sorts its bucket
        performBucketSorting();
        
        // Step 3: Generate final output by concatenating sorted buckets
        generateFinalOutput();
        
        communicationRounds++;
        std::cout << "Final communication round completed. Total rounds: " << communicationRounds << "\n";
    }
    
    void distributeToBuckets() {
        mapperBuckets.clear();
        int numBuckets = globalPivots.size() + 1;
        
        std::cout << "Distributing data to " << numBuckets << " buckets:\n";
        
        // Each mapper distributes its data to appropriate buckets
        for (int mapperIdx = 0; mapperIdx < numMappers; mapperIdx++) {
            mapperBuckets[mapperIdx].resize(numBuckets);
            
            for (int value : mapperData[mapperIdx]) {
                int bucketIdx = findBucketForValue(value);
                mapperBuckets[mapperIdx][bucketIdx].push_back(value);
                dataMovements++;
            }
            
            // Show mapper's bucket distribution
            std::cout << "  Mapper " << mapperIdx << " buckets:\n";
            for (int b = 0; b < numBuckets; b++) {
                if (!mapperBuckets[mapperIdx][b].empty()) {
                    std::cout << "    Bucket " << b << ": ";
                    printVector(mapperBuckets[mapperIdx][b], 6);
                }
            }
        }
    }
    
    int findBucketForValue(int value) {
        // Find appropriate bucket using binary search on global pivots
        // This matches the JavaScript logic: while (value >= globalPivots[bucket]) bucket++
        int bucket = 0;
        for (int pivot : globalPivots) {
            if (value < pivot) break;
            bucket++;
        }
        return bucket;
    }
    
    void performBucketSorting() {
        reducerBuckets.clear();
        int numBuckets = globalPivots.size() + 1;
        reducerBuckets.resize(numBuckets);
        
        std::cout << "Reducer processing (collecting and sorting buckets):\n";
        
        // Each bucket becomes a reducer's responsibility
        for (int bucketIdx = 0; bucketIdx < numBuckets; bucketIdx++) {
            std::vector<int> bucketData;
            
            // Collect data from all mappers for this bucket
            for (int mapperIdx = 0; mapperIdx < numMappers; mapperIdx++) {
                const auto& mapperBucket = mapperBuckets[mapperIdx][bucketIdx];
                bucketData.insert(bucketData.end(), mapperBucket.begin(), mapperBucket.end());
            }
            
            // Sort the collected data (local sort within reducer)
            if (!bucketData.empty()) {
                std::sort(bucketData.begin(), bucketData.end());
                reducerBuckets[bucketIdx] = bucketData;
                
                std::cout << "  Reducer " << bucketIdx << " sorted " << bucketData.size() << " elements: ";
                printVector(bucketData, 8);
            }
        }
    }
    
    void generateFinalOutput() {
        finalOutput.clear();
        
        std::cout << "Merging sorted buckets:\n";
        
        // Concatenate all sorted reducer outputs in order
        // This naturally produces sorted output due to pivot-based partitioning
        for (int i = 0; i < reducerBuckets.size(); i++) {
            if (!reducerBuckets[i].empty()) {
                finalOutput.insert(finalOutput.end(), 
                                 reducerBuckets[i].begin(), 
                                 reducerBuckets[i].end());
                std::cout << "  Added bucket " << i << " (" << reducerBuckets[i].size() << " elements)\n";
            }
        }
    }
    
    // ========== USER INTERFACE METHODS ==========
    
    void printHeader() {
        std::cout << "╔════════════════════════════════════════════════════════════╗\n";
        std::cout << "║             DISTRIBUTED SORTING SIMULATOR                 ║\n";
        std::cout << "║                                                            ║\n";
        std::cout << "║  Implements the same Two-Phase MapReduce algorithm         ║\n";
        std::cout << "║  used in the web simulation                                ║\n";
        std::cout << "║                                                            ║\n";
        std::cout << "║  Phase 1: Pivot Selection                                  ║\n";
        std::cout << "║  Phase 2: Bucket Distribution & Sorting                   ║\n";
        std::cout << "╚════════════════════════════════════════════════════════════╝\n\n";
    }
    
    void printMenu() {
        std::cout << "=== MENU ===\n";
        std::cout << "1. Run with demo data (15 numbers)\n";
        std::cout << "2. Run with random data\n";
        std::cout << "3. Run with your custom data\n";
        std::cout << "4. Configure mappers/reducers\n";
        std::cout << "5. Performance comparison test\n";
        std::cout << "6. Exit\n";
        std::cout << "Choose option (1-6): ";
    }
    
    int getChoice() {
        int choice;
        std::cin >> choice;
        std::cin.ignore(); // Clear input buffer
        return choice;
    }
    
    void runWithDemoData() {
        std::vector<int> demoData = {64, 25, 12, 22, 11, 90, 5, 77, 30, 55, 73, 41, 85, 29, 67};
        std::cout << "\n=== DEMO DATA EXECUTION ===\n";
        distributedSort(demoData);
    }
    
    void runWithRandomData() {
        std::cout << "\nEnter data size (10-100 recommended): ";
        int size;
        std::cin >> size;
        size = std::max(5, std::min(500, size)); // Clamp to reasonable range
        
        std::vector<int> randomData = generateRandomData(size);
        std::cout << "\n=== RANDOM DATA EXECUTION ===\n";
        distributedSort(randomData);
    }
    
    void runWithCustomData() {
        std::cout << "\nEnter numbers separated by spaces (press Enter when done):\n";
        std::string line;
        std::getline(std::cin, line);
        
        std::vector<int> customData;
        std::istringstream iss(line);
        int num;
        while (iss >> num) {
            customData.push_back(num);
        }
        
        if (customData.empty()) {
            std::cout << "No valid numbers entered. Using demo data instead.\n";
            runWithDemoData();
        } else {
            std::cout << "\n=== CUSTOM DATA EXECUTION ===\n";
            distributedSort(customData);
        }
    }
    
    void configureSystem() {
        std::cout << "\nCurrent configuration: " << numMappers << " mappers, " << numReducers << " reducers\n";
        
        std::cout << "Enter number of mappers (2-8): ";
        int m;
        std::cin >> m;
        numMappers = std::max(2, std::min(8, m));
        
        int maxReducers = numMappers + 1;
        std::cout << "Enter number of reducers (2-" << maxReducers << ", max = mappers+1): ";
        int r;
        std::cin >> r;
        numReducers = std::max(2, std::min(maxReducers, r));
        
        std::cout << "Configuration updated: " << numMappers << " mappers, " << numReducers << " reducers\n";
        std::cout << "Note: Reducers are limited to mappers+1 (" << maxReducers << ") for optimal performance\n";
    }
    
    void runPerformanceTest() {
        std::cout << "\n=== PERFORMANCE TEST ===\n";
        std::cout << "Testing algorithm correctness with different data sizes...\n\n";
        
        std::vector<int> testSizes = {20, 50, 100, 200};
        
        for (int size : testSizes) {
            std::cout << "Testing with " << size << " elements:\n";
            
            std::vector<int> testData = generateRandomData(size);
            std::vector<int> expected = testData;
            std::sort(expected.begin(), expected.end());
            
            // Reset for clean test
            reset();
            std::vector<int> result = distributedSort(testData);
            
            bool correct = (result == expected);
            std::cout << "  Result: " << (correct ? "✓ CORRECT" : "✗ INCORRECT") << "\n";
            std::cout << "  Data movements: " << dataMovements << "\n";
            std::cout << "  Communication rounds: " << communicationRounds << "\n\n";
        }
    }
    
    // ========== UTILITY METHODS ==========
    
    std::vector<int> generateRandomData(int size) {
        std::vector<int> data;
        for (int i = 0; i < size; i++) {
            data.push_back(std::rand() % 1000 + 1);
        }
        return data;
    }
    
    void printVector(const std::vector<int>& vec, int maxShow = -1) {
        std::cout << "[";
        int limit = (maxShow > 0 && maxShow < static_cast<int>(vec.size())) ? maxShow : static_cast<int>(vec.size());
        
        for (int i = 0; i < limit; i++) {
            std::cout << vec[i];
            if (i < limit - 1) std::cout << ", ";
        }
        
        if (maxShow > 0 && static_cast<int>(vec.size()) > maxShow) {
            std::cout << " ... (+" << (vec.size() - maxShow) << " more)";
        }
        
        std::cout << "]\n";
    }
    
    void printResults() {
        std::cout << "\n=== RESULTS ===\n";
        std::cout << "Final sorted output: ";
        printVector(finalOutput, 20);
        
        // Verify correctness
        std::vector<int> expected = inputData;
        std::sort(expected.begin(), expected.end());
        bool correct = (finalOutput == expected);
        
        std::cout << "Correctness: " << (correct ? "✓ PASSED" : "✗ FAILED") << "\n";
        std::cout << "Elements processed: " << inputData.size() << "\n";
        std::cout << "Data movements: " << dataMovements << "\n";
        std::cout << "Communication rounds: " << communicationRounds << "\n";
        
        // Calculate load balancing
        if (!reducerBuckets.empty()) {
            int maxSize = 0;
            int minSize = INT_MAX;
            int totalSize = 0;
            int nonEmptyBuckets = 0;
            
            for (const auto& bucket : reducerBuckets) {
                if (!bucket.empty()) {
                    int bucketSize = static_cast<int>(bucket.size());
                    maxSize = std::max(maxSize, bucketSize);
                    minSize = std::min(minSize, bucketSize);
                    totalSize += bucketSize;
                    nonEmptyBuckets++;
                }
            }
            
            if (nonEmptyBuckets > 0) {
                double avgSize = static_cast<double>(totalSize) / nonEmptyBuckets;
                double loadBalance = (minSize == 0) ? 0.0 : (static_cast<double>(minSize) / maxSize) * 100;
                std::cout << "Load balance: " << std::fixed << std::setprecision(1) 
                         << loadBalance << "% (min: " << minSize 
                         << ", max: " << maxSize << ", avg: " << std::setprecision(1) << avgSize << ")\n";
            }
        }
    }
    
    void reset() {
        inputData.clear();
        mapperData.clear();
        localPivots.clear();
        globalPivots.clear();
        mapperBuckets.clear();
        reducerBuckets.clear();
        finalOutput.clear();
        communicationRounds = 0;
        dataMovements = 0;
    }
};

// ========== MAIN FUNCTION ==========

int main() {
    DistributedSortSimulator simulator;
    simulator.runInteractiveDemo();
    return 0;
}

/*
 * COMPILATION & USAGE EXAMPLES:
 * =============================
 * 
 * Windows (Visual Studio):
 *   cl standalone_distributed_sort.cpp
 *   standalone_distributed_sort.exe
 * 
 * Windows (MinGW):
 *   g++ standalone_distributed_sort.cpp -o sort.exe
 *   sort.exe
 * 
 * Linux/Mac:
 *   g++ standalone_distributed_sort.cpp -o sort
 *   ./sort
 * 
 * Online Compilers:
 *   - Copy this entire file to https://onlinegdb.com
 *   - Or https://replit.com (C++ template)
 *   - Or https://godbolt.org
 * 
 * ALGORITHM SUMMARY:
 * =================
 * 
 * This implementation faithfully reproduces the distributed sorting algorithm
 * from the web simulation using a two-phase MapReduce approach:
 * 
 * 1. PHASE 1 - PIVOT SELECTION:
 *    - Distribute input data among mappers (round-robin)
 *    - Each mapper selects a local pivot (random sampling)
 *    - Global pivots created by sorting local pivots
 * 
 * 2. PHASE 2 - BUCKET SORT:
 *    - Data partitioned into buckets using global pivots
 *    - Each reducer sorts its assigned bucket
 *    - Final result obtained by concatenating sorted buckets
 * 
 * The key insight is that pivot-based partitioning ensures that all elements
 * in bucket i are ≤ all elements in bucket i+1, so concatenating sorted
 * buckets produces a globally sorted result.
 * 
 * This matches the JavaScript implementation's logic exactly while providing
 * detailed step-by-step output for educational purposes.
 */