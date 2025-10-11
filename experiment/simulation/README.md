# Interactive Map-Reduce Sorting Experiment

This experiment demonstrates how the Map-Reduce programming framework orchestrates data movement to solve the sorting problem, even when compute volume is relatively low.

## Available Implementations

### 🌐 Web Simulation (`index.html`)
Interactive browser-based simulation with visual graphics and step-by-step execution.

### 💻 C++ Implementation 
Standalone C++ programs that implement the same sorting algorithm:
- `simple_distributed_sort.cpp` - Easy-to-understand version
- `distributed_sort.cpp` - Full-featured with detailed statistics
- `compile.bat` / `compile.ps1` - Compilation scripts for Windows
- See `cpp_implementation_README.md` for detailed instructions

## Features

- **Interactive Learning**: Step through each phase of Map-Reduce
- **Visual Data Movement**: See how data flows between components
- **Multiple Strategies**: Compare Hash vs Range partitioning
- **Real-time Statistics**: Monitor data movement and efficiency
- **Quiz System**: Test your understanding with interactive questions

## Map-Reduce Phases

### 1. Input Phase 📥
- Load your dataset of numbers to sort
- Configure number of mappers and reducers
- Choose partitioning strategy

### 2. Map Phase 🗺️
- Input data is distributed to mappers (round-robin)
- Each mapper transforms data into key-value pairs
- For sorting: (value, value) pairs are emitted

### 3. Shuffle & Partition Phase 🔄
- Data is grouped by key
- **Hash Partitioning**: `reducer = hash(key) % num_reducers`
- **Range Partitioning**: Data divided into sorted ranges
- Network data movement occurs here

### 4. Reduce Phase ⚙️
- Each reducer sorts its assigned data partition
- Parallel processing across multiple reducers
- Local sorting within each reducer

### 5. Output Phase 📤
- Final sorted result is produced
- Hash partitioning requires merging
- Range partitioning provides naturally ordered output

## Learning Objectives

1. **Data Movement Orchestration**: Understand how Map-Reduce coordinates data flow
2. **Partitioning Strategies**: Compare trade-offs between hash and range partitioning
3. **Parallelism**: See how work is distributed across multiple workers
4. **Network Efficiency**: Observe data movement patterns and costs
5. **Load Balancing**: Learn about data distribution challenges

## Usage Instructions

1. **Load Data**: Enter comma-separated numbers or use the default dataset
2. **Configure**: Set number of mappers/reducers and partitioning strategy
3. **Step Through**: Use "Step" button to go through each phase manually
4. **Auto Run**: Watch the complete process automatically
5. **Analyze**: Review statistics and data movement charts
6. **Quiz**: Answer questions to test your understanding

## Key Insights

- **Low Compute, High Coordination**: Sorting demonstrates how Map-Reduce excels at data orchestration
- **Trade-offs**: Hash partitioning provides load balancing but requires final merge
- **Scalability**: Adding more reducers can improve parallelism but increases coordination overhead
- **Data Skew**: Range partitioning can suffer from uneven data distribution

## Technical Implementation

- Pure JavaScript/HTML/CSS
- Chart.js for data visualization
- Interactive animations showing data movement
- Responsive design for various screen sizes
- Comprehensive error handling and user feedback
