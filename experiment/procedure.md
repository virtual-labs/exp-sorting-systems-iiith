

Follow these step-by-step instructions to perform the sorting through reducing experiment:

#### Step 1: Initialize the Experiment
1. **Load the Simulation**: Open the sorting simulation interface
2. **Review Controls**: Familiarize yourself with the available buttons and options:
   - Input array field
   - Pivot selection dropdown
   - Speed control slider
   - Step-by-step execution buttons
   - Reset button

#### Step 2: Input Data
1. **Enter Array Elements**: 
   - Input a custom array of integers (e.g., [64, 34, 25, 12, 22, 11, 90])
   - Or use the "Generate Random Array" button for automatic data generation
   - Ensure array size is between 5-20 elements for optimal visualization

2. **Select Array Size**: Choose appropriate array size using the size selector (recommended: 8-12 elements)

#### Step 3: Choose Pivot Selection Strategy
1. **Select Pivot Method** from the dropdown:
   - **First Element**: Uses the first element as pivot
   - **Last Element**: Uses the last element as pivot
   - **Random Element**: Randomly selects pivot
   - **Median-of-Three**: Uses median of first, middle, and last elements

2. **Observe Pivot Highlighting**: The selected pivot will be highlighted in a distinct color

#### Step 4: Execute the Algorithm
1. **Start Sorting**: Click the "Start QuickSort" button to begin the algorithm

2. **Monitor Partitioning Process**:
   - Watch how the array gets partitioned around the pivot
   - Observe elements moving to correct sides of the pivot
   - Note the color coding:
     - **Red**: Current pivot element
     - **Blue**: Elements being compared
     - **Green**: Elements in correct position
     - **Yellow**: Current partition boundaries

3. **Step-by-Step Execution** (Optional):
   - Use "Step Forward" button to advance one operation at a time
   - Use "Step Backward" button to review previous steps
   - Observe the recursion stack visualization on the right panel

#### Step 5: Analyze the Process
1. **Observe Recursion Depth**:
   - Monitor the recursion tree displayed in the side panel
   - Note how problem size reduces with each recursive call
   - Count the maximum depth of recursion

2. **Track Performance Metrics**:
   - **Comparisons**: Number of element comparisons made
   - **Swaps**: Number of element swaps performed
   - **Recursive Calls**: Total number of function calls
   - **Time Complexity**: Observe if it's O(n log n) or O(n²)

#### Step 6: Experiment with Different Scenarios
1. **Test Different Pivot Strategies**:
   - Reset the simulation
   - Try the same array with different pivot selection methods
   - Compare the number of comparisons and swaps for each method

2. **Test Edge Cases**:
   - **Already Sorted Array**: [1, 2, 3, 4, 5, 6, 7, 8]
   - **Reverse Sorted Array**: [8, 7, 6, 5, 4, 3, 2, 1]
   - **Array with Duplicates**: [5, 2, 8, 2, 9, 1, 5, 5]
   - **Single Element**: [42]

#### Step 7: Compare Performance
1. **Record Measurements** for different inputs:
   - Create a table with columns: Array Type, Pivot Method, Comparisons, Swaps, Time
   - Fill in data for at least 5 different test cases

2. **Analyze Results**:
   - Which pivot strategy performs best on average?
   - Which input causes worst-case behavior?
   - How does performance scale with array size?

#### Step 8: Advanced Exploration
1. **3-Way Partitioning** (if available):
   - Test arrays with many duplicate elements
   - Observe how 3-way partitioning handles duplicates more efficiently

2. **Hybrid Algorithm** (if available):
   - Notice when the algorithm switches to insertion sort for small subarrays
   - Observe the threshold size for switching algorithms

#### Step 9: Documentation
1. **Take Screenshots**: Capture key moments in the sorting process
2. **Note Observations**: 
   - Which pivot strategy worked best for your test cases?
   - What was the maximum recursion depth observed?
   - How did duplicate elements affect performance?

#### Step 10: Reset and Repeat
1. **Reset the Simulation**: Use the reset button to clear all data
2. **Try New Configurations**: Experiment with different array sizes and pivot strategies
3. **Compare Results**: Build a comprehensive understanding of algorithm behavior

#### Expected Learning Outcomes
After completing this procedure, you should be able to:
- Understand how QuickSort partitions arrays around pivot elements
- Recognize the impact of pivot selection on algorithm performance
- Visualize the divide-and-conquer approach in action
- Identify best-case, average-case, and worst-case scenarios
- Appreciate the efficiency of partition-based sorting algorithms

#### Troubleshooting Tips
- If animation is too fast, use the speed control slider to slow it down
- If you lose track of the process, use the step-by-step controls
- Reset the simulation if you encounter any unexpected behavior
- Take notes during the experiment to remember key observations