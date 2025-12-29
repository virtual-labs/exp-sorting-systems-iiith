// Distributed Sort Using MapReduce - Two Phase Implementation
// =============================================================
// Based on DistributedSort.java logic with pivot-based sample sort

class DistributedSortExperiment {
    constructor() {
        this.currentPhase = 'input';
        this.isRunning = false;
        this.isPaused = false;
        this.autoMode = false;
        
        // Two-phase approach like DistributedSort.java
        this.inputData = [];
        this.mappers = [];
        this.reducers = [];
        
        // Phase 1: Pivot Selection
        this.localPivots = [];  // Each mapper selects a local pivot
        this.globalPivots = [];  // Global pivots selected from local pivots
        
        // Phase 2: Bucket Sort
        this.mapperBuckets = new Map();  // Mapper data distributed into buckets
        this.reducerBuckets = new Map();  // Final sorted buckets
        this.finalOutput = [];
        
        // Communication tracking (like DistributedSort.java)
        this.communicationRounds = 0;
        this.mapperOutputs = 0;
        
        this.statistics = {
            dataMovement: 0,
            networkLoad: 0,
            parallelism: 1,
            efficiency: 0,
            pivotSelectionRounds: 0,
            bucketSortRounds: 0
        };
        
        // Activity log and experiment statistics
        this.logContainer = null;
        this.experimentStats = {
            totalSorts: 0,
            totalElementsProcessed: 0,
            totalCommunicationRounds: 0,
            totalExperiments: 0
        };
        
        // Interactive elements
        this.predictionScore = { correct: 0, total: 0 };
        this.trackingItem = null;
        this.currentTargetItem = null;
        this.dataMovements = 0;
        this.performanceMetrics = {
            movements: 0,
            loadBalance: 'Perfect',
            efficiency: 100
        };
        
        this.quizQuestions = [
            {
                question: "In Phase 1 of distributed sort, what do mappers do with their local data?",
                options: [
                    "Sort all data immediately",
                    "Select random local pivots from their data",
                    "Distribute data to reducers",
                    "Merge with other mappers"
                ],
                correct: 1,
                explanation: "In Phase 1, each mapper selects a local pivot (sample) from its data chunk. These local pivots are then collected to determine global pivots."
            },
            {
                question: "What is the purpose of global pivots in distributed sort?",
                options: [
                    "To speed up local sorting",
                    "To reduce network traffic",
                    "To partition data into balanced buckets across all mappers",
                    "To eliminate the need for reducers"
                ],
                correct: 2,
                explanation: "Global pivots are used to partition data into buckets that ensure balanced distribution across reducers, preventing load imbalance."
            },
            {
                question: "In Phase 2, how do mappers decide which bucket a data element belongs to?",
                options: [
                    "Using hash function like hash(value) % reducers",
                    "Comparing the value against sorted global pivots",
                    "Random assignment to any bucket",
                    "Round-robin distribution"
                ],
                correct: 1,
                explanation: "Mappers compare each data element against the sorted global pivots to determine which bucket (reducer) it should be sent to."
            },
            {
                question: "What advantage does this two-phase approach have over simple hash partitioning?",
                options: [
                    "Faster execution time",
                    "Less memory usage",
                    "Better load balancing with unknown data distributions",
                    "Simpler implementation"
                ],
                correct: 2,
                explanation: "The two-phase pivot-based approach provides better load balancing because pivots are chosen based on actual data samples, adapting to the data distribution."
            },
            {
                question: "In the final output, how are the sorted buckets combined?",
                options: [
                    "They need to be merged and sorted again",
                    "They are concatenated in reducer order (naturally sorted)",
                    "They are randomly shuffled",
                    "Only the largest bucket is used"
                ],
                correct: 1,
                explanation: "Since each reducer sorts its bucket locally and buckets are partitioned by global pivots, the final output is created by concatenating reducer outputs in order."
            },
            {
                question: "What metric helps measure the efficiency of the distributed sort?",
                options: [
                    "Total memory usage",
                    "Number of comparison operations",
                    "Communication rounds between phases",
                    "File I/O operations"
                ],
                correct: 2,
                explanation: "Communication rounds measure how much data needs to be transferred between mappers and reducers, which is a key efficiency metric in distributed systems."
            }
        ];
        
        this.currentQuiz = null;
        this.selectedQuizOption = undefined;
        
        this.initializeEventListeners();
        this.initializeChart();
        this.initializeLog();
        this.loadDefaultData();
    }
    
    // Generate random data similar to DistributedSort.java
    generateRandomData(size) {
        const data = [];
        const half = Math.floor(size / 2);
        
        // First half: range [1, 200]
        for (let i = 0; i < half; i++) {
            data.push(Math.floor(Math.random() * 200) + 1);
        }
        
        // Second half: range [201, 1000]
        for (let i = half; i < size; i++) {
            data.push(Math.floor(Math.random() * 800) + 201);
        }
        
        return data;
    }
    
    // Simulate local pivot selection like PivotMapper in DistributedSort.java
    selectLocalPivots() {
        this.localPivots = [];
        const mapperCount = parseInt(document.getElementById('mapperCount').value);
        
        // Distribute data among mappers
        const dataPerMapper = Math.ceil(this.inputData.length / mapperCount);
        this.mappers = [];
        
        for (let i = 0; i < mapperCount; i++) {
            const startIdx = i * dataPerMapper;
            const endIdx = Math.min(startIdx + dataPerMapper, this.inputData.length);
            const mapperData = this.inputData.slice(startIdx, endIdx);
            
            this.mappers.push({
                id: i,
                data: mapperData,
                localPivot: null
            });
            
            // Select random local pivot from mapper's data
            if (mapperData.length > 0) {
                const randomIdx = Math.floor(Math.random() * mapperData.length);
                const localPivot = mapperData[randomIdx];
                this.mappers[i].localPivot = localPivot;
                this.localPivots.push(localPivot);
                this.mapperOutputs++; // Track communication like DistributedSort.java
            }
        }
    }
    
    // Simulate global pivot selection like PivotReducer in DistributedSort.java
    selectGlobalPivots() {
        // Sort local pivots and select global pivots
        this.localPivots.sort((a, b) => a - b);
        this.globalPivots = [...this.localPivots]; // Use all local pivots as global pivots
        
        // Track communication round
        this.communicationRounds += this.mapperOutputs;
        this.statistics.pivotSelectionRounds = this.mapperOutputs;
        this.mapperOutputs = 0;
    }
    
    // Simulate bucket distribution like BucketMapper in DistributedSort.java
    distributeToBuckets() {
        const reducerCount = parseInt(document.getElementById('reducerCount').value);
        
        // Initialize reducers array
        this.reducers = [];
        for (let i = 0; i < reducerCount; i++) {
            this.reducers.push({ id: i, data: [] });
        }
        
        // Initialize buckets for each mapper
        this.mapperBuckets.clear();
        
        this.mappers.forEach(mapper => {
            const buckets = new Map();
            
            // Initialize buckets for this mapper 
            // We need globalPivots.length + 1 buckets (one more than the number of pivots)
            const numBuckets = Math.max(reducerCount + 1, this.globalPivots.length + 1);
            for (let i = 0; i < numBuckets; i++) {
                buckets.set(i, []);
            }
            
            // Distribute mapper's data into buckets based on global pivots
            mapper.data.forEach(value => {
                let bucket = 0;
                // Find appropriate bucket using global pivots
                while (bucket < this.globalPivots.length && value >= this.globalPivots[bucket]) {
                    bucket++;
                }
                
                // Ensure bucket exists
                if (!buckets.has(bucket)) {
                    buckets.set(bucket, []);
                }
                
                buckets.get(bucket).push(value);
                this.mapperOutputs++; // Track communication
            });
            
            this.mapperBuckets.set(mapper.id, buckets);
        });
    }
    
    // Simulate final sorting like SortReducer in DistributedSort.java
    performFinalSort() {
        const reducerCount = parseInt(document.getElementById('reducerCount').value);
        this.reducerBuckets.clear();
        
        // Find the maximum bucket ID used
        let maxBucketId = 0;
        this.mapperBuckets.forEach(mapperBuckets => {
            for (let bucketId of mapperBuckets.keys()) {
                maxBucketId = Math.max(maxBucketId, bucketId);
            }
        });
        
        // Each reducer collects its bucket from all mappers and sorts
        for (let reducerId = 0; reducerId <= maxBucketId; reducerId++) {
            const reducerData = [];
            
            // Collect data from all mappers for this reducer
            this.mapperBuckets.forEach(mapperBuckets => {
                const bucketData = mapperBuckets.get(reducerId) || [];
                reducerData.push(...bucketData);
            });
            
            // Only store non-empty buckets
            if (reducerData.length > 0) {
                // Sort the collected data
                reducerData.sort((a, b) => a - b);
                this.reducerBuckets.set(reducerId, reducerData);
            }
        }
        
        // Track final communication round
        this.communicationRounds += this.mapperOutputs;
        this.statistics.bucketSortRounds = this.mapperOutputs;
        this.mapperOutputs = 0;
        
        // Generate final output by concatenating sorted buckets in order
        this.finalOutput = [];
        for (let i = 0; i <= maxBucketId; i++) {
            const bucketData = this.reducerBuckets.get(i) || [];
            this.finalOutput.push(...bucketData);
        }
    }
    
    initializeEventListeners() {
        // Modal controls
        document.getElementById('floatingInstructionsBtn').addEventListener('click', () => {
            this.showModal();
        });
        
        document.getElementById('closeInstructionsModal').addEventListener('click', () => {
            this.hideModal();
        });
        
        document.getElementById('instructionsModal').addEventListener('click', (e) => {
            if (e.target.id === 'instructionsModal') {
                this.hideModal();
            }
        });
        
        // Main controls
        document.getElementById('resetBtn').addEventListener('click', () => {
            this.resetExperiment();
        });
        
        document.getElementById('stepBtn').addEventListener('click', () => {
            this.log('Manual step forward initiated', 'info');
            this.stepNext();
        });
        
        document.getElementById('autoRunBtn').addEventListener('click', () => {
            this.log('Auto-run mode started', 'info');
            this.startAutoRun();
        });
        
        document.getElementById('pauseBtn').addEventListener('click', () => {
            this.log('Auto-run mode paused', 'warning');
            this.pauseAutoRun();
        });
        
        document.getElementById('loadDataBtn').addEventListener('click', () => {
            this.loadInputData();
        });
        
        // C++ Download button
        document.getElementById('downloadCppBtn').addEventListener('click', () => {
            this.downloadCppCode();
        });
        
        // Configuration changes
        document.getElementById('mapperCount').addEventListener('change', () => {
            this.updateConfiguration();
        });
        
        document.getElementById('reducerCount').addEventListener('change', () => {
            this.updateConfiguration();
        });
        
        document.getElementById('partitionStrategy').addEventListener('change', () => {
            this.updateConfiguration();
        });
        
        // Spacebar for pause/step
        document.addEventListener('keydown', (e) => {
            if (e.code === 'Space') {
                e.preventDefault();
                if (this.autoMode) {
                    this.pauseAutoRun();
                } else {
                    this.stepNext();
                }
            }
        });
        
        // Quiz controls - use optional chaining to avoid errors if elements don't exist
        document.getElementById('submitQuiz')?.addEventListener('click', () => {
            this.submitQuiz();
        });
        
        document.getElementById('skipQuiz')?.addEventListener('click', () => {
            this.skipQuiz();
        });
    }
    
    resetExperiment() {
        this.currentPhase = 'input';
        this.isRunning = false;
        this.isPaused = false;
        this.autoMode = false;
        
        this.log('Experiment reset - ready for new configuration', 'info');
        
        // Reset all data structures
        this.localPivots = [];
        this.globalPivots = [];
        this.mapperBuckets.clear();
        this.reducerBuckets.clear();
        this.finalOutput = [];
        this.mappers = [];
        this.reducers = [];
        
        // Reset communication tracking
        this.communicationRounds = 0;
        this.mapperOutputs = 0;
        
        // Reset prediction score
        this.predictionScore = { correct: 0, total: 0 };
        const scoreElement = document.getElementById('predictionScore');
        if (scoreElement) scoreElement.textContent = '0/0';
        
        const resultElement = document.getElementById('predictionResult');
        if (resultElement) resultElement.innerHTML = '';
        
        this.statistics = {
            dataMovement: 0,
            networkLoad: 0,
            parallelism: 1,
            efficiency: 0,
            pivotSelectionRounds: 0,
            bucketSortRounds: 0
        };
        
        this.updateConfiguration();
        this.updatePhaseIndicator();
        this.renderInputData();
        this.hideQuiz();
        this.updateStatistics();
        this.updateChart();
        this.setupPredictionGame();
        this.updatePerformanceMetrics();
        
        document.getElementById('autoRunBtn').classList.remove('hidden');
        document.getElementById('pauseBtn').classList.add('hidden');
    }
    
    stepNext() {
        if (this.autoMode) return;
        
        switch (this.currentPhase) {
            case 'input':
                this.startPivotSelectionPhase();
                break;
            case 'pivot-selection':
                this.startBucketDistributionPhase();
                break;
            case 'bucket-distribution':
                this.startFinalSortPhase();
                break;
            case 'final-sort':
                this.generateFinalOutput();
                break;
            case 'output':
                this.showQuiz();
                break;
        }
        
        // Update interactive elements
        this.updatePerformanceMetrics();
    }
    
    startAutoRun() {
        this.autoMode = true;
        document.getElementById('autoRunBtn').classList.add('hidden');
        document.getElementById('pauseBtn').classList.remove('hidden');
        
        this.runNextPhaseAuto();
    }
    
    pauseAutoRun() {
        this.autoMode = false;
        this.isPaused = true;
        document.getElementById('autoRunBtn').classList.remove('hidden');
        document.getElementById('pauseBtn').classList.add('hidden');
    }
    
    runNextPhaseAuto() {
        if (!this.autoMode) return;
        
        setTimeout(() => {
            if (!this.autoMode) return;
            
            switch (this.currentPhase) {
                case 'input':
                    this.startPivotSelectionPhase();
                    this.runNextPhaseAuto();
                    break;
                case 'pivot-selection':
                    this.startBucketDistributionPhase();
                    this.runNextPhaseAuto();
                    break;
                case 'bucket-distribution':
                    this.startFinalSortPhase();
                    this.runNextPhaseAuto();
                    break;
                case 'final-sort':
                    this.generateFinalOutput();
                    this.runNextPhaseAuto();
                    break;
                case 'output':
                    this.showQuiz();
                    this.autoMode = false;
                    document.getElementById('autoRunBtn').classList.remove('hidden');
                    document.getElementById('pauseBtn').classList.add('hidden');
                    break;
            }
        }, 2500);
    }
    
    // Phase 1: Pivot Selection (like Job 1 in DistributedSort.java)
    startPivotSelectionPhase() {
        this.currentPhase = 'pivot-selection';
        this.log('Phase 1: Starting pivot selection phase', 'info');
        this.selectLocalPivots();
        this.selectGlobalPivots();
        this.log(`Local pivots selected: [${this.localPivots.join(', ')}]`, 'success');
        this.log(`Global pivots created: [${this.globalPivots.join(', ')}]`, 'success');
        this.updatePhaseIndicator();
        this.renderPivotSelectionPhase();
        this.updateStatistics();
    }
    
    // Phase 2: Bucket Distribution (like Job 2 setup in DistributedSort.java)
    startBucketDistributionPhase() {
        this.currentPhase = 'bucket-distribution';
        this.log('Phase 2: Starting bucket distribution phase', 'info');
        this.distributeToBuckets();
        this.log(`Data distributed into ${this.reducers.length} buckets across ${this.mappers.length} mappers`, 'success');
        this.updatePhaseIndicator();
        this.renderBucketDistributionPhase();
        this.updateStatistics();
    }
    
    // Phase 3: Final Sort (like Job 2 reduce in DistributedSort.java)
    startFinalSortPhase() {
        this.currentPhase = 'final-sort';
        this.log('Phase 3: Starting final sort phase', 'info');
        this.performFinalSort();
        const sortedBuckets = Array.from(this.reducerBuckets.values()).filter(bucket => bucket.length > 0);
        this.log(`Final sorting completed: ${sortedBuckets.length} non-empty buckets processed`, 'success');
        this.updatePhaseIndicator();
        this.renderFinalSortPhase();
        this.updateStatistics();
    }
    
    // Phase 4: Generate Final Output
    generateFinalOutput() {
        this.currentPhase = 'output';
        this.log('Phase 4: Generating final output', 'info');
        const isCorrectlySorted = this.isSorted(this.finalOutput);
        this.log(`Final output generated: ${this.finalOutput.length} elements, ${isCorrectlySorted ? 'correctly sorted' : 'sorting failed'}`, isCorrectlySorted ? 'success' : 'error');
        this.log(`Total communication rounds: ${this.communicationRounds}`, 'info');
        this.updatePhaseIndicator();
        this.renderFinalOutput();
        this.updateStatistics();
        this.updateChart();
        this.recordExperimentCompletion();
    }
    
    // Rendering Methods for Visualization
    renderPivotSelectionPhase() {
        // Clear previous visualizations
        document.getElementById('mappersViz').innerHTML = '';
        document.getElementById('shuffleViz').innerHTML = '';
        document.getElementById('reducersViz').innerHTML = '';
        
        const mappersContainer = document.getElementById('mappersViz');
        
        // Show mappers with their local data and selected pivots
        this.mappers.forEach((mapper, index) => {
            const mapperDiv = document.createElement('div');
            mapperDiv.className = 'mapper-card';
            mapperDiv.innerHTML = `
                <h4>Mapper ${index + 1}</h4>
                <div class="mb-2">
                    <strong>Data:</strong> 
                    <div class="worker-data">
                        ${mapper.data.map(value => 
                            `<span class="data-item">${value}</span>`
                        ).join('')}
                    </div>
                </div>
                <div class="mt-2">
                    <strong>Local Pivot:</strong> 
                    <span class="bg-red-500 text-white px-2 py-1 rounded font-bold">${mapper.localPivot}</span>
                </div>
            `;
            mappersContainer.appendChild(mapperDiv);
        });
        
        // Show global pivots in shuffle area
        const shuffleContainer = document.getElementById('shuffleViz');
        shuffleContainer.innerHTML = `
            <div class="text-center">
                <h4 class="font-semibold text-purple-700 mb-2">Global Pivots (Sorted)</h4>
                <div class="flex justify-center gap-2">
                    ${this.globalPivots.map(pivot => 
                        `<span class="bg-purple-500 text-white px-3 py-2 rounded-lg font-bold">${pivot}</span>`
                    ).join('')}
                </div>
                <p class="text-sm text-gray-600 mt-2">
                    Communication Rounds: <strong>${this.statistics.pivotSelectionRounds}</strong>
                </p>
            </div>
        `;
    }
    
    renderBucketDistributionPhase() {
        const mappersContainer = document.getElementById('mappersViz');
        mappersContainer.innerHTML = '';
        
        // Show mappers with their bucket distributions
        this.mappers.forEach((mapper, index) => {
            const mapperDiv = document.createElement('div');
            mapperDiv.className = 'mapper-card';
            
            const buckets = this.mapperBuckets.get(index);
            let bucketsHtml = '';
            
            for (let [bucketId, bucketData] of buckets) {
                if (bucketData.length > 0) {
                    bucketsHtml += `
                        <div class="mb-2">
                            <strong>→ Reducer ${bucketId + 1}:</strong>
                            <div class="worker-data">
                                ${bucketData.map(value => 
                                    `<span class="data-item">${value}</span>`
                                ).join('')}
                            </div>
                        </div>
                    `;
                }
            }
            
            mapperDiv.innerHTML = `
                <h4>Mapper ${index + 1} Buckets</h4>
                ${bucketsHtml}
            `;
            mappersContainer.appendChild(mapperDiv);
        });

        // Create animated shuffle visualization
        this.renderShuffleAnimation();
    }

    renderShuffleAnimation() {
        const shuffleContainer = document.getElementById('shuffleViz');
        
        // Calculate dynamic height based on number of mappers and reducers
        const maxItems = Math.max(this.mappers.length, this.reducers.length);
        const baseHeight = 80; // Base height for headers and padding
        const itemHeight = 28; // Height per mapper/reducer box
        const dynamicHeight = baseHeight + (maxItems * itemHeight);
        
        shuffleContainer.innerHTML = `
            <div class="shuffle-animation-container">
                <h4 class="text-center font-semibold text-orange-700 mb-4">🔄 Phase 2: Data Distribution</h4>
                <div id="animationArea" style="position: relative; background: linear-gradient(to right, rgba(239, 246, 255, 0.8), rgba(240, 253, 244, 0.8)); border: 2px dashed rgba(246, 173, 85, 0.6); border-radius: 8px; margin: 10px 0; padding: 15px; height: ${dynamicHeight}px; min-height: ${dynamicHeight}px; min-width: 500px; overflow: visible; display: flex; align-items: center; justify-content: space-between;">
                    <!-- Mapper side -->
                    <div style="position: absolute; top: 8px; left: 15px; font-size: 0.875rem; font-weight: 600; color: #2563eb;">📦 Mappers</div>
                    <div style="position: absolute; left: 15px; top: 30px;">
                        ${this.mappers.map((_, index) => 
                            `<div style="width: 36px; height: 24px; background: rgba(191, 219, 254, 1); border: 1px solid #3b82f6; border-radius: 4px; font-size: 0.75rem; display: flex; align-items: center; justify-content: center; margin-bottom: 2px;">M${index + 1}</div>`
                        ).join('')}
                    </div>
                    
                    <!-- Arrow and flow area -->
                    <div style="position: absolute; left: 110px; right: 110px; top: 50%; transform: translateY(-50%);">
                        <div style="text-align: center; font-size: 1.5rem;">
                            <span style="animation: pulse 1.5s ease-in-out infinite;">→ → →</span>
                        </div>
                    </div>
                    
                    <!-- Reducer side -->
                    <div style="position: absolute; top: 8px; right: 15px; font-size: 0.875rem; font-weight: 600; color: #16a34a;">⚙️ Reducers</div>
                    <div style="position: absolute; right: 15px; top: 30px;">
                        ${this.reducers.map((_, index) => 
                            `<div style="width: 36px; height: 24px; background: rgba(187, 247, 208, 1); border: 1px solid #10b981; border-radius: 4px; font-size: 0.75rem; display: flex; align-items: center; justify-content: center; margin-bottom: 2px;">R${index + 1}</div>`
                        ).join('')}
                    </div>
                    
                    <!-- Animation area for moving items -->
                    <div id="movingData" style="position: absolute; top: 0; left: 0; right: 0; bottom: 0;"></div>
                    
                    <!-- Progress bar -->
                    <div style="position: absolute; bottom: 8px; left: 16px; right: 16px;">
                        <div style="font-size: 0.75rem; color: #6b7280; margin-bottom: 4px; text-align: center;">Distribution Progress</div>
                        <div style="width: 100%; background-color: #e5e7eb; border-radius: 9999px; height: 8px;">
                            <div id="progressBar" style="background: linear-gradient(to right, #3b82f6, #10b981); height: 8px; border-radius: 9999px; transition: all 0.3s ease; width: 0%;"></div>
                        </div>
                    </div>
                </div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; text-align: center;">
                    <div style="background: rgba(239, 246, 255, 1); border-radius: 8px; padding: 12px;">
                        <div style="font-size: 0.875rem; color: #6b7280;">Items Sent</div>
                        <div style="font-size: 1.25rem; font-weight: bold; color: #2563eb;" id="itemsSent">0</div>
                    </div>
                    <div style="background: rgba(240, 253, 244, 1); border-radius: 8px; padding: 12px;">
                        <div style="font-size: 0.875rem; color: #6b7280;">Communication Rounds</div>
                        <div style="font-size: 1.25rem; font-weight: bold; color: #16a34a;" id="liveCommRounds">0</div>
                    </div>
                </div>
                <div style="text-align: center; margin-top: 8px;">
                    <div style="font-size: 0.875rem; color: #6b7280;">
                        <span id="animationStatus">Starting data distribution...</span>
                    </div>
                </div>
            </div>
        `;
        
        // Start the animation
        this.startDataMovementAnimation();
    }

    startDataMovementAnimation() {
        const animationArea = document.getElementById('movingData');
        const statusElement = document.getElementById('animationStatus');
        const commRoundsElement = document.getElementById('liveCommRounds');
        const itemsSentElement = document.getElementById('itemsSent');
        const progressBar = document.getElementById('progressBar');
        
        let currentCommRounds = 0;
        let itemsSent = 0;
        const totalItems = this.inputData.length;
        
        this.log(`Starting data movement animation - ${totalItems} items to distribute`, 'info');
        
        // Clear any existing animations
        animationArea.innerHTML = '';
        
        // Function to create moving data item with enhanced animation
        const createMovingItem = (value, delay, sourceMapper, targetReducer) => {
            setTimeout(() => {
                const item = document.createElement('div');
                item.className = 'moving-data-item';
                item.textContent = value;
                
                // Start position (near source mapper)
                const startX = 30 + (sourceMapper * 2); // Closer to left edge, less spacing
                const startY = sourceMapper; // Better alignment with mapper boxes
                
                item.style.cssText = `
                    position: absolute;
                    left: ${startX}px;
                    top: ${startY}px;
                    background: linear-gradient(135deg, #3182ce, #4299e1);
                    color: white;
                    padding: 4px 8px;
                    border-radius: 6px;
                    font-size: 11px;
                    font-weight: bold;
                    transition: all 2s cubic-bezier(0.25, 0.46, 0.45, 0.94);
                    z-index: 10;
                    box-shadow: 0 2px 8px rgba(49, 130, 206, 0.4);
                    border: 1px solid rgba(255,255,255,0.3);
                `;
                
                animationArea.appendChild(item);
                
                // Add floating effect during movement
                setTimeout(() => {
                    item.style.transform = 'translateY(-8px)';
                }, 50);
                
                // Animate to target position (near target reducer)
                setTimeout(() => {
                    // Fixed target position to align with reducers on the right
                    const containerWidth = animationArea.offsetWidth;
                    const targetX = containerWidth - 60; // Position near the right edge
                    const targetY = 45 + (targetReducer * 28); // Better vertical spacing
                    
                    item.style.left = `${targetX}px`;
                    item.style.top = `${targetY}px`;
                    item.style.background = `linear-gradient(135deg, #10b981, #34d399)`;
                    item.style.boxShadow = '0 4px 12px rgba(16, 185, 129, 0.4)';
                    item.style.transform = 'translateY(0px) scale(1.1)';
                    
                    // Update counters
                    currentCommRounds++;
                    itemsSent++;
                    commRoundsElement.textContent = currentCommRounds;
                    itemsSentElement.textContent = itemsSent;
                    
                    // Update progress bar
                    const progress = (itemsSent / totalItems) * 100;
                    progressBar.style.width = `${progress}%`;
                    
                    statusElement.textContent = `Transferring ${value} from Mapper ${sourceMapper + 1} to Reducer ${targetReducer + 1}`;
                }, 200);
                
                // Fade out and remove item
                setTimeout(() => {
                    item.style.opacity = '0';
                    item.style.transform = 'translateY(0px) scale(0.8)';
                }, 1800);
                
                setTimeout(() => {
                    if (item.parentNode) {
                        item.remove();
                    }
                }, 2200);
                
            }, delay);
        };
        
        // Animate each data item with realistic source mapping
        let delay = 500; // Start delay
        this.mappers.forEach((mapper, mapperId) => {
            const buckets = this.mapperBuckets.get(mapperId);
            buckets.forEach((bucketData, bucketId) => {
                bucketData.forEach((value) => {
                    createMovingItem(value, delay, mapperId, bucketId);
                    delay += 400; // Stagger animations more
                });
            });
        });
        
        // Update status messages with more detail
        const statusMessages = [
            "🔍 Analyzing global pivots for data partitioning...",
            "📊 Calculating bucket assignments for each data item...",
            "🚀 Transferring data from mappers to reducers...",
            "✅ Data distribution phase completed successfully!"
        ];
        
        statusMessages.forEach((message, index) => {
            setTimeout(() => {
                statusElement.textContent = message;
            }, (index * delay) / 4);
        });
        
        // Final status with summary
        setTimeout(() => {
            statusElement.textContent = `🎉 Distribution complete! ${totalItems} items transferred to ${this.reducerBuckets.size} reducers.`;
            // Keep the final communication rounds count from the animation
            // commRoundsElement.textContent already shows the correct count from the animation
            // Update the main communicationRounds to match the animation count
            this.communicationRounds = currentCommRounds;
            progressBar.style.width = '100%';
            
            this.log(`Data distribution animation completed: ${totalItems} items transferred in ${currentCommRounds} rounds`, 'success');
            
            // Add completion effect
            progressBar.style.background = 'linear-gradient(135deg, #10b981, #34d399)';
            progressBar.style.boxShadow = '0 2px 8px rgba(16, 185, 129, 0.4)';
        }, delay + 1500);
    }
    
    renderFinalSortPhase() {
        const reducersContainer = document.getElementById('reducersViz');
        reducersContainer.innerHTML = '';
        
        // Show reducers with their sorted data
        for (let [reducerId, sortedData] of this.reducerBuckets) {
            if (sortedData.length > 0) {
                const reducerDiv = document.createElement('div');
                reducerDiv.className = 'reducer-card';
                reducerDiv.innerHTML = `
                    <h4>Reducer ${reducerId + 1}</h4>
                    <div class="mb-2">
                        <strong>Sorted Data:</strong>
                        <div class="worker-data">
                            ${sortedData.map(value => 
                                `<span class="data-item">${value}</span>`
                            ).join('')}
                        </div>
                    </div>
                    <div class="text-sm text-gray-600">
                        Count: <strong>${sortedData.length}</strong> elements
                    </div>
                `;
                reducersContainer.appendChild(reducerDiv);
            }
        }
    }
    
    renderFinalOutput() {
        const outputContainer = document.getElementById('outputDataViz');
        outputContainer.innerHTML = '';
        
        if (this.finalOutput.length > 0) {
            const outputDiv = document.createElement('div');
            outputDiv.className = 'w-full';
            outputDiv.innerHTML = `
                <div class="flex flex-wrap gap-2">
                    ${this.finalOutput.map((value, index) => 
                        `<span class="bg-green-500 text-white px-2 py-1 rounded font-semibold">${value}</span>`
                    ).join('')}
                </div>
                <div class="mt-3 text-sm text-gray-600">
                    <strong>Total Elements:</strong> ${this.finalOutput.length} | 
                    <strong>Total Communication Rounds:</strong> ${this.communicationRounds} |
                    <strong>Is Sorted:</strong> ${this.isSorted(this.finalOutput) ? '✅ Yes' : '❌ No'}
                </div>
            `;
            outputContainer.appendChild(outputDiv);
        }
    }
    
    isSorted(array) {
        for (let i = 1; i < array.length; i++) {
            if (array[i] < array[i - 1]) {
                return false;
            }
        }
        return true;
    }
    
    updatePhaseIndicator() {
        const phases = {
            'input': 'Input Data Loaded',
            'pivot-selection': 'Phase 1: Pivot Selection',
            'bucket-distribution': 'Phase 2: Bucket Distribution',
            'final-sort': 'Phase 3: Final Sort',
            'output': 'Output Generated'
        };
        
        // Update phase display if element exists
        const phaseIndicator = document.getElementById('currentPhase');
        if (phaseIndicator) {
            phaseIndicator.textContent = phases[this.currentPhase] || this.currentPhase;
        }
    }
    
    renderInputData() {
        const container = document.getElementById('inputDataViz');
        container.innerHTML = '';
        
        if (this.inputData.length > 0) {
            const dataDiv = document.createElement('div');
            dataDiv.className = 'flex flex-wrap gap-2';
            dataDiv.innerHTML = this.inputData.map(value => 
                `<span class="bg-gray-200 text-gray-800 px-3 py-1 rounded-lg font-medium">${value}</span>`
            ).join('');
            container.appendChild(dataDiv);
        }
    }
    
    loadInputData() {
        const inputText = document.getElementById('inputData').value.trim();
        if (inputText) {
            // Parse comma-separated values
            this.inputData = inputText.split(',')
                .map(val => parseInt(val.trim()))
                .filter(val => !isNaN(val));
        } else {
            // Generate random data if no input provided
            this.inputData = this.generateRandomData(20);
        }

        this.log(`Data loaded: ${this.inputData.length} elements [${this.inputData.slice(0, 10).join(', ')}${this.inputData.length > 10 ? '...' : ''}]`, 'info');
        this.resetExperiment();
        this.renderInputData();
    }    loadDefaultData() {
        // Load some default data that demonstrates the bimodal distribution like DistributedSort.java
        this.inputData = [45, 123, 12, 567, 89, 234, 67, 345, 78, 456, 23, 678, 91, 234, 56];
        this.log('Default data set loaded', 'info');
        this.renderInputData();
        this.updateConfiguration();
    }
    
    updateConfiguration() {
        const mapperCount = parseInt(document.getElementById('mapperCount').value);
        const reducerCount = parseInt(document.getElementById('reducerCount').value);
        const partitionStrategy = document.getElementById('partitionStrategy').value;
        
        this.log(`Configuration updated: ${mapperCount} mappers, ${reducerCount} reducers, ${partitionStrategy} partitioning`, 'info');
        
        // Clear previous visualizations
        document.getElementById('mappersViz').innerHTML = '';
        document.getElementById('reducersViz').innerHTML = '';
        
        // Update prediction game buttons based on actual bucket count
        const actualBucketCount = Math.max(reducerCount + 1, this.globalPivots.length + 1);
        this.updatePredictionGame(actualBucketCount);
    }
    
    updatePredictionGame(bucketCount) {
        const predictionContainer = document.querySelector('#predictionChallenge .grid');
        if (predictionContainer) {
            predictionContainer.innerHTML = '';
            
            for (let i = 0; i < bucketCount; i++) {
                const button = document.createElement('button');
                button.className = 'prediction-btn btn-small';
                button.dataset.reducer = i;
                button.textContent = `Bucket ${i + 1}`;
                button.addEventListener('click', (e) => {
                    this.handlePrediction(parseInt(e.target.dataset.reducer));
                });
                predictionContainer.appendChild(button);
            }
        }
    }
    
    setupPredictionGame() {
        if (this.inputData.length > 0) {
            // Pick a random item for prediction
            const randomIdx = Math.floor(Math.random() * this.inputData.length);
            this.currentTargetItem = this.inputData[randomIdx];
            
            const targetElement = document.getElementById('targetItem');
            if (targetElement) {
                targetElement.textContent = this.currentTargetItem;
            }
        }
    }
    
    handlePrediction(predictedReducer) {
        if (this.currentTargetItem === null) return;
        
        // Calculate correct reducer based on global pivots
        let correctReducer = 0;
        if (this.globalPivots.length > 0) {
            while (correctReducer < this.globalPivots.length && 
                   this.currentTargetItem >= this.globalPivots[correctReducer]) {
                correctReducer++;
            }
        }
        
        const isCorrect = predictedReducer === correctReducer;
        this.predictionScore.total++;
        
        if (isCorrect) {
            this.predictionScore.correct++;
        }
        
        // Update display
        const resultElement = document.getElementById('predictionResult');
        const scoreElement = document.getElementById('predictionScore');
        
        if (resultElement) {
            resultElement.innerHTML = isCorrect ? 
                '<span class="text-green-600">✅ Correct!</span>' : 
                `<span class="text-red-600">❌ Wrong! Goes to Reducer ${correctReducer + 1}</span>`;
        }
        
        if (scoreElement) {
            scoreElement.textContent = `${this.predictionScore.correct}/${this.predictionScore.total}`;
        }
        
        // Setup next prediction
        setTimeout(() => {
            this.setupPredictionGame();
            if (resultElement) resultElement.innerHTML = '';
        }, 2000);
    }
    
    updateStatistics() {
        this.statistics.dataMovement = this.communicationRounds;
        this.statistics.efficiency = this.calculateEfficiency();
        
        // Update performance metrics display
        const dataMovementsElement = document.getElementById('dataMovements');
        const efficiencyElement = document.getElementById('efficiency');
        const efficiencyBarElement = document.getElementById('efficiencyBar');
        
        if (dataMovementsElement) {
            dataMovementsElement.textContent = this.communicationRounds;
        }
        
        if (efficiencyElement) {
            efficiencyElement.textContent = `${Math.round(this.statistics.efficiency)}%`;
        }
        
        if (efficiencyBarElement) {
            efficiencyBarElement.style.width = `${this.statistics.efficiency}%`;
        }
    }
    
    calculateEfficiency() {
        // Simple efficiency calculation based on load balance
        if (this.reducerBuckets.size === 0) return 100;
        
        const bucketSizes = Array.from(this.reducerBuckets.values()).map(bucket => bucket.length);
        if (bucketSizes.length === 0) return 100;
        
        const maxSize = Math.max(...bucketSizes);
        const minSize = Math.min(...bucketSizes);
        const avgSize = bucketSizes.reduce((sum, size) => sum + size, 0) / bucketSizes.length;
        
        // Calculate efficiency based on how balanced the buckets are
        if (avgSize === 0) return 100;
        const imbalance = maxSize - minSize;
        const efficiency = Math.max(0, 100 - (imbalance / avgSize * 50));
        
        return efficiency;
    }
    
    updatePerformanceMetrics() {
        // Update load balance
        const loadBalanceElement = document.getElementById('loadBalance');
        if (loadBalanceElement && this.reducerBuckets.size > 0) {
            const bucketSizes = Array.from(this.reducerBuckets.values()).map(bucket => bucket.length);
            if (bucketSizes.length > 0) {
                const maxSize = Math.max(...bucketSizes);
                const minSize = Math.min(...bucketSizes);
                
                if (maxSize - minSize <= 1) {
                    loadBalanceElement.textContent = 'Perfect';
                    loadBalanceElement.className = 'font-bold text-green-600';
                } else if (maxSize - minSize <= 3) {
                    loadBalanceElement.textContent = 'Good';
                    loadBalanceElement.className = 'font-bold text-yellow-600';
                } else {
                    loadBalanceElement.textContent = 'Poor';
                    loadBalanceElement.className = 'font-bold text-red-600';
                }
            }
        }
    }
    
    showModal() {
        const modal = document.getElementById('instructionsModal');
        if (modal) {
            modal.classList.remove('hidden');
        }
    }
    
    hideModal() {
        const modal = document.getElementById('instructionsModal');
        if (modal) {
            modal.classList.add('hidden');
        }
    }
    
    showQuiz() {
        const quizSection = document.getElementById('quizSection');
        if (quizSection && this.quizQuestions.length > 0) {
            const randomQuiz = this.quizQuestions[Math.floor(Math.random() * this.quizQuestions.length)];
            this.currentQuiz = randomQuiz;
            
            const questionElement = document.getElementById('quizQuestion');
            if (questionElement) {
                questionElement.textContent = randomQuiz.question;
            }
            
            const optionsContainer = document.getElementById('quizOptions');
            if (optionsContainer) {
                optionsContainer.innerHTML = '';
                
                randomQuiz.options.forEach((option, index) => {
                    const button = document.createElement('button');
                    button.className = 'quiz-option w-full text-left p-3 border border-gray-300 rounded hover:bg-blue-50 transition-colors';
                    button.textContent = `${String.fromCharCode(65 + index)}. ${option}`;
                    button.addEventListener('click', () => {
                        // Clear previous selections
                        optionsContainer.querySelectorAll('.quiz-option').forEach(btn => {
                            btn.classList.remove('bg-blue-200', 'border-blue-500');
                        });
                        // Mark this option as selected
                        button.classList.add('bg-blue-200', 'border-blue-500');
                        this.selectedQuizOption = index;
                    });
                    optionsContainer.appendChild(button);
                });
            }
            
            quizSection.classList.remove('hidden');
            
            const feedbackElement = document.getElementById('quizFeedback');
            if (feedbackElement) {
                feedbackElement.innerHTML = '';
            }
        }
    }
    
    hideQuiz() {
        const quizSection = document.getElementById('quizSection');
        if (quizSection) {
            quizSection.classList.add('hidden');
        }
    }
    
    submitQuiz() {
        if (this.selectedQuizOption === undefined || !this.currentQuiz) return;
        
        const isCorrect = this.selectedQuizOption === this.currentQuiz.correct;
        const feedbackElement = document.getElementById('quizFeedback');
        
        if (feedbackElement) {
            feedbackElement.innerHTML = isCorrect ?
                `<div class="text-green-600 font-semibold">✅ Correct!</div><div class="text-sm mt-2">${this.currentQuiz.explanation}</div>` :
                `<div class="text-red-600 font-semibold">❌ Incorrect.</div><div class="text-sm mt-2">${this.currentQuiz.explanation}</div>`;
        }
        
        setTimeout(() => {
            this.hideQuiz();
        }, 4000);
    }
    
    skipQuiz() {
        this.hideQuiz();
    }
    
    initializeChart() {
        // Placeholder for chart initialization if needed
        this.chart = null;
    }
    
    updateChart() {
        // Placeholder for chart updates if needed
    }
    
    // Activity Log Functions
    initializeLog() {
        this.logContainer = document.getElementById('logContainer');
        if (this.logContainer) {
            // Clear existing logs first
            this.logContainer.innerHTML = '';
            this.log('Distributed Sort MapReduce system initialized', 'success');
            this.log('Configure parameters and click "Load Data" to begin', 'info');
        }
        // Initialize statistics display
        this.updateExperimentStats();
    }
    
    log(message, type = 'info') {
        if (!this.logContainer) {
            this.logContainer = document.getElementById('logContainer');
        }
        
        if (this.logContainer) {
            const logEntry = document.createElement('div');
            logEntry.className = `log-entry ${type}`;
            const timestamp = new Date().toLocaleTimeString();
            logEntry.textContent = `[${timestamp}] ${message}`;
            
            this.logContainer.appendChild(logEntry);
            this.logContainer.scrollTop = this.logContainer.scrollHeight;
            
            // Keep only last 50 entries
            if (this.logContainer.children.length > 50) {
                this.logContainer.removeChild(this.logContainer.firstChild);
            }
        }
    }
    
    updateExperimentStats() {
        const elements = {
            totalSortsCount: document.getElementById('totalSortsCount'),
            totalElementsProcessed: document.getElementById('totalElementsProcessed'),
            avgCommunicationRounds: document.getElementById('avgCommunicationRounds'),
            totalExperiments: document.getElementById('totalExperiments')
        };
        
        if (elements.totalSortsCount) {
            elements.totalSortsCount.textContent = this.experimentStats.totalSorts;
        }
        if (elements.totalElementsProcessed) {
            elements.totalElementsProcessed.textContent = this.experimentStats.totalElementsProcessed;
        }
        if (elements.avgCommunicationRounds) {
            const avgRounds = this.experimentStats.totalExperiments > 0 ? 
                Math.round(this.experimentStats.totalCommunicationRounds / this.experimentStats.totalExperiments) : 0;
            elements.avgCommunicationRounds.textContent = avgRounds;
        }
        if (elements.totalExperiments) {
            elements.totalExperiments.textContent = this.experimentStats.totalExperiments;
        }
    }
    
    recordExperimentCompletion() {
        if (this.finalOutput.length === 0) return; // Only record successful completions
        
        this.experimentStats.totalSorts++;
        this.experimentStats.totalElementsProcessed += this.inputData.length;
        this.experimentStats.totalCommunicationRounds += this.communicationRounds;
        this.experimentStats.totalExperiments++;
        
        this.updateExperimentStats();
        const efficiency = Math.round(this.calculateEfficiency());
        this.log(`Experiment #${this.experimentStats.totalExperiments} completed successfully!`, 'success');
        this.log(`Elements: ${this.inputData.length}, Comm rounds: ${this.communicationRounds}, Efficiency: ${efficiency}%`, 'info');
    }
    
    // Download C++ implementation
    downloadCppCode() {
        this.log('Downloading C++ implementation of distributed sort algorithm', 'info');
        
        // Read the C++ code from the standalone file
        fetch('standalone_distributed_sort.cpp')
            .then(response => {
                if (!response.ok) {
                    // If the file doesn't exist via fetch, use embedded code
                    return this.getEmbeddedCppCode();
                }
                return response.text();
            })
            .catch(() => {
                // Fallback to embedded code if fetch fails
                return this.getEmbeddedCppCode();
            })
            .then(cppCode => {
                // Create download
                const blob = new Blob([cppCode], { type: 'text/cpp' });
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'distributed_sort.cpp';
                a.style.display = 'none';
                
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                window.URL.revokeObjectURL(url);
                
                this.log('C++ code downloaded successfully! Compile with: g++ distributed_sort.cpp -o sort', 'success');
            })
            .catch(error => {
                console.error('Error downloading C++ code:', error);
                this.log('Error downloading C++ code. Please check console for details.', 'error');
            });
    }
    
    // Embedded C++ code as fallback
    getEmbeddedCppCode() {
        return `/*
 * DISTRIBUTED SORTING ALGORITHM - STANDALONE C++ IMPLEMENTATION
 * =============================================================
 * 
 * Downloaded from the MapReduce Distributed Sort Web Simulation
 * This file implements the same distributed sorting algorithm used in the simulation.
 * It's a complete, self-contained program that requires no external dependencies.
 * 
 * COMPILATION (choose one):
 *   Windows: cl distributed_sort.cpp
 *   GCC:     g++ distributed_sort.cpp -o sort
 *   Clang:   clang++ distributed_sort.cpp -o sort
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
                    std::cout << "\\nThank you for using the Distributed Sort Simulator!\\n";
                    return;
                default:
                    std::cout << "\\nInvalid choice. Please try again.\\n";
            }
            
            std::cout << "\\nPress Enter to continue...";
            std::cin.ignore();
            std::cin.get();
        }
    }
    
    // ========== CORE ALGORITHM ==========
    
    std::vector<int> distributedSort(const std::vector<int>& data) {
        reset();
        inputData = data;
        
        std::cout << "\\n=== DISTRIBUTED SORT EXECUTION ===\\n";
        std::cout << "Input: ";
        printVector(inputData, 15);
        std::cout << "Mappers: " << numMappers << ", Reducers: " << numReducers << "\\n\\n";
        
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
        std::cout << "--- PHASE 1: PIVOT SELECTION ---\\n";
        
        // Step 1: Distribute data among mappers (round-robin like the web simulation)
        distributeDataToMappers();
        
        // Step 2: Each mapper selects a local pivot (random selection like JavaScript)
        selectLocalPivots();
        
        // Step 3: Create global pivots from local pivots (sort local pivots)
        createGlobalPivots();
        
        communicationRounds++;
        std::cout << "Communication round completed. Total rounds: " << communicationRounds << "\\n\\n";
    }
    
    void distributeDataToMappers() {
        mapperData.clear();
        mapperData.resize(numMappers);
        
        // Round-robin distribution (matches web simulation logic)
        for (int i = 0; i < inputData.size(); i++) {
            mapperData[i % numMappers].push_back(inputData[i]);
        }
        
        std::cout << "Data distribution to mappers:\\n";
        for (int i = 0; i < numMappers; i++) {
            std::cout << "  Mapper " << i << " (" << mapperData[i].size() << " elements): ";
            printVector(mapperData[i], 8);
        }
    }
    
    void selectLocalPivots() {
        localPivots.clear();
        
        std::cout << "Local pivot selection:\\n";
        for (int i = 0; i < numMappers; i++) {
            if (!mapperData[i].empty()) {
                // Random pivot selection (matches JavaScript random selection)
                int pivotIndex = std::rand() % mapperData[i].size();
                int pivot = mapperData[i][pivotIndex];
                localPivots.push_back(pivot);
                std::cout << "  Mapper " << i << " selected pivot: " << pivot << "\\n";
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
        std::cout << "--- PHASE 2: BUCKET DISTRIBUTION & SORTING ---\\n";
        
        // Step 1: Distribute data to buckets based on global pivots
        distributeToBuckets();
        
        // Step 2: Each reducer sorts its bucket
        performBucketSorting();
        
        // Step 3: Generate final output by concatenating sorted buckets
        generateFinalOutput();
        
        communicationRounds++;
        std::cout << "Final communication round completed. Total rounds: " << communicationRounds << "\\n";
    }
    
    void distributeToBuckets() {
        mapperBuckets.clear();
        int numBuckets = globalPivots.size() + 1;
        
        std::cout << "Distributing data to " << numBuckets << " buckets:\\n";
        
        // Each mapper distributes its data to appropriate buckets
        for (int mapperIdx = 0; mapperIdx < numMappers; mapperIdx++) {
            mapperBuckets[mapperIdx].resize(numBuckets);
            
            for (int value : mapperData[mapperIdx]) {
                int bucketIdx = findBucketForValue(value);
                mapperBuckets[mapperIdx][bucketIdx].push_back(value);
                dataMovements++;
            }
            
            // Show mapper's bucket distribution
            std::cout << "  Mapper " << mapperIdx << " buckets:\\n";
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
        
        std::cout << "Reducer processing (collecting and sorting buckets):\\n";
        
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
        
        std::cout << "Merging sorted buckets:\\n";
        
        // Concatenate all sorted reducer outputs in order
        // This naturally produces sorted output due to pivot-based partitioning
        for (int i = 0; i < reducerBuckets.size(); i++) {
            if (!reducerBuckets[i].empty()) {
                finalOutput.insert(finalOutput.end(), 
                                 reducerBuckets[i].begin(), 
                                 reducerBuckets[i].end());
                std::cout << "  Added bucket " << i << " (" << reducerBuckets[i].size() << " elements)\\n";
            }
        }
    }
    
    // ========== USER INTERFACE METHODS ==========
    
    void printHeader() {
        std::cout << "╔════════════════════════════════════════════════════════════╗\\n";
        std::cout << "║             DISTRIBUTED SORTING SIMULATOR                 ║\\n";
        std::cout << "║                                                            ║\\n";
        std::cout << "║  Implements the same Two-Phase MapReduce algorithm         ║\\n";
        std::cout << "║  used in the web simulation                                ║\\n";
        std::cout << "║                                                            ║\\n";
        std::cout << "║  Phase 1: Pivot Selection                                  ║\\n";
        std::cout << "║  Phase 2: Bucket Distribution & Sorting                   ║\\n";
        std::cout << "╚════════════════════════════════════════════════════════════╝\\n\\n";
    }
    
    void printMenu() {
        std::cout << "=== MENU ===\\n";
        std::cout << "1. Run with demo data (15 numbers)\\n";
        std::cout << "2. Run with random data\\n";
        std::cout << "3. Run with your custom data\\n";
        std::cout << "4. Configure mappers/reducers\\n";
        std::cout << "5. Performance comparison test\\n";
        std::cout << "6. Exit\\n";
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
        std::cout << "\\n=== DEMO DATA EXECUTION ===\\n";
        distributedSort(demoData);
    }
    
    void runWithRandomData() {
        std::cout << "\\nEnter data size (10-100 recommended): ";
        int size;
        std::cin >> size;
        size = std::max(5, std::min(500, size)); // Clamp to reasonable range
        
        std::vector<int> randomData = generateRandomData(size);
        std::cout << "\\n=== RANDOM DATA EXECUTION ===\\n";
        distributedSort(randomData);
    }
    
    void runWithCustomData() {
        std::cout << "\\nEnter numbers separated by spaces (press Enter when done):\\n";
        std::string line;
        std::getline(std::cin, line);
        
        std::vector<int> customData;
        std::istringstream iss(line);
        int num;
        while (iss >> num) {
            customData.push_back(num);
        }
        
        if (customData.empty()) {
            std::cout << "No valid numbers entered. Using demo data instead.\\n";
            runWithDemoData();
        } else {
            std::cout << "\\n=== CUSTOM DATA EXECUTION ===\\n";
            distributedSort(customData);
        }
    }
    
    void configureSystem() {
        std::cout << "\\nCurrent configuration: " << numMappers << " mappers, " << numReducers << " reducers\\n";
        
        std::cout << "Enter number of mappers (2-8): ";
        int m;
        std::cin >> m;
        numMappers = std::max(2, std::min(8, m));
        
        std::cout << "Enter number of reducers (2-8): ";
        int r;
        std::cin >> r;
        numReducers = std::max(2, std::min(8, r));
        
        std::cout << "Configuration updated: " << numMappers << " mappers, " << numReducers << " reducers\\n";
    }
    
    void runPerformanceTest() {
        std::cout << "\\n=== PERFORMANCE TEST ===\\n";
        std::cout << "Testing algorithm correctness with different data sizes...\\n\\n";
        
        std::vector<int> testSizes = {20, 50, 100, 200};
        
        for (int size : testSizes) {
            std::cout << "Testing with " << size << " elements:\\n";
            
            std::vector<int> testData = generateRandomData(size);
            std::vector<int> expected = testData;
            std::sort(expected.begin(), expected.end());
            
            // Reset for clean test
            reset();
            std::vector<int> result = distributedSort(testData);
            
            bool correct = (result == expected);
            std::cout << "  Result: " << (correct ? "✓ CORRECT" : "✗ INCORRECT") << "\\n";
            std::cout << "  Data movements: " << dataMovements << "\\n";
            std::cout << "  Communication rounds: " << communicationRounds << "\\n\\n";
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
        
        std::cout << "]\\n";
    }
    
    void printResults() {
        std::cout << "\\n=== RESULTS ===\\n";
        std::cout << "Final sorted output: ";
        printVector(finalOutput, 20);
        
        // Verify correctness
        std::vector<int> expected = inputData;
        std::sort(expected.begin(), expected.end());
        bool correct = (finalOutput == expected);
        
        std::cout << "Correctness: " << (correct ? "✓ PASSED" : "✗ FAILED") << "\\n";
        std::cout << "Elements processed: " << inputData.size() << "\\n";
        std::cout << "Data movements: " << dataMovements << "\\n";
        std::cout << "Communication rounds: " << communicationRounds << "\\n";
        
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
                         << ", max: " << maxSize << ", avg: " << std::setprecision(1) << avgSize << ")\\n";
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
 *   cl distributed_sort.cpp
 *   distributed_sort.exe
 * 
 * Windows (MinGW):
 *   g++ distributed_sort.cpp -o sort.exe
 *   sort.exe
 * 
 * Linux/Mac:
 *   g++ distributed_sort.cpp -o sort
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
 */`;
    }
}

// Initialize the experiment when the page loads
let experiment;
document.addEventListener('DOMContentLoaded', () => {
    experiment = new DistributedSortExperiment();
});
