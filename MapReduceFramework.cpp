#include "MapReduceFramework.h"
#include "Barrier/Barrier.h" // Assuming Barrier.h is in Barrier/ subdirectory
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <algorithm>    // For std::sort
#include <system_error> // For std::system_error related to thread creation
#include <new>          // For std::bad_alloc
#include <functional>   // For std::ref, std::bind (not strictly needed here yet)
#include <mutex>        // For std::once_flag, std::call_once

// Constants for atomicJobState encoding (Stage: 2 bits, Count: 62 bits)
const uint64_t STAGE_SHIFT = 62;
const uint64_t COUNT_MASK = (1ULL << STAGE_SHIFT) - 1;

// Helper to get stage value for atomic state
inline uint64_t get_stage_val(stage_t stage) {
    return static_cast<uint64_t>(stage);
}

struct JobContext {
    const MapReduceClient* client;
    const InputVec* inputVec;
    OutputVec* outputVec;
    int multiThreadLevel;

    std::vector<std::thread> threads;
    Barrier* barrier;

    std::atomic<uint64_t> atomicJobState; // Bits 63-62 for stage, 0-61 for processed count for current stage
    std::atomic<uint64_t> totalItemsInCurrentStage; // Total items for the current stage to calculate percentage

    // Data structures for map phase
    std::vector<IntermediateVec> intermediateVecsPerThread;
    std::atomic<unsigned int> nextInputPairIndex;

    // Data structures for shuffle phase (Thread 0 will populate this)
    IntermediateVec allIntermediatePairsForShuffle; // Temporary flat list before grouping
    std::vector<IntermediateVec> shuffledIntermediateVecs; // Output of shuffle, input to reduce

    // Data structures for reduce phase
    std::atomic<unsigned int> nextShuffledGroupIndexToReduce;

    // Synchronization
    std::mutex outputVecMutex; // To protect outputVec during emit3
    std::atomic<bool> joined;
    std::once_flag mapStageInitializedFlag;
    std::once_flag reduceStageInitializedFlag; // For setting up REDUCE stage

    // Counter for total intermediate elements produced by map (used for shuffle's total)
    std::atomic<uint64_t> totalIntermediateElementsProduced;

    JobContext(const MapReduceClient& client_ref, const InputVec& input_vec_ref, OutputVec& output_vec_ref, int mt_level)
        : client(&client_ref), inputVec(&input_vec_ref), outputVec(&output_vec_ref), multiThreadLevel(mt_level),
          barrier(nullptr), atomicJobState(0), totalItemsInCurrentStage(0),
          nextInputPairIndex(0), nextShuffledGroupIndexToReduce(0), joined(false),
          mapStageInitializedFlag(), reduceStageInitializedFlag(), totalIntermediateElementsProduced(0)
    {
        try {
            // Resize intermediate vectors for each thread
            if (multiThreadLevel > 0) { // Ensure multiThreadLevel is positive
                intermediateVecsPerThread.resize(multiThreadLevel);
            } else {
                // Handle case where multiThreadLevel might be 0 or negative if necessary,
                // though problem constraints usually imply >= 1.
                // For now, assume multiThreadLevel >= 1 based on typical usage.
                // If it can be 0, this resize might not be needed or error.
            }
        } catch (const std::bad_alloc& e) {
            std::cerr << "system error: Failed to allocate memory for intermediateVecsPerThread in JobContext constructor: " << e.what() << std::endl;
            exit(1); // As per readme for system errors
        }
        // Initialize atomicJobState to UNDEFINED_STAGE, 0%
        uint64_t initial_state = (get_stage_val(UNDEFINED_STAGE) << STAGE_SHIFT);
        atomicJobState.store(initial_state, std::memory_order_relaxed);
    }

    ~JobContext() {
        delete barrier; // Safe to delete nullptr
        barrier = nullptr;

        // Note: This framework assumes K*/V* pointers passed to emit functions
        // are managed by the client or are trivial. If the framework were to
        // take ownership of copies, cleanup would be needed here for
        // intermediateVecsPerThread, shuffledIntermediateVecs, and outputVec.
        // Based on typical MapReduce, client manages original data, framework manages pointers/containers.
    }

    // Atomically update stage and reset count
    void setStage(stage_t stage) {
        uint64_t new_val = (get_stage_val(stage) << STAGE_SHIFT) | 0; // Reset count to 0
        atomicJobState.store(new_val);
    }
    
    // Atomically increment processed items count for the current stage
    void incrementProcessedCount() {
        atomicJobState.fetch_add(1, std::memory_order_relaxed);
    }

    // Set total items for the current stage (used for percentage calculation)
    void setTotalItemsForStage(uint64_t total) {
        totalItemsInCurrentStage.store(total);
    }
};

// Context for emit2
struct Emit2Context {
    JobContext* jobCtx;
    int threadID; 
};

// Context for emit3
struct Emit3Context {
    JobContext* jobCtx;
};

// Forward declaration for the worker thread function
void workerThreadRoutine(JobContext* jobCtx, int threadID);

void emit2 (K2* key, V2* value, void* context) {
    Emit2Context* ctx = static_cast<Emit2Context*>(context);
    JobContext* jobCtx = ctx->jobCtx;
    int threadID = ctx->threadID;

    jobCtx->intermediateVecsPerThread[threadID].push_back({key, value});
    jobCtx->totalIntermediateElementsProduced.fetch_add(1, std::memory_order_relaxed);
}

void emit3 (K3* key, V3* value, void* context) {
    Emit3Context* ctx = static_cast<Emit3Context*>(context);
    JobContext* jobCtx = ctx->jobCtx;

    std::lock_guard<std::mutex> lock(jobCtx->outputVecMutex);
    jobCtx->outputVec->push_back({key, value});
}

JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel)
{
    JobContext* jobCtx = nullptr;
    try {
        jobCtx = new JobContext(client, inputVec, outputVec, multiThreadLevel);
        if (multiThreadLevel > 0) { // Barrier is only created if there are worker threads
            jobCtx->barrier = new Barrier(multiThreadLevel);
        }
    } catch (const std::bad_alloc& e) {
        std::cerr << "system error: Failed to allocate JobContext or Barrier: " << e.what() << std::endl;
        delete jobCtx; // Clean up partially allocated jobCtx if barrier allocation failed
        exit(1);
    } catch (...) { // Catch any other exceptions from JobContext constructor
        std::cerr << "system error: Unknown error during JobContext or Barrier allocation." << std::endl;
        delete jobCtx;
        exit(1);
    }


    jobCtx->threads.reserve(multiThreadLevel);
    for (int i = 0; i < multiThreadLevel; ++i) {
        try {
            jobCtx->threads.emplace_back(workerThreadRoutine, jobCtx, i);
        } catch (const std::system_error& e) {
            std::cerr << "system error: Failed to create thread " << i << ": " << e.what() << std::endl;
            // According to readme, exit(1). Before exiting, attempt to join already created threads.
            // However, a simple exit(1) is what's asked.
            // To be robust, one might set an abort flag, join threads, then delete jobCtx.
            // For now, following the exit(1) directive.
            jobCtx->joined = true; // Prevent waitForJob from trying to join later if it's called amidst this.
            for(int j = 0; j < i; ++j) { // Attempt to join threads that were successfully created
                if (jobCtx->threads[j].joinable()) {
                    // jobCtx->threads[j].join(); // This might hang if threads are stuck.
                                               // A more complex shutdown (e.g. condition variable) would be needed.
                                               // Given exit(1), this is best-effort.
                }
            }
            delete jobCtx; // This will call destructor, deleting barrier
            exit(1);
        }
    }

    return static_cast<JobHandle>(jobCtx);
}

void workerThreadRoutine(JobContext* jobCtx, int threadID) {
    // MAP PHASE
    // Initialize Map Stage (only once by one thread)
    std::call_once(jobCtx->mapStageInitializedFlag, [&]() {
        jobCtx->totalItemsInCurrentStage.store(jobCtx->inputVec->size());
        uint64_t initial_state = (get_stage_val(MAP_STAGE) << STAGE_SHIFT);
        if (jobCtx->inputVec->empty()) { // Handle empty input
             initial_state |= COUNT_MASK; // Mark as 100%
        }
        jobCtx->atomicJobState.store(initial_state, std::memory_order_release); // Use release order
        jobCtx->nextInputPairIndex.store(0, std::memory_order_relaxed);
    });

    // Process input pairs for Map
    Emit2Context emit2Context = {jobCtx, threadID};
    unsigned int index;
    while ((index = jobCtx->nextInputPairIndex.fetch_add(1, std::memory_order_relaxed)) < jobCtx->inputVec->size()) {
        const InputPair& pair = (*jobCtx->inputVec)[index];
        jobCtx->client->map(pair.first, pair.second, &emit2Context);
        
        // Update progress for MAP_STAGE (increment processed count)
        // This ensures that even if a stage changes, we only increment if we are in MAP_STAGE
        uint64_t current_val = jobCtx->atomicJobState.load(std::memory_order_relaxed);
        uint64_t processed_count;
        stage_t current_stage_enum;
        do {
            current_stage_enum = static_cast<stage_t>((current_val >> STAGE_SHIFT) & 0x3);
            processed_count = current_val & COUNT_MASK;
            if (current_stage_enum != MAP_STAGE) break; 
            processed_count++;
        } while (!jobCtx->atomicJobState.compare_exchange_weak(current_val,
                                                              (get_stage_val(MAP_STAGE) << STAGE_SHIFT) | processed_count,
                                                              std::memory_order_release,
                                                              std::memory_order_relaxed));
    }

    // SORT PHASE (Local sort per thread)
    std::sort(jobCtx->intermediateVecsPerThread[threadID].begin(),
              jobCtx->intermediateVecsPerThread[threadID].end(),
              [](const IntermediatePair& a, const IntermediatePair& b) {
                  return *(a.first) < *(b.first);
              });

    jobCtx->barrier->barrier(); // Wait for all threads to finish Map and Sort

    // SHUFFLE PHASE (Only thread 0 performs the shuffle)
    if (threadID == 0) {
        // 1. Set stage to SHUFFLE and total items for shuffle
        uint64_t total_intermediate_items = jobCtx->totalIntermediateElementsProduced.load(std::memory_order_relaxed);
        jobCtx->totalItemsInCurrentStage.store(total_intermediate_items); // This is for internal use by shuffle progress itself if needed
        
        uint64_t initial_shuffle_state = (get_stage_val(SHUFFLE_STAGE) << STAGE_SHIFT);
        if (total_intermediate_items == 0) {
            initial_shuffle_state |= COUNT_MASK; // Mark as 100% if no items
        }
        jobCtx->atomicJobState.store(initial_shuffle_state, std::memory_order_release); // Use release order
        
        uint64_t shuffle_processed_count = 0;

        // 2. Collect all intermediate pairs from all threads
        jobCtx->allIntermediatePairsForShuffle.clear(); // Clear from any previous potential use (not strictly necessary here)
        for (int i = 0; i < jobCtx->multiThreadLevel; ++i) {
            for (const auto& pair : jobCtx->intermediateVecsPerThread[i]) {
                jobCtx->allIntermediatePairsForShuffle.push_back(pair);
                // Update shuffle progress for each pair collected
                shuffle_processed_count++;
                if (total_intermediate_items > 0) { // Avoid division by zero if no items
                     // This store is for shuffle progress, stage is already SHUFFLE_STAGE
                     // Using relaxed is fine as it's within the same stage, by the same thread.
                     jobCtx->atomicJobState.store((get_stage_val(SHUFFLE_STAGE) << STAGE_SHIFT) | shuffle_processed_count, std::memory_order_relaxed);
                }
            }
            jobCtx->intermediateVecsPerThread[i].clear(); // Clear after collecting
        }
        
        // If total_intermediate_items was 0, state is already 100%. Otherwise, if loop finished, it's 100%.
        // Ensure the final count for shuffle stage is correctly set if it wasn't empty.
        if (total_intermediate_items > 0 && shuffle_processed_count == total_intermediate_items) {
             jobCtx->atomicJobState.store((get_stage_val(SHUFFLE_STAGE) << STAGE_SHIFT) | shuffle_processed_count, std::memory_order_relaxed);
        }


        // 3. Sort all collected intermediate pairs globally
        std::sort(jobCtx->allIntermediatePairsForShuffle.begin(),
                  jobCtx->allIntermediatePairsForShuffle.end(),
                  [](const IntermediatePair& a, const IntermediatePair& b) {
                      return *(a.first) < *(b.first);
                  });

        // 4. Group sorted pairs by K2 into shuffledIntermediateVecs
        jobCtx->shuffledIntermediateVecs.clear();
        if (!jobCtx->allIntermediatePairsForShuffle.empty()) {
            jobCtx->shuffledIntermediateVecs.emplace_back();
            jobCtx->shuffledIntermediateVecs.back().push_back(jobCtx->allIntermediatePairsForShuffle[0]);

            for (size_t i = 1; i < jobCtx->allIntermediatePairsForShuffle.size(); ++i) {
                // If current K2 is same as previous K2 (dereferenced comparison)
                if (!(*(jobCtx->allIntermediatePairsForShuffle[i].first) < *(jobCtx->allIntermediatePairsForShuffle[i-1].first)) &&
                    !(*(jobCtx->allIntermediatePairsForShuffle[i-1].first) < *(jobCtx->allIntermediatePairsForShuffle[i].first)))
                {
                    jobCtx->shuffledIntermediateVecs.back().push_back(jobCtx->allIntermediatePairsForShuffle[i]);
                } else { // New K2
                    jobCtx->shuffledIntermediateVecs.emplace_back();
                    jobCtx->shuffledIntermediateVecs.back().push_back(jobCtx->allIntermediatePairsForShuffle[i]);
                }
            }
        }
        jobCtx->allIntermediatePairsForShuffle.clear(); // Free memory after grouping
    }

    jobCtx->barrier->barrier(); // Wait for shuffle phase to complete

    // REDUCE PHASE
    // Initialize Reduce Stage (only once by one thread)
    std::call_once(jobCtx->reduceStageInitializedFlag, [&]() {
        uint64_t num_groups_to_reduce = jobCtx->shuffledIntermediateVecs.size();
        jobCtx->totalItemsInCurrentStage.store(num_groups_to_reduce);
        jobCtx->nextShuffledGroupIndexToReduce.store(0, std::memory_order_relaxed);

        uint64_t initial_reduce_state = (get_stage_val(REDUCE_STAGE) << STAGE_SHIFT);
        if (num_groups_to_reduce == 0) {
            initial_reduce_state |= COUNT_MASK; // Mark as 100% if no groups
        }
        jobCtx->atomicJobState.store(initial_reduce_state, std::memory_order_release);
    });

    Emit3Context emit3Context = {jobCtx};
    unsigned int groupIdx;
    while (true) {
        groupIdx = jobCtx->nextShuffledGroupIndexToReduce.fetch_add(1, std::memory_order_relaxed);
        
        // Check if all groups have been claimed
        if (groupIdx >= jobCtx->shuffledIntermediateVecs.size()) {
            break; 
        }

        const IntermediateVec* currentGroup = &(jobCtx->shuffledIntermediateVecs[groupIdx]);
        // It's possible client->reduce is called with an empty vector if a key had no values,
        // though current shuffle logic should produce non-empty vectors for existing keys.
        // The client's reduce function should handle this if it's a possibility.
        // Here, we assume currentGroup points to a valid (possibly empty) vector if groupIdx is valid.
        
        jobCtx->client->reduce(currentGroup, &emit3Context);

        // Update progress for REDUCE_STAGE
        jobCtx->atomicJobState.fetch_add(1, std::memory_order_release); // Changed from acq_rel to release
    }

    jobCtx->barrier->barrier(); // Wait for all threads to finish Reduce phase
}

void getJobState(JobHandle job, JobState* state) {
    if (!job || !state) {
        if (state) { // If state is not null, set to undefined
            state->stage = UNDEFINED_STAGE;
            state->percentage = 0.0f;
        }
        return;
    }

    JobContext* jobCtx = static_cast<JobContext*>(job);
    uint64_t current_atomic_state = jobCtx->atomicJobState.load(std::memory_order_acquire); // Use acquire order

    state->stage = static_cast<stage_t>((current_atomic_state >> STAGE_SHIFT));
    uint64_t processed_count = current_atomic_state & COUNT_MASK;
    
    uint64_t actual_total_for_stage_read;

    switch (state->stage) {
        case UNDEFINED_STAGE:
            actual_total_for_stage_read = 0;
            break;
        case MAP_STAGE:
            actual_total_for_stage_read = jobCtx->inputVec->size();
            break;
        case SHUFFLE_STAGE:
            actual_total_for_stage_read = jobCtx->totalIntermediateElementsProduced.load(std::memory_order_relaxed);
            break;
        case REDUCE_STAGE:
            // shuffledIntermediateVecs.size() is stable after shuffle barrier and before reduce phase fully completes.
            // Accessing shuffledIntermediateVecs here is safe as it's populated by thread 0
            // and then only read by reducer threads or this getJobState.
            actual_total_for_stage_read = jobCtx->shuffledIntermediateVecs.size(); 
            break;
        default: // Should not happen
            actual_total_for_stage_read = 0;
            break;
    }

    if (processed_count == COUNT_MASK && state->stage != UNDEFINED_STAGE) { // Explicit 100% marker
        state->percentage = 100.0f;
    } else if (actual_total_for_stage_read == 0) {
        // If total is 0, stage is 100% (unless UNDEFINED_STAGE).
        // This covers empty input for map, no intermediate for shuffle, no groups for reduce.
        state->percentage = (state->stage == UNDEFINED_STAGE) ? 0.0f : 100.0f;
    } else {
        state->percentage = (static_cast<float>(processed_count) / actual_total_for_stage_read) * 100.0f;
    }
    
    // Clamp percentage in case of floating point inaccuracies or if processed_count somehow exceeds actual_total.
    if (state->percentage > 100.0f) {
        state->percentage = 100.0f;
    }
    // It should not be less than 0 if counts are positive.
}

void waitForJob(JobHandle job) {
    if (!job) return;
    JobContext* jobCtx = static_cast<JobContext*>(job);

    bool already_joined = jobCtx->joined.exchange(true, std::memory_order_acq_rel);
    if (!already_joined) {
        for (auto& thread : jobCtx->threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }
}

void closeJobHandle(JobHandle job) {
    if (!job) return;
    // Ensure all threads are joined before deleting JobContext
    waitForJob(job); 

    JobContext* jobCtx = static_cast<JobContext*>(job);
    delete jobCtx;
}
