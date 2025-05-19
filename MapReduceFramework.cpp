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
    if (!context) {
        // Consider logging an error or specific handling if context is null
        return;
    }
    Emit2Context* emitCtx = static_cast<Emit2Context*>(context);
    JobContext* jobCtx = emitCtx->jobCtx;
    int threadID = emitCtx->threadID;

    if (threadID < 0 || static_cast<size_t>(threadID) >= jobCtx->intermediateVecsPerThread.size()) {
        std::cerr << "system error: Invalid threadID in emit2 context." << std::endl;
        // This indicates a programming error.
        exit(1); // Critical error
    }

    try {
        jobCtx->intermediateVecsPerThread[threadID].emplace_back(key, value);
    } catch (const std::bad_alloc& e) {
        std::cerr << "system error: Failed to allocate memory for intermediate pair in emit2: " << e.what() << std::endl;
        exit(1); // As per readme for system errors
    }
    
    jobCtx->totalIntermediateElementsProduced.fetch_add(1, std::memory_order_relaxed);
}

void emit3 (K3* key, V3* value, void* context) {
    if (!context) return;
    Emit3Context* emitCtx = static_cast<Emit3Context*>(context);
    JobContext* jobCtx = emitCtx->jobCtx;

    std::lock_guard<std::mutex> lock(jobCtx->outputVecMutex);
    try {
        jobCtx->outputVec->emplace_back(key, value);
    } catch (const std::bad_alloc& e) {
        std::cerr << "system error: Failed to allocate memory for output pair in emit3: " << e.what() << std::endl;
        exit(1);
    }
    // Note: The main atomicJobState (for overall job progress) is updated in workerThreadRoutine
    // after a reduce task (a K2 group) is completed, not per emit3 call.
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
    std::call_once(jobCtx->mapStageInitializedFlag, [&]() {
        size_t totalInputItems = jobCtx->inputVec ? jobCtx->inputVec->size() : 0;
        jobCtx->totalItemsInCurrentStage.store(totalInputItems, std::memory_order_relaxed);
        
        uint64_t initial_map_state = (get_stage_val(MAP_STAGE) << STAGE_SHIFT); // count is 0
        jobCtx->atomicJobState.store(initial_map_state, std::memory_order_release);
    });

    Emit2Context emit2Ctx = {jobCtx, threadID};
    unsigned int current_input_idx;
    size_t total_input_size = jobCtx->inputVec ? jobCtx->inputVec->size() : 0;

    if (total_input_size > 0) { // Only proceed if there's input
        while ((current_input_idx = jobCtx->nextInputPairIndex.fetch_add(1, std::memory_order_relaxed)) < total_input_size) {
            const InputPair& pair = (*jobCtx->inputVec)[current_input_idx];
            jobCtx->client->map(pair.first, pair.second, &emit2Ctx);

            uint64_t old_state, new_state;
            old_state = jobCtx->atomicJobState.load(std::memory_order_relaxed);
            do {
                uint64_t current_stage_val = (old_state >> STAGE_SHIFT);
                if (current_stage_val != MAP_STAGE) { // Stage changed, stop processing for this stage
                    goto end_map_phase; // Exit map processing loop if stage changed
                }
                uint64_t current_count = old_state & COUNT_MASK;
                uint64_t stage_bits = old_state & (~COUNT_MASK);
                new_state = stage_bits | (current_count + 1);
            } while (!jobCtx->atomicJobState.compare_exchange_weak(old_state, new_state, std::memory_order_release, std::memory_order_relaxed));
        }
    }
    end_map_phase:; // Label for goto

    // Barrier after map phase
    if (jobCtx->multiThreadLevel > 1 && jobCtx->barrier) {
        jobCtx->barrier->barrier();
    }

    // SORT PHASE (Local sort per thread)
    if (static_cast<size_t>(threadID) < jobCtx->intermediateVecsPerThread.size()) {
        try {
            std::sort(jobCtx->intermediateVecsPerThread[threadID].begin(),
                      jobCtx->intermediateVecsPerThread[threadID].end(),
                      [](const IntermediatePair& a, const IntermediatePair& b) {
                          // Assuming K2* are always valid as per typical MapReduce client contract
                          // and emit2 receives valid keys.
                          if (a.first == nullptr || b.first == nullptr) {
                              // This case should ideally not happen if client and emit2 are correct.
                              // If it can, specific error handling or definition of order is needed.
                              // For now, consider null < non-null, and null == null.
                              if (a.first == nullptr && b.first != nullptr) return true; // nulls first
                              if (a.first != nullptr && b.first == nullptr) return false;
                              if (a.first == nullptr && b.first == nullptr) return false; // Equal or arbitrary
                              // Fallthrough if both are non-null
                          }
                          return *(a.first) < *(b.first);
                      });
        } catch (const std::exception& e) {
            // std::sort can throw if comparison throws or on bad_alloc with element moves.
            // K2::operator< should not throw. bad_alloc is the main concern if elements are large
            // or custom move operations allocate.
            std::cerr << "system error: Exception during sort in thread " << threadID << ": " << e.what() << std::endl;
            exit(1); // Critical error during sort
        }
    }

    // Barrier after sort phase
    if (jobCtx->multiThreadLevel > 1 && jobCtx->barrier) {
        jobCtx->barrier->barrier();
    }

    // SHUFFLE PHASE (Thread 0 only)
    // Placeholder: Thread 0 collects all intermediate pairs from intermediateVecsPerThread,
    // sorts them globally, groups them by K2, and populates shuffledIntermediateVecs.
    // Updates atomicJobState to SHUFFLE_STAGE and its progress.
    // jobCtx->totalItemsInCurrentStage for SHUFFLE_STAGE will be jobCtx->totalIntermediateElementsProduced.

    // Barrier after shuffle phase
    if (jobCtx->multiThreadLevel > 1 && jobCtx->barrier) {
        jobCtx->barrier->barrier();
    }

    // REDUCE PHASE
    // Placeholder: Threads pick groups from shuffledIntermediateVecs, call client->reduce(),
    // and use emit3 to output results.
    // Updates atomicJobState to REDUCE_STAGE and its progress.
    // jobCtx->totalItemsInCurrentStage for REDUCE_STAGE will be the number of unique K2 keys (groups).
}

// ...existing code...
void getJobState(JobHandle job, JobState* state) {
    if (!job || !state) {
        if (state) { // If state is not null, set to undefined
            state->stage = UNDEFINED_STAGE;
            state->percentage = 0.0f;
        }
        return;
    }

    JobContext* jobCtx = static_cast<JobContext*>(job);
    uint64_t current_atomic_state = jobCtx->atomicJobState.load(std::memory_order_relaxed);

    state->stage = static_cast<stage_t>((current_atomic_state >> STAGE_SHIFT));
    uint64_t processed_count = current_atomic_state & COUNT_MASK;
    uint64_t total_for_stage = jobCtx->totalItemsInCurrentStage.load(std::memory_order_relaxed);

    if (total_for_stage == 0) {
        // If total items for the stage is 0:
        // - If the stage is active (MAP, SHUFFLE, REDUCE), it's 100% complete.
        // - If the stage is UNDEFINED, it's 0%.
        // This handles cases like empty inputVec (MAP stage, 0 total, 100%),
        // or no intermediate elements (SHUFFLE stage, 0 total, 100%).
        state->percentage = (state->stage == UNDEFINED_STAGE) ? 0.0f : 100.0f;
    } else {
        state->percentage = (static_cast<float>(processed_count) / total_for_stage) * 100.0f;
    }
    
    // Ensure percentage does not exceed 100%
    if (state->percentage > 100.0f) {
        state->percentage = 100.0f;
    }
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
