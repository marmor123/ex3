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
        // Barrier allocation can throw std::bad_alloc, caught by startMapReduceJob
        barrier = new Barrier(multiThreadLevel); 
        
        threads.reserve(multiThreadLevel);
        intermediateVecsPerThread.resize(multiThreadLevel);

        // Initial state: UNDEFINED_STAGE, 0 processed items
        uint64_t initial_state = (get_stage_val(UNDEFINED_STAGE) << STAGE_SHIFT) | 0;
        atomicJobState.store(initial_state);
    }

    ~JobContext() {
        delete barrier;
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
    
    jobCtx->intermediateVecsPerThread[ctx->threadID].emplace_back(key, value);
    jobCtx->totalIntermediateElementsProduced.fetch_add(1, std::memory_order_relaxed);
}

void emit3 (K3* key, V3* value, void* context) {
    Emit3Context* ctx = static_cast<Emit3Context*>(context);
    JobContext* jobCtx = ctx->jobCtx;

    std::lock_guard<std::mutex> lock(jobCtx->outputVecMutex);
    jobCtx->outputVec->emplace_back(key, value);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel)
{
    JobContext* jobCtx = nullptr;
    try {
        jobCtx = new JobContext(client, inputVec, outputVec, multiThreadLevel);
    } catch (const std::bad_alloc& e) {
        std::cerr << "system error: Failed to allocate JobContext" << std::endl;
        exit(1);
    } catch (const std::system_error& e) { // Barrier constructor might throw this if it uses system calls
        std::cerr << "system error: Failed to initialize JobContext (Barrier): " << e.what() << std::endl;
        exit(1);
    }


    try {
        for (int i = 0; i < multiThreadLevel; ++i) {
            jobCtx->threads.emplace_back(workerThreadRoutine, jobCtx, i);
        }
    } catch (const std::system_error& e) {
        std::cerr << "system error: Failed to create thread: " << e.what() << std::endl;
        // Make sure to clean up allocated JobContext and its Barrier before exiting
        delete jobCtx; 
        exit(1);
    }
    return static_cast<JobHandle>(jobCtx);
}

void workerThreadRoutine(JobContext* jobCtx, int threadID) {
    // Phase 1: Map
    // Initialize MAP stage (done once by one thread)
    std::call_once(jobCtx->mapStageInitializedFlag, [&]() {
        jobCtx->setStage(MAP_STAGE);
        jobCtx->setTotalItemsForStage(jobCtx->inputVec->size());
    });

    Emit2Context emit2Context = {jobCtx, threadID};
    unsigned int current_input_idx;
    while ((current_input_idx = jobCtx->nextInputPairIndex.fetch_add(1, std::memory_order_relaxed)) < jobCtx->inputVec->size()) {
        const InputPair& pair = (*jobCtx->inputVec)[current_input_idx];
        if (pair.first && pair.second) { // Basic check
             jobCtx->client->map(pair.first, pair.second, &emit2Context);
        }
        jobCtx->incrementProcessedCount(); 
    }

    // Phase 1.5: Sort local intermediate vector
    std::sort(jobCtx->intermediateVecsPerThread[threadID].begin(),
              jobCtx->intermediateVecsPerThread[threadID].end(),
              [](const IntermediatePair& p1, const IntermediatePair& p2) {
                  // Ensure K2 objects are valid before dereferencing
                  if (!p1.first || !p2.first) {
                      // This case should ideally not happen if client behaves correctly
                      // and always emits valid K2* pointers.
                      // If it can happen, define consistent behavior (e.g., nulls first/last or error).
                      // For now, assume valid K2* from client.
                      if (!p1.first && !p2.first) return false; // both null, consider equal
                      if (!p1.first) return true; // nulls first
                      if (!p2.first) return false; // nulls first
                  }
                  return (*(p1.first) < *(p2.first));
              });
    
    jobCtx->barrier->barrier(); // Wait for all threads to finish map and sort

    // Phase 2: Shuffle (only thread 0)
    if (threadID == 0) {
        jobCtx->setStage(SHUFFLE_STAGE);
        uint64_t total_intermediate_for_shuffle = jobCtx->totalIntermediateElementsProduced.load(std::memory_order_relaxed);
        jobCtx->setTotalItemsForStage(total_intermediate_for_shuffle);
        
        // Consolidate all intermediate pairs from per-thread vectors
        for (const auto& thread_vec : jobCtx->intermediateVecsPerThread) {
            for (const auto& pair : thread_vec) {
                jobCtx->allIntermediatePairsForShuffle.push_back(pair);
            }
        }
        
        // Sort all intermediate pairs globally
        std::sort(jobCtx->allIntermediatePairsForShuffle.begin(),
                  jobCtx->allIntermediatePairsForShuffle.end(),
                  [](const IntermediatePair& p1, const IntermediatePair& p2) {
                      if (!p1.first || !p2.first) {
                          if (!p1.first && !p2.first) return false;
                          if (!p1.first) return true;
                          return false;
                      }
                      return (*(p1.first) < *(p2.first));
                  });

        // Group sorted pairs by key for the reduce phase
        // And update shuffle progress
        if (!jobCtx->allIntermediatePairsForShuffle.empty()) {
            K2* current_key = nullptr; // Will be set by the first valid pair
            IntermediateVec current_group;

            for (const auto& pair : jobCtx->allIntermediatePairsForShuffle) {
                if (!pair.first) continue; // Skip null keys if they somehow appear

                if (current_group.empty() || (*(pair.first) < *current_key || *current_key < *(pair.first))) { // New key
                    if (!current_group.empty()) {
                        jobCtx->shuffledIntermediateVecs.push_back(current_group);
                    }
                    current_group.clear();
                    current_key = pair.first;
                }
                current_group.push_back(pair);
                jobCtx->incrementProcessedCount(); // Increment for each item added to a group (or processed for shuffle)
            }
            if (!current_group.empty()) { // Add the last group
                jobCtx->shuffledIntermediateVecs.push_back(current_group);
            }
        } else if (total_intermediate_for_shuffle == 0) {
             // If there were no intermediate elements, shuffle is trivially complete.
             // The percentage will be 0/0 -> handled by getJobState as 100% if stage is SHUFFLE.
        }
    }

    jobCtx->barrier->barrier(); // Wait for shuffle to complete

    // Phase 3: Reduce
    std::call_once(jobCtx->reduceStageInitializedFlag, [&]() {
        jobCtx->setStage(REDUCE_STAGE);
        jobCtx->setTotalItemsForStage(jobCtx->shuffledIntermediateVecs.size()); // Total unique K2 groups
    });

    Emit3Context emit3Context = {jobCtx};
    unsigned int current_group_idx;
    while ((current_group_idx = jobCtx->nextShuffledGroupIndexToReduce.fetch_add(1, std::memory_order_relaxed)) < jobCtx->shuffledIntermediateVecs.size()) {
        const IntermediateVec& group_to_reduce = jobCtx->shuffledIntermediateVecs[current_group_idx];
        if (!group_to_reduce.empty()) { // Ensure group is not empty
            jobCtx->client->reduce(&group_to_reduce, &emit3Context);
        }
        jobCtx->incrementProcessedCount(); // Increment processed groups for REDUCE stage
    }
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
