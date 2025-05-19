I. Project Setup & Core Data Structures

Task:

Set up your Makefile to compile your source files into libMapReduceFramework.a.
Reference: readme.md (Submission section)
Define the JobState struct and stage_t enum as specified.
stage_t: UNDEFINED, MAP, SHUFFLE, REDUCE.
JobState: stage_t stage, float percentage.
Reference: readme.md (Framework Interface Overview)
Define JobHandle (which is void*). This will likely point to an internal struct that holds all information about a job.
Create an internal JobContext struct (or class). This will store:
The MapReduceClient object.
Pointers to inputVec and outputVec.
multiThreadLevel.
JobState instance.
Thread handles (std::vector<std::thread>).
Atomic counters for progress (e.g., input items processed, intermediate items emitted/shuffled, output items produced).
Data structures for intermediate elements (e.g., std::vector<std::vector<std::pair<K2*, V2*>>> for per-thread map outputs).
Data structures for shuffled elements (e.g., std::vector<std::vector<std::pair<K2*, V2*>>> or std::map<K2*, std::vector<V2*>> accessible by reducer threads).
Synchronization primitives (mutexes, condition variables, barriers).
A flag to indicate if waitForJob has already joined threads.
Tips & Guidelines:

Start with the basic structure. You'll add more to JobContext as you implement features.
Consider the advice on using a single 64-bit atomic variable for JobState for more robust atomic updates of stage and percentage. (Reference: readme.md - "To check and update the job state...")
Verification & Testing:

Ensure your Makefile can create an empty library.
Compile the files with the basic struct definitions.
II. startMapReduceJob Implementation (Initial)

Task:

Implement the startMapReduceJob function signature.
Reference: readme.md (Framework Interface Overview)
Inside, dynamically allocate your JobContext struct.
Initialize JobContext:
Store client, inputVec, outputVec, multiThreadLevel.
Set JobState.stage to UNDEFINED_STAGE and JobState.percentage to 0.
Initialize atomic counters for progress to 0.
Initialize thread handles vector.
Return the pointer to JobContext cast to JobHandle.
Thread Safety: The readme.md states startMapReduceJob must be thread-safe. If multiple framework jobs can be initiated concurrently by different application threads, ensure that the allocation and initialization of job-specific resources are protected. This usually means protecting any global state that startMapReduceJob might access, though typically each call creates independent resources.
Tips & Guidelines:

Handle potential memory allocation failures for JobContext (print error and exit(1)).
Reference: readme.md ("When a system call or standard library function fails...")
Verification & Testing:

Call startMapReduceJob and ensure it returns a non-null JobHandle.
(Later) Verify with getJobState that the initial state is UNDEFINED, 0%.
III. Thread Creation and Worker Function Stub

Task:

In startMapReduceJob, after initializing JobContext, create multiThreadLevel threads using std::thread.
Reference: readme.md ("You will have to create threads using the C++ thread standard library...")
Each thread should execute a common worker function (e.g., void worker_thread_routine(JobContext* jobCtx, int threadID)).
Pass the JobContext* and a unique threadID (0 to multiThreadLevel - 1) to each thread.
Store the std::thread objects in JobContext's thread handles vector.
Handle std::thread constructor failures (print error and exit(1)).
Tips & Guidelines:

The worker function will eventually contain the logic for Map, Sort, Shuffle (for thread 0), and Reduce phases.
Thread 0 has special responsibilities (Shuffle).
Verification & Testing:

Add a simple print statement (for temporary debugging, remove later) in the worker function to confirm threads are created and run.
Ensure the correct number of threads are created.
IV. Map Phase

Task:

Input Splitting: Implement logic in worker_thread_routine for threads to pick input pairs (K1*, V1*) from inputVec. Use an atomic counter (e.g., std::atomic<int> next_input_idx) shared among threads to get unique indices.
Reference: readme.md ("Splitting the input values between the threads – this will be done using an atomic variable...")
Job State Update (Start of Map): The first thread to start mapping should update JobState.stage to MAP_STAGE (atomically or protected).
Call client.map(): For each input pair, call client.map(k1, v1, context_for_emit2). The context_for_emit2 will be specific to the current thread and job, allowing emit2 to store data correctly. This context could be a pointer to a per-thread data structure within your JobContext.
emit2 Implementation:
void emit2 (K2* key, V2* value, void* context)
The context parameter should allow emit2 to access the current thread's intermediate vector (e.g., std::vector<std::pair<K2*, V2*>>) and the job's global atomic counter for total intermediate elements.
Store the (key, value) pair in the thread's intermediate vector.
Atomically increment a global counter for the total number of intermediate elements produced across all threads for this job.
Reference: readme.md ("emit2 – This function produces a (K2*, V2*) pair...")
Job State Update (Progress): Atomically update the count of processed input items. JobState.percentage for the MAP stage is (processed_input_items / total_input_items) * 100.
Reference: readme.md ("In the map stage the percentage is the number of input vector items processed...")
Tips & Guidelines:

Each thread should have its own vector to store intermediate pairs produced by emit2 to avoid race conditions during this phase. These can be part of JobContext, perhaps a std::vector<ThreadLocalData>, where ThreadLocalData contains this intermediate vector.
The context for emit2 could be a simple struct containing a pointer to the thread's intermediate vector and a pointer to the job's atomic counter for total intermediate elements.
Verification & Testing:

Use the SampleClient.cpp or a simple custom client.
Verify that all input items are processed.
Verify that emit2 correctly stores intermediate pairs in per-thread structures.
Check the total count of intermediate elements.
(Later) Use getJobState to monitor MAP phase progress.
V. getJobState Implementation

Task:

Implement void getJobState(JobHandle job, JobState* state).
Reference: readme.md (Framework Interface Overview)
Cast job back to your JobContext*.
Atomically read the current stage and percentage from JobContext's JobState members (or from the combined 64-bit atomic variable if you chose that advanced path).
Copy these values into the state struct provided by the user.
Tips & Guidelines:

Accessing JobState members must be thread-safe. If not using a single 64-bit atomic, use a mutex to protect reads/writes to JobState or ensure individual members are atomic and read carefully. The readme.md suggests a 64-bit atomic for stage and progress counters.
Verification & Testing:

Call getJobState at various points (before map, during map, after map) and verify the reported stage and percentage are correct.
VI. Sort Phase (Per-Thread)

Task:

After a thread has finished processing its share of the input in the Map phase, it must sort its locally collected intermediate (K2*, V2*) pairs.
The sort key is K2*. Use std::sort with a custom comparator that uses operator< on K2 objects.
Remember: a == b iff !(a < b) && !(b < a).
Reference: readme.md ("Tips ... Since the keys only have the < operator...")
Tips & Guidelines:

This sort happens within each worker thread on its own data, so no complex synchronization is needed for the sort operation itself.
Verification & Testing:

After the map phase, inspect the per-thread intermediate vectors to ensure they are sorted by K2*.
VII. Barrier Synchronization (Post-Map/Sort)

Task:

All threads must complete their Map and Sort phases before the Shuffle phase (executed by thread 0) can begin.
Implement or use a barrier. You have a Barrier class in your workspace (Barrier.h, Barrier.cpp). Integrate this.
Initialize the barrier in startMapReduceJob for multiThreadLevel threads.
Each thread calls barrier.wait() after it finishes sorting.
Reference: readme.md ("The shuffle phase will start only after all threads have completed the map and sort phase.")
Tips & Guidelines:

The barrier ensures that thread 0 doesn't start shuffling prematurely and other threads don't proceed to Reduce before shuffling is done.
Verification & Testing:

Difficult to test in isolation directly, but crucial for the correctness of Shuffle. Debug prints (temporary) can confirm all threads reach the barrier.
VIII. Shuffle Phase (Thread 0)

Task:

Only threadID == 0 executes this phase. This logic comes after the barrier.
Job State Update (Start of Shuffle): Thread 0 updates JobState.stage to SHUFFLE_STAGE.
Thread 0 needs to gather all sorted intermediate pairs from all other threads.
Create new sequences (vectors) of (K2*, V2*) pairs where all K2* in a sequence are identical (based on !(a < b) && !(b < a)).
A common approach is to create std::vector<std::vector<std::pair<K2*, V2*>>> shuffled_data; where each inner vector contains items with the same key.
Or, std::map<K2*, std::vector<V2*>, K2PointerComparator> grouped_data; might be more direct if K2 supports proper comparison for map keys.
Iterate through all per-thread sorted intermediate vectors. Merge them (like in merge-sort) and group by K2*.
Store these new grouped/shuffled sequences in a shared data structure within JobContext that reducer threads can access.
Job State Update (Progress): JobState.percentage for SHUFFLE is (intermediate_pairs_shuffled / total_intermediate_pairs_from_map) * 100. Update an atomic counter for intermediate_pairs_shuffled.
Reference: readme.md ("In the shuffle stage the percentage is the number of intermediate pairs shuffled...")
Tips & Guidelines:

This is a critical and complex phase. The goal is to prepare data such that reduce is called once for each unique K2.
The "total intermediate pairs" is the count you got from emit2 calls.
The output of shuffle is a list of lists, where each inner list contains all values for a particular key.
Verification & Testing:

After shuffle, inspect the resulting data structure. Verify that all intermediate pairs are present and correctly grouped by K2*.
(Later) Use getJobState to monitor SHUFFLE phase progress.
IX. Reduce Phase

Task:

Job State Update (Start of Reduce): After Shuffle, thread 0 (or the first thread to start reducing) updates JobState.stage to REDUCE_STAGE.
Work Distribution: Distribute the shuffled sequences (each sequence corresponding to a unique K2*) among all multiThreadLevel threads. An atomic counter can be used for threads to pick the next available sequence to reduce.
Call client.reduce(): Each thread takes a sequence (K2*, std::vector<V2*>) and calls client.reduce(vector_of_values_for_this_k2, context_for_emit3). The vector_of_values_for_this_k2 is essentially the std::vector<V2*> part of the shuffled data for a given K2*.
The readme.md states: "The reduce function receives a sequence of pairs (k2, v2) as input, where all keys are identical". So you'll pass a std::vector<std::pair<K2*, V2*>> (or just the key and std::vector<V2*>) to the client's reduce.
emit3 Implementation:
The readme.md mentions emit3(K3, V3, context) but doesn't give its signature. Assume void emit3 (K3* key, V3* value, void* context).
The context should allow emit3 to access the job's outputVec and an atomic counter for total output elements.
emit3 adds the (K3*, V3*) pair to the outputVec. This access must be synchronized (e.g., using a mutex protecting outputVec.push_back()).
Atomically increment a global counter for the total number of output elements produced.
Job State Update (Progress): JobState.percentage for REDUCE is (shuffled_sequences_reduced / total_unique_k2_sequences_from_shuffle) * 100. Update an atomic counter for shuffled_sequences_reduced.
Reference: readme.md ("In the reduce stage the percentage is the number of shuffled pairs (key, value) reduced...")
Tips & Guidelines:

Ensure outputVec is protected by a mutex during push_back operations in emit3.
The "total shuffled pairs" (or rather, total unique K2 groups) is the number of sequences created by the Shuffle phase.
Verification & Testing:

Verify that client.reduce is called once for each unique K2*.
Verify that emit3 correctly adds output pairs to outputVec.
Check the final content of outputVec against expected results.
(Later) Use getJobState to monitor REDUCE phase progress.
X. waitForJob Implementation

Task:

Implement void waitForJob(JobHandle job).
Reference: readme.md (Framework Interface Overview)
Cast job to JobContext*.
Iterate through the std::thread objects stored in JobContext and call join() on each.
Handle being called multiple times: Use a flag in JobContext (e.g., bool joined) to ensure join() is only called once per thread. Set this flag after joining.
Tips & Guidelines:

join() blocks until the respective thread finishes execution.
"Pay attention that calling join() twice from the same process has undefined behavior and you must avoid that."
Verification & Testing:

Call waitForJob. The program should wait until all MapReduce operations are complete.
Call waitForJob multiple times and ensure it doesn't crash or misbehave.
XI. closeJobHandle Implementation

Task:

Implement void closeJobHandle(JobHandle job).
Reference: readme.md ("closeJobHandle – Releasing all resources of a job.")
Cast job to JobContext*.
Wait if necessary: If the job is not yet finished (e.g., check a "finished" flag in JobContext or if joined flag is false), call your waitForJob logic on this handle first.
Release all dynamically allocated resources:
Memory for intermediate data structures.
The JobContext struct itself (delete jobCtx).
Any other resources allocated for this job.
The JobHandle becomes invalid after this call.
Tips & Guidelines:

This is crucial for preventing memory leaks.
"You can assume that for each framework job the function closeJobHandle will be called."
Verification & Testing:

Use a memory checker like Valgrind to ensure no memory leaks after closeJobHandle is called.
Test calling closeJobHandle on a job that's still running (it should wait) and on one that has finished.
XII. Error Handling and Final Touches

Task:

Ensure all system call/standard library failures (thread creation, memory allocation) print system error: text\n and call exit(1).
Reference: readme.md ("When a system call or standard library function fails...")
Double-check thread safety for all shared data access (JobState, counters, intermediate/output vectors).
Ensure no printf or std::cout calls remain, except for the specified error message.
Review performance considerations: avoid unnecessary data copying.
Reference: readme.md ("Pay attention to your runtime and complexity...")
Tips & Guidelines:

Be systematic with error checks, especially around new and std::thread construction.
Verification & Testing:

Induce errors (e.g., try to allocate huge memory to force failure) to test error reporting (do this in a controlled test, not in final code).
Final Valgrind check for memory leaks and race conditions (using Helgrind or DRD if available).
XIII. Comprehensive Testing Strategy

Task:

Use the provided SampleClient.cpp ([Sample Client/SampleClient.cpp](Sample Client/SampleClient.cpp)) extensively.
Create your own, more complex clients:
Clients with different key/value types.
Map functions that emit zero, one, or many intermediate pairs per input.
Map functions that emit the same intermediate key multiple times.
Reduce functions with different logic.
Input data that is empty, has one element, many elements, or elements that generate many identical intermediate keys.
Test with various multiThreadLevel values (1, 2, typical core counts like 4, 8).
multiThreadLevel = 1 is a good test case for basic logic correctness without concurrency complexities.
Test getJobState thoroughly during all phases.
Test waitForJob and closeJobHandle in various scenarios (job running, job finished).
If you aim for it: Test startMapReduceJob being called from multiple threads simultaneously with different job configurations.
Tips & Guidelines:

"Test early, test often."
Break down testing by phase as much as possible initially.