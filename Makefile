CXX = g++
CXXFLAGS = -std=c++11 -Wall -Wextra -pthread -g -I.
LDFLAGS = -pthread

# Source files for the framework library
FRAMEWORK_SRCS = MapReduceFramework.cpp Barrier/Barrier.cpp
FRAMEWORK_OBJS = $(FRAMEWORK_SRCS:.cpp=.o)

# Define clean paths first for clarity and use in commands
CLEAN_SAMPLE_CLIENT_DIR = Sample Client
CLEAN_SAMPLE_CLIENT_SRC = $(CLEAN_SAMPLE_CLIENT_DIR)/SampleClient.cpp
CLEAN_SAMPLE_CLIENT_OBJ = $(CLEAN_SAMPLE_CLIENT_DIR)/SampleClient.o

# Escaped versions for Make's internal use (targets, prerequisites)
# Use a single backslash to escape the space for make's parser.
ESCAPED_SAMPLE_CLIENT_SRC = Sample\ Client/SampleClient.cpp
ESCAPED_SAMPLE_CLIENT_OBJ = Sample\ Client/SampleClient.o

# Target library
LIB_TARGET = libMapReduceFramework.a

# Sample client executable
# SAMPLE_CLIENT_SRC and SAMPLE_CLIENT_OBJ are effectively replaced by the new variables.
SAMPLE_EXEC = sample

# Default target
all: $(LIB_TARGET) $(SAMPLE_EXEC)

# Rule to create the framework library
$(LIB_TARGET): $(FRAMEWORK_OBJS)
	ar rcs $@ $^

# Rule to compile and link the sample client
# Dependency uses the ESCAPED name for make
$(SAMPLE_EXEC): $(ESCAPED_SAMPLE_CLIENT_OBJ) $(LIB_TARGET)
# Command uses the CLEAN name, quoted, for the shell
	$(CXX) $(CXXFLAGS) "$(CLEAN_SAMPLE_CLIENT_OBJ)" $(LIB_TARGET) -o $@ $(LDFLAGS)

# Generic rule to compile .cpp files to .o files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Specific rule for Barrier.cpp due to its location
Barrier/Barrier.o: Barrier/Barrier.cpp Barrier/Barrier.h MapReduceClient.h
	$(CXX) $(CXXFLAGS) -c Barrier/Barrier.cpp -o $@

MapReduceFramework.o: MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h Barrier/Barrier.h
	$(CXX) $(CXXFLAGS) -c MapReduceFramework.cpp -o $@

# Specific rule for SampleClient.o
# Target and prerequisite use ESCAPED names for make
$(ESCAPED_SAMPLE_CLIENT_OBJ): $(ESCAPED_SAMPLE_CLIENT_SRC) MapReduceClient.h MapReduceFramework.h
# Command uses CLEAN names, quoted, for the shell
	$(CXX) $(CXXFLAGS) -c "$(CLEAN_SAMPLE_CLIENT_SRC)" -o "$(CLEAN_SAMPLE_CLIENT_OBJ)"

clean:
# For rm, use the CLEAN name, quoted, for the shell
	rm -f $(FRAMEWORK_OBJS) "$(CLEAN_SAMPLE_CLIENT_OBJ)" $(LIB_TARGET) $(SAMPLE_EXEC) Barrier/Barrier.o

.PHONY: all clean
