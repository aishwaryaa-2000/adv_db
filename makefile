# Makefile for RepCRec Distributed Database
# Author: RepCRec Team
# Date: December 2024

CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
TARGET = repcrec

# Source files
SOURCES = main.cpp \
          DataManager.cpp \
          TransactionManager.cpp \ 
          Parser.cpp

# Header files (for dependency tracking)
HEADERS = Constants.h \
          Version.h \
          WaitInfo.h \
          ReadInfo.h \
          WriteInfo.h \
          Transaction.h \
          SiteState.h \
          DataManager.h \
          TransactionManager.h \
          Parser.h

# Object files
OBJECTS = $(SOURCES:.cpp=.o)

# Default target
all: $(TARGET)

# Link the executable
$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJECTS)
	@echo "Build complete: $(TARGET)"

# Compile source files
%.o: %.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean build artifacts
clean:
	rm -f $(OBJECTS) $(TARGET)
	@echo "Clean complete"

# Run with a test file
test: $(TARGET)
	@echo "Running test..."
	./$(TARGET) < test1.txt

# Phony targets
.PHONY: all clean test