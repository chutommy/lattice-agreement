CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -Wpedantic

all: bin/links bin/broadcast bin/lattice

bin/%: src/%.cpp src/*.hpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -rf bin

.PHONY: all clean
