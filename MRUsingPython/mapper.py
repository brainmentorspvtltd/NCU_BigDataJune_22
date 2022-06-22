import sys

# sys.stdin - standard input - read input from command terminal
for line in sys.stdin:
    # trim leading and trailing spaces
    line = line.strip()
    # split - performs tokenization
    words = line.split()
    for word in words:
        print(f"{word}\t{1}")
