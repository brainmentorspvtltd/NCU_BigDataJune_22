import sys

currentWord = None
currentCount = 0
word = None

for line in sys.stdin:
    # print(line)
    line = line.strip()
    word, count = line.split("\t",1)
    # print(word, count)
    
    try:
        count = int(count)
    except BaseException as ex:
        print(ex)
        continue

    if currentWord == word:
        currentCount += count
    else:
        if currentWord:
            print(f"{currentWord}\t{currentCount}")
        currentCount = count
        currentWord = word

# this if is for the last word
if currentWord == word:
    print(f"{currentWord}\t{currentCount}")
