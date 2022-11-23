import splitter
import mapper
import reducer

splitter = splitter.Splitter()
mapper1 = mapper.Mapper("1")
mapper2 = mapper.Mapper("2")
mapper3 = mapper.Mapper("3")
reducer1 = reducer.Reducer("1")
reducer2 = reducer.Reducer("2")

# start work
reducer1.start()
reducer2.start()

mapper1.start()
mapper2.start()
mapper3.start()

splitter.start()

# make sure the reducers are done
reducer1.join()
reducer2.join()

# result
print("Reducer1 Dictonary:")
print(reducer1.dictionary)
print("Reducer2 Dictonary:")
print(reducer2.dictionary)
print("Reducer 1 counted: " + str(reducer1.counter) + " words")
print("Reducer 2 counted: " + str(reducer2.counter) + " words")
print("Totalwordcount: " + str(reducer1.counter + reducer2.counter))
