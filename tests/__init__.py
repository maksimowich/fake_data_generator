l = [1, 2, 3]

i = (filter(lambda x: x == 23, l))

print(next(i, None))