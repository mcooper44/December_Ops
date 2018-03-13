with open('LastNames.txt') as f:
    content = f.readlines()
    content = [x.strip() for x in content]
    content = [x[0] for x in [y.split() for y in content]]


