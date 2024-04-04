import random

numbers = [random.randint(0, 100) for _ in range(10)]

#usando shuffle
random.shuffle(numbers)